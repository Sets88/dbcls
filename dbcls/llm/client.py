"""Talking to an OpenAI-compatible chat-completions endpoint.

One HTTP shape covers every model the user is likely to point dbcls at —
OpenRouter, Ollama, vLLM, LM Studio, llama.cpp, or a corporate proxy — so the
only things to configure are the base URL, the model and (optionally) a key.
The request goes out through the standard library, which keeps the whole
feature dependency-free.
"""
import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Union

#: How many times the model may call tools before we stop feeding results back.
#: Each round trip is a full request, so this bounds both time and token spend.
MAX_TOOL_ROUNDS = 8

#: Cap on a single tool result handed back to the model, in characters.
MAX_TOOL_RESULT_CHARS = 8000


class LLMError(Exception):
    """The endpoint could not be reached, or answered with something unusable."""


@dataclass
class LLMConfig:
    """Everything needed to reach a model.  Without *base_url* and *model* the
    chat is simply not offered (see :func:`is_configured`)."""

    base_url: str = ''
    api_key: str = ''
    model: str = ''
    max_tokens: int = 131072
    timeout: float = 600.0
    #: Extra HTTP headers (OpenRouter's HTTP-Referer / X-Title, for instance).
    headers: Dict[str, str] = field(default_factory=dict)
    #: Ask the user before running a tool registered with needs_approval —
    #: read on every call, so switching it mid-conversation takes effect at once.
    #: On unless ``no_confirm_tools`` is set.
    confirm_tools: bool = True
    #: The same for a tool registered with ``executes`` — one that runs code
    #: the model wrote (``run_sql``; a shell command, say, from a plugin).
    #: Separate on purpose: turning off the questions about lookups leaves this
    #: one on, and only ``no_confirm_exec`` turns it off.
    confirm_exec: bool = True

    def is_configured(self) -> bool:
        return bool(self.base_url and self.model)

    @property
    def endpoint(self) -> str:
        return self.base_url.rstrip('/') + '/chat/completions'

    @classmethod
    def from_mapping(cls, values: Optional[dict]) -> 'LLMConfig':
        """Build a config from CLI/env/JSON values, ignoring empty ones so a
        later source cannot blank out an earlier one."""
        values = values or {}
        config = cls()
        for name in ('base_url', 'api_key', 'model'):
            value = values.get(name)
            if value:
                setattr(config, name, str(value))
        for name, cast in (('max_tokens', int), ('timeout', float)):
            value = values.get(name)
            if value in (None, ''):
                continue
            try:
                setattr(config, name, cast(value))
            except (TypeError, ValueError):
                raise LLMError(f'--llm-{name.replace("_", "-")} must be a number, got {value!r}')
        headers = values.get('headers')
        if isinstance(headers, dict):
            config.headers = {str(k): str(v) for k, v in headers.items()}
        config.confirm_tools = not parse_bool(values.get('no_confirm_tools'))
        config.confirm_exec = not parse_bool(values.get('no_confirm_exec'))
        return config


def parse_bool(value: Any) -> bool:
    """A flag from any source: argparse gives a bool, the config file a bool
    or a string, ``DBCLS_LLM_*`` always a string."""
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on')
    return bool(value)


def _post_json(url: str, payload: dict, headers: Dict[str, str], timeout: float) -> dict:
    """Blocking POST — always called on a worker thread (see LLMClient.complete)."""
    data = json.dumps(payload).encode('utf-8')
    request = urllib.request.Request(url, data=data, method='POST')
    request.add_header('Content-Type', 'application/json')
    for name, value in headers.items():
        request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', 'replace')[:500] if exc.fp else ''
        raise LLMError(f'HTTP {exc.code} from {url}: {detail or exc.reason}') from None
    except urllib.error.URLError as exc:
        raise LLMError(f'Cannot reach {url}: {exc.reason}') from None
    except OSError as exc:
        raise LLMError(f'Cannot reach {url}: {exc}') from None
    try:
        return json.loads(body)
    except ValueError:
        raise LLMError(f'{url} did not answer with JSON: {body[:300]}') from None


class LLMClient:
    """A chat conversation with tools, over the OpenAI chat-completions API."""

    def __init__(self, config: LLMConfig, tools: Optional['ToolRegistry'] = None,
                 approve: Optional[Callable[[str, dict], Awaitable[Union[None, str, dict]]]] = None):
        self.config = config
        self.tools = tools
        #: ``async approve(name, arguments)`` — asked before a tool that needs
        #: approval runs, while the config's switch for its kind is on
        #: (confirm_tools, or confirm_exec for an ``executes`` tool).  None lets it
        #: run; a string refuses, and is what the model gets back as the
        #: result; a dict runs it with those arguments instead — the user
        #: edited the call (see :func:`edited_result`).
        self.approve = approve

    def _headers(self) -> Dict[str, str]:
        headers = dict(self.config.headers)
        if self.config.api_key:
            headers['Authorization'] = f'Bearer {self.config.api_key}'
        return headers

    async def complete(self, messages: List[dict], *, use_tools: bool = True,
                       force_tool: Optional[str] = None) -> dict:
        """One round trip; returns the assistant message dict.

        *force_tool* names a tool the model must call this turn instead of
        choosing for itself — the endpoint's ``tool_choice``."""
        payload = {
            'model': self.config.model,
            'messages': messages,
            'max_tokens': self.config.max_tokens,
        }
        if use_tools and self.tools and self.tools.schemas():
            payload['tools'] = self.tools.schemas()
            payload['tool_choice'] = (
                {'type': 'function', 'function': {'name': force_tool}}
                if force_tool else 'auto')

        response = await asyncio.to_thread(
            _post_json, self.config.endpoint, payload, self._headers(), self.config.timeout)

        if isinstance(response, dict) and response.get('error'):
            error = response['error']
            message = error.get('message') if isinstance(error, dict) else str(error)
            raise LLMError(str(message))
        choices = (response or {}).get('choices') or []
        if not choices:
            raise LLMError(f'No choices in the response: {str(response)[:300]}')
        message = choices[0].get('message') or {}
        if not isinstance(message, dict):
            raise LLMError('Malformed message in the response')
        return message

    async def run(self, messages: List[dict], *,
                  on_event: Optional[Callable[[str, dict], None]] = None,
                  max_rounds: int = MAX_TOOL_ROUNDS,
                  require_tool: Optional[str] = None,
                  satisfied_by: Sequence[str] = ()) -> List[dict]:
        """Drive the conversation until the model answers with text.

        *messages* is extended in place with everything exchanged — the
        assistant's tool calls, each tool result, and the final answer — so the
        caller can both display the trace and keep it as the conversation's
        history.  *on_event* is called with ``('thinking'|'tool'|'tool_result',
        details)`` so a UI can show what is happening; it must be cheap and must
        not raise.

        *require_tool* names a tool the answer is worthless without — the one
        that hands the result over.  Models forget to call it and just describe
        the answer instead, so if the run ends without it, one more request goes
        out asking for it and nothing else (see :meth:`_demand_tool`).
        *satisfied_by* names tools that end a turn just as validly, so a run
        that called one of them is complete and nothing is demanded — the chat
        answers a question that way, without proposing a query.

        Returns the messages appended during this run.
        """
        appended: List[dict] = []
        called: set = set()

        def emit(kind: str, details: dict) -> None:
            if on_event is not None:
                on_event(kind, details)

        for _round in range(max_rounds):
            emit('thinking', {})
            message = await self.complete(messages)
            messages.append(message)
            appended.append(message)

            tool_calls = message.get('tool_calls') or []
            if not tool_calls:
                appended.extend(await self._demand_tool(require_tool, called, messages,
                                                        emit, satisfied_by))
                return appended

            for call in tool_calls:
                called.add(((call.get('function') or {}).get('name')) or '')
                result_message = await self._run_tool_call(call, emit)
                messages.append(result_message)
                appended.append(result_message)

        # Out of rounds: ask for a final answer with the tools withheld, so the
        # user gets something rather than a silent stop.
        emit('thinking', {})
        message = await self.complete(messages, use_tools=False)
        messages.append(message)
        appended.append(message)
        return appended

    async def _demand_tool(self, name: Optional[str], called: set,
                           messages: List[dict], emit,
                           satisfied_by: Sequence[str] = ()) -> List[dict]:
        """Ask once more for *name*, when the model finished without calling it.

        The request forces that tool through ``tool_choice``, so a model that
        merely described its answer has to hand it over.  If the endpoint will
        not take a forced choice, or the model still refuses, the conversation
        is left as it was — the caller reports that nothing was handed over.

        A run that called one of *satisfied_by* ended the way it was meant to
        and is left alone: demanding a query there would undo the answer.
        """
        if not name or name in called or self.tools is None or name not in self.tools.names():
            return []
        if called.intersection(satisfied_by):
            return []

        nudge = {
            'role': 'user',
            'content': (f'You have not called {name} yet, so nothing reached the '
                        f'editor. Call {name} now with the finished query. Do not '
                        f'explain it again — just make the call.'),
        }
        messages.append(nudge)
        emit('thinking', {})
        try:
            message = await self.complete(messages, force_tool=name)
        except LLMError:
            messages.pop()          # leave the history as it was
            return []

        appended: List[dict] = [nudge, message]
        messages.append(message)
        for call in message.get('tool_calls') or []:
            called.add(((call.get('function') or {}).get('name')) or '')
            result_message = await self._run_tool_call(call, emit)
            messages.append(result_message)
            appended.append(result_message)
        return appended

    async def _run_tool_call(self, call: dict, emit) -> dict:
        """Execute one tool call and build the ``role: tool`` reply for it.

        A failing tool is reported back to the model as its result rather than
        raised: the model can then correct itself (wrong table name, say)
        instead of the whole chat dying.
        """
        function = call.get('function') or {}
        name = function.get('name') or ''
        raw_arguments = function.get('arguments') or '{}'
        try:
            arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else dict(raw_arguments)
            if not isinstance(arguments, dict):
                raise ValueError('arguments must be a JSON object')
        except (ValueError, TypeError) as exc:
            arguments = {}
            content = f'Error: cannot parse arguments {raw_arguments!r}: {exc}'
        else:
            emit('tool', {'name': name, 'arguments': arguments})
            content = await self._call_tool(name, arguments)
        emit('tool_result', {'name': name, 'content': content})
        return {
            'role': 'tool',
            'tool_call_id': call.get('id', ''),
            'name': name,
            'content': content,
        }

    async def _call_tool(self, name: str, arguments: dict) -> str:
        if self.tools is None:
            return f'Error: no tools are available (tried to call {name!r})'
        edited = None
        if self.approve is not None and self._asks_before(name):
            answer = await self.approve(name, arguments)
            if isinstance(answer, str):
                # The handler never runs; the model is told why instead, so it
                # carries on without the result rather than retrying blindly.
                return answer
            if isinstance(answer, dict):
                edited = arguments = answer
        try:
            result = await self.tools.call(name, arguments)
        except Exception as exc:   # a broken tool must not kill the conversation
            result = f'Error: {type(exc).__name__}: {exc}'
            if edited is None:
                return result
        if edited is not None:
            result = edited_result(edited, result)
        return truncate_result(result, self.tools.result_limit(name))

    def _asks_before(self, name: str) -> bool:
        """Whether *name* is put to the user before it runs, as things stand."""
        kind = self.tools.approval_kind(name)
        return {'tools': self.config.confirm_tools,
                'exec': self.config.confirm_exec}.get(kind, False)


def edited_result(arguments: dict, result: Any) -> dict:
    """The result of a call the user changed before it ran.  The model has to
    be told: otherwise it reads the rows as the answer to what *it* asked."""
    return {
        'edited_by_user': True,
        'ran_with': arguments,
        'note': 'The user edited this call before it ran; the result is for '
                'the edited version, not the one you sent.',
        'result': result,
    }


def truncate_result(result: Any, limit: Optional[int] = MAX_TOOL_RESULT_CHARS) -> str:
    """Serialise a tool result to JSON, keeping it small enough to send.

    Long results are cut with a note saying so, so the model knows it is
    looking at a sample rather than the whole answer.  A *limit* of None sends
    the result whole — for a tool whose whole point is a long document.
    """
    if isinstance(result, str):
        text = result
    else:
        try:
            text = json.dumps(result, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            text = str(result)
    if limit is None or len(text) <= limit:
        return text
    return text[:limit] + f'\n... [truncated at {limit} characters]'


class ToolRegistry:
    """The tools offered to the model: their JSON schemas and implementations.

    Plugins add to it through ``PluginAPI.add_llm_tool``, so an extension can
    teach the chat about anything dbcls itself does not know.
    """

    def __init__(self):
        self._tools: Dict[str, dict] = {}

    def add(self, name: str, description: str, parameters: dict, handler,
            max_result_chars: Optional[int] = MAX_TOOL_RESULT_CHARS,
            needs_approval: bool = True, executes: Optional[str] = None) -> None:
        """*handler* is ``async def handler(**arguments) -> Any``.

        *max_result_chars* caps what the tool may send back; pass None for a
        tool whose result must arrive whole (a reference document, say).

        *needs_approval* False exempts the tool from ``confirm_tools``: the
        chat's own ways of answering and asking, and the reference documents,
        touch nothing the user would want to be asked about.

        *executes* names the argument that carries code the tool runs — the
        statement of ``run_sql``, a shell command line.  It puts the tool under
        ``confirm_exec`` instead: turning off the questions about lookups must
        not let that through unasked.  The permission prompt shows that
        argument and lets the user edit it before the call goes ahead."""
        self._tools[name] = {
            'name': name,
            'description': description,
            'parameters': parameters,
            'handler': handler,
            'max_result_chars': max_result_chars,
            'needs_approval': needs_approval,
            'executes': executes,
        }

    def names(self) -> List[str]:
        return list(self._tools)

    def approval_kind(self, name: str) -> Optional[str]:
        """Which switch asks before *name*: ``'exec'`` (confirm_exec),
        ``'tools'`` (confirm_tools) or None — never asked about.  A tool
        nobody registered counts as an ordinary one."""
        tool = self._tools.get(name)
        if tool is None:
            return 'tools'
        if tool['executes']:
            return 'exec'
        return 'tools' if tool['needs_approval'] else None

    def executes(self, name: str) -> Optional[str]:
        """The argument of *name* that holds the code it runs, or None."""
        tool = self._tools.get(name)
        return None if tool is None else tool['executes']

    def result_limit(self, name: str) -> Optional[int]:
        tool = self._tools.get(name)
        return MAX_TOOL_RESULT_CHARS if tool is None else tool['max_result_chars']

    def schemas(self) -> List[dict]:
        return [
            {
                'type': 'function',
                'function': {
                    'name': tool['name'],
                    'description': tool['description'],
                    'parameters': tool['parameters'],
                },
            }
            for tool in self._tools.values()
        ]

    async def call(self, name: str, arguments: dict) -> Any:
        tool = self._tools.get(name)
        if tool is None:
            known = ', '.join(self._tools) or 'none'
            raise LLMError(f'Unknown tool {name!r}. Available tools: {known}')
        return await tool['handler'](**arguments)
