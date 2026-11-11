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
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

#: How many rounds of tool calls one request may take before the user is asked
#: what to do (``max_tool_rounds``; 0 lifts the limit).  Each round trip is a
#: full request, so this bounds both time and token spend.
MAX_TOOL_ROUNDS = 10

#: The user's answers when the rounds are spent (see LLMClient.extend).
EXTEND = 'extend'      # as many rounds again, nothing said to the model
FINISH = 'finish'      # tell the model the budget is spent; it finishes with what it has
STOP = 'stop'          # end the request here

#: Rounds granted after FINISH for handing the answer over: the lookups are
#: refused by then, only the finishing tools still run.
CLOSING_ROUNDS = 2

#: Cap on a single tool result handed back to the model, in characters.
MAX_TOOL_RESULT_CHARS = 8000

#: What the model gets in place of a lookup it called after FINISH.
BUDGET_SPENT = ('Refused: the tool budget for this request is spent and {name} is '
                'not available any more. Finish with what you already have.')
#: What a BUDGET_SPENT refusal is rewritten to once its request is over: the
#: call has to stay answered, but the next question starts with the lookups open.
BUDGET_REOPENED = ('Not run: the tool budget of that earlier request was spent. '
                   '{name} is available again.')


class LLMError(Exception):
    """The endpoint could not be reached, or answered with something unusable."""


class ToolBudgetSpent(LLMError):
    """The request used up its rounds of tool calls and the user stopped it."""


@dataclass
class Decision:
    """The user's answer to a permission prompt (see :attr:`LLMClient.approve`).

    Allowed, the call runs — with *arguments* in place of the model's when the
    user edited them.  Refused, *refusal* is what the model gets as the result,
    so it carries on without the call instead of retrying blindly.  *note* is
    what the user wants to tell the model either way — "use the orders table",
    "that column is a timestamp" — and goes along with the result or the
    refusal."""

    allowed: bool = True
    arguments: Optional[dict] = None
    refusal: str = ''
    note: str = ''


@dataclass
class LLMConfig:
    """Everything needed to reach a model.  Without *base_url* and *model* the
    chat is simply not offered (see :func:`is_configured`)."""

    base_url: str = ''
    api_key: str = ''
    model: str = ''
    max_tokens: int = 131072
    timeout: float = 600.0
    #: Rounds of tool calls one request may take; 0 for no limit.
    max_tool_rounds: int = MAX_TOOL_ROUNDS
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
        for name, cast in (('max_tokens', int), ('timeout', float), ('max_tool_rounds', int)):
            value = values.get(name)
            if value in (None, ''):
                continue
            try:
                setattr(config, name, cast(value))
            except (TypeError, ValueError):
                raise LLMError(f'--llm-{name.replace("_", "-")} must be a number, got {value!r}')
        if config.max_tool_rounds < 0:
            raise LLMError('--llm-max-tool-rounds must be 0 (no limit) or more, '
                           f'got {config.max_tool_rounds}')
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
                 approve: Optional[Callable[[str, dict], Awaitable[Decision]]] = None,
                 extend: Optional[Callable[[int, int], Awaitable[str]]] = None):
        self.config = config
        self.tools = tools
        #: ``async approve(name, arguments) -> Decision`` — asked before a tool
        #: that needs approval runs, while the config's switch for its kind is
        #: on (confirm_tools, or confirm_exec for an ``executes`` tool).
        self.approve = approve
        #: ``async extend(rounds_used, more)`` — asked when a request has used
        #: up its rounds of tool calls (``max_tool_rounds``) without finishing.
        #: Answers EXTEND (*more* rounds, the model is told nothing), FINISH
        #: (the model is told the budget is spent and finishes with what it
        #: has) or STOP (the request ends).  Without a callback the request
        #: stops (see :meth:`run`).
        self.extend = extend

    def _headers(self) -> Dict[str, str]:
        headers = dict(self.config.headers)
        if self.config.api_key:
            headers['Authorization'] = f'Bearer {self.config.api_key}'
        return headers

    async def complete(self, messages: List[dict], *, allow_tools: bool = True,
                       force_tool: Optional[str] = None) -> dict:
        """One round trip; returns the assistant message dict.

        *force_tool* names a tool the model must call this turn instead of
        choosing for itself — the endpoint's ``tool_choice``.  *allow_tools*
        False forbids calls the same way (``tool_choice: none``) while still
        sending the definitions: a history with tool calls in it is rejected
        by some endpoints when the tools they refer to are missing."""
        payload = {
            'model': self.config.model,
            'messages': messages,
            'max_tokens': self.config.max_tokens,
        }
        schemas = self.tools.schemas() if self.tools else None
        if schemas:
            payload['tools'] = schemas
            if force_tool:
                payload['tool_choice'] = {'type': 'function', 'function': {'name': force_tool}}
            else:
                payload['tool_choice'] = 'auto' if allow_tools else 'none'

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
                  max_rounds: Optional[int] = None,
                  require_tool: Optional[str] = None,
                  satisfied_by: Sequence[str] = ()) -> List[dict]:
        """Drive the conversation until the model answers with text.

        *messages* is extended in place with everything exchanged — the
        assistant's tool calls, each tool result, and the final answer — so the
        caller can both display the trace and keep it as the conversation's
        history.  *on_event* is called with ``('thinking'|'tool'|'tool_result'|
        'budget', details)`` so a UI can show what is happening; it must be
        cheap and must not raise.

        *require_tool* names a tool the answer is worthless without — the one
        that hands the result over.  Models forget to call it and just describe
        the answer instead, so if the run ends without it, one more request goes
        out asking for it and nothing else (see :meth:`_demand_tool`).
        *satisfied_by* names tools that end a turn just as validly, so a run
        that called one of them is complete and nothing is demanded — the chat
        answers a question that way, without proposing a query.

        *max_rounds* (the config's ``max_tool_rounds`` unless given; 0 for no
        limit) is how many rounds of tool calls may go by before the question
        is put to :attr:`extend`: as many again, or have the model finish with
        what it has, or stop.  The model is told nothing unless the answer is
        FINISH — a model told its budget is short rushes to a poor answer.
        Stopping raises :class:`ToolBudgetSpent`; the history is left whole,
        every call answered, so the conversation can go on from there.

        The turn that tells the model its budget is spent (FINISH) is about
        this request only, so it is taken out of *messages* once the run is
        over, however it ends — left in, the next question in the chat would
        be answered with the lookups still "closed".  It is not among the
        messages returned either: nothing of it outlives the run.  The lookups
        refused meanwhile stay — a call must keep its answer — but each refusal
        is rewritten to say the tool is available again (BUDGET_REOPENED).

        A round that hands the answer over (*require_tool* or one of
        *satisfied_by*) just as the budget runs out is not followed by the
        question: the model gets one round more to close with text, once per
        run, and only if it goes on calling tools is the user asked.

        Returns the messages appended during this run.
        """
        appended: List[dict] = []
        called: set = set()
        step = self.config.max_tool_rounds if max_rounds is None else max_rounds
        budget = step           # grows by *step* with every EXTEND
        finishing = {name for name in (require_tool, *satisfied_by) if name}
        rounds = 0
        #: Rounds left for handing the answer over once FINISH was chosen; None
        #: while the lookups are still open.
        closing: Optional[int] = None
        nudge: Optional[dict] = None
        refused: List[dict] = []    # BUDGET_SPENT replies, reworded on the way out
        handed_over = False         # the round just made handed the answer over
        wrap_up_given = False       # the one round past the budget was granted

        def emit(kind: str, details: dict) -> None:
            if on_event is not None:
                on_event(kind, details)

        try:
            while True:
                if (budget and rounds >= budget and closing is None
                        and handed_over and not wrap_up_given):
                    wrap_up_given = True    # the answer is in: let it say so
                elif budget and rounds >= budget and closing is None:
                    answer = await self.extend(rounds, step) if self.extend is not None else STOP
                    emit('budget', {'rounds': rounds, 'answer': answer})
                    if answer == EXTEND:
                        budget += step
                    elif answer == FINISH:
                        closing = CLOSING_ROUNDS
                        nudge = closing_nudge(rounds, finishing)
                        messages.append(nudge)
                    else:
                        raise ToolBudgetSpent(
                            f'Stopped after {rounds} rounds of tool calls without an answer')

                emit('thinking', {})
                message = await self._complete_round(messages, last=closing == 0)
                messages.append(message)
                appended.append(message)

                tool_calls = message.get('tool_calls') or []
                if not tool_calls:
                    appended.extend(await self._demand_tool(require_tool, called, messages,
                                                            emit, satisfied_by))
                    return appended

                handed_over = False
                for call in tool_calls:
                    name = call_name(call)
                    called.add(name)
                    if closing is not None and name not in finishing:
                        result_message = tool_reply(call, BUDGET_SPENT.format(name=name))
                        refused.append(result_message)
                        emit('tool_result', {'name': name, 'content': result_message['content']})
                    else:
                        result_message, ran = await self._run_tool_call(call, emit)
                        # only a finishing call that went through: one with
                        # unparsable arguments, denied or failing handed
                        # nothing over, and earns no round past the budget
                        handed_over = handed_over or (ran and name in finishing)
                    messages.append(result_message)
                    appended.append(result_message)
                rounds += 1

                if closing is not None:
                    if closing == 0:
                        return appended     # it would not stop calling; the UI reports that
                    closing -= 1
        finally:
            if nudge is not None:
                remove_message(messages, nudge)
            for result_message in refused:
                result_message['content'] = BUDGET_REOPENED.format(
                    name=result_message['name'])

    async def _complete_round(self, messages: List[dict], *, last: bool) -> dict:
        """One round of :meth:`run`.

        The *last* closing round forbids calls outright: the model has had its
        chance to hand over, now it answers in text or not at all.  Not every
        endpoint takes ``tool_choice: none`` (some know only ``auto`` and a
        named function), and this is the round meant to salvage an answer, so
        a refusal is retried as an ordinary round — any lookup the model makes
        then is refused by the caller anyway."""
        if not last:
            return await self.complete(messages)
        try:
            return await self.complete(messages, allow_tools=False)
        except LLMError:
            return await self.complete(messages)

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
            called.add(call_name(call))
            result_message, _ran = await self._run_tool_call(call, emit)
            messages.append(result_message)
            appended.append(result_message)
        return appended

    async def _run_tool_call(self, call: dict, emit) -> Tuple[dict, bool]:
        """Execute one tool call and build the ``role: tool`` reply for it.

        A failing tool is reported back to the model as its result rather than
        raised: the model can then correct itself (wrong table name, say)
        instead of the whole chat dying.

        Returns the reply and whether the handler ran to the end: False for
        arguments that do not parse, a call the user refused and a handler
        that raised.
        """
        name = call_name(call)
        raw_arguments = (call.get('function') or {}).get('arguments') or '{}'
        try:
            arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else dict(raw_arguments)
            if not isinstance(arguments, dict):
                raise ValueError('arguments must be a JSON object')
        except (ValueError, TypeError) as exc:
            arguments = {}
            content = f'Error: cannot parse arguments {raw_arguments!r}: {exc}'
            ran = False
        else:
            emit('tool', {'name': name, 'arguments': arguments})
            content, ran = await self._call_tool(name, arguments)
        emit('tool_result', {'name': name, 'content': content})
        return tool_reply(call, content), ran

    async def _call_tool(self, name: str, arguments: dict) -> Tuple[str, bool]:
        """The result of *name* for the model, and whether its handler ran to
        the end (not refused by the user, not raising)."""
        if self.tools is None:
            return f'Error: no tools are available (tried to call {name!r})', False
        decision = Decision()
        if self.approve is not None and self._asks_before(name):
            decision = await self.approve(name, arguments)
            if not decision.allowed:
                # The handler never runs; the model is told why instead.
                return with_note(decision.refusal, decision.note), False
            if decision.arguments is not None:
                arguments = decision.arguments
        ran = True
        try:
            result = await self.tools.call(name, arguments)
        except Exception as exc:   # a broken tool must not kill the conversation
            result = f'Error: {type(exc).__name__}: {exc}'
            ran = False
            if decision.arguments is None and not decision.note:
                return result, ran
        result = annotated_result(result, edited=decision.arguments, note=decision.note)
        return truncate_result(result, self.tools.result_limit(name)), ran

    def _asks_before(self, name: str) -> bool:
        """Whether *name* is put to the user before it runs, as things stand."""
        kind = self.tools.approval_kind(name)
        return {'tools': self.config.confirm_tools,
                'exec': self.config.confirm_exec}.get(kind, False)


def closing_nudge(rounds: int, finishing: set) -> dict:
    """The user's turn that tells the model its lookups are over (FINISH).

    It names the tools that hand the answer over, when there are any, so the
    model does not describe its answer in text and leave the Result pane empty."""
    if finishing:
        hand_over = (f'call {" or ".join(sorted(finishing))} with your best answer '
                     'so far')
    else:
        hand_over = 'answer now in text with your best answer so far'
    return {
        'role': 'user',
        'content': (f'You have made {rounds} rounds of tool calls and the budget for '
                    'this request is spent: the lookup tools are closed and any call '
                    f'to them will be refused. Finish with what you already have — '
                    f'{hand_over}, and say plainly what you could not check.'),
    }


def call_name(call: dict) -> str:
    """The tool a ``tool_calls`` entry names; '' when it is malformed."""
    return ((call.get('function') or {}).get('name')) or ''


def tool_reply(call: dict, content: str) -> dict:
    """The ``role: tool`` message answering *call* — with its result, or with
    why it was not run."""
    return {
        'role': 'tool',
        'tool_call_id': call.get('id', ''),
        'name': call_name(call),
        'content': content,
    }


def remove_message(messages: List[dict], message: dict) -> None:
    """Take *message* — that very dict, not an equal one — out of *messages*."""
    for index, candidate in enumerate(messages):
        if candidate is message:
            del messages[index]
            return


def annotated_result(result: Any, *, edited: Optional[dict] = None, note: str = '') -> Any:
    """The result of a call the user had a say in before it ran.

    *edited*: the arguments it actually ran with — the model has to be told,
    otherwise it reads the rows as the answer to what *it* asked.  *note*: the
    user's words to the model, sent as ``user_says``.  With neither, *result*
    is returned as it is."""
    if edited is None and not note:
        return result
    wrapped: dict = {}
    if edited is not None:
        wrapped.update({
            'edited_by_user': True,
            'ran_with': edited,
            'note': 'The user edited this call before it ran; the result is for '
                    'the edited version, not the one you sent.',
        })
    if note:
        wrapped['user_says'] = note
    wrapped['result'] = result
    return wrapped


def with_note(refusal: str, note: str) -> str:
    """A refusal with the user's words to the model appended, if any."""
    return f'{refusal}\nThe user says: {note}' if note else refusal


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
