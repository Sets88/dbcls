"""Optional LLM chat for dbcls.

The pieces live in submodules and are imported only when the feature is
actually configured — see :mod:`dbcls.llm.plugin`, which checks for a base URL
and a model before pulling in the chat window:

* :mod:`dbcls.llm.client` — the OpenAI-compatible endpoint and the tool loop
* :mod:`dbcls.llm.tools`  — the read-only database tools offered to the model
* :mod:`dbcls.llm.prompt` — the system prompt and reading the query back out
* :mod:`dbcls.llm.chat`   — the three-pane chat window

Nothing is imported here, so ``import dbcls.llm`` stays free.
"""
