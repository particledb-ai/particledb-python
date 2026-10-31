"""AI and LLM operations for ParticleDB.

Provides methods for text generation, classification, summarization,
sentiment analysis, structured extraction, translation, embeddings,
prompt management, tool invocation, agent execution, knowledge base
search, and async job management -- all backed by ParticleDB SQL functions.

Example::

    from particledb import ParticleDB

    db = ParticleDB()

    # Generate text
    answer = db.ai.generate("Explain quantum computing in simple terms")

    # Classify text
    label = db.ai.classify("I love this product!", "positive", "negative", "neutral")

    # Run an agent
    result = db.ai.run_agent("research_agent", "Find recent papers on RAG")

    # Async job
    job_id = db.ai.submit_job("Summarize all customer feedback from Q1")
    status = db.ai.job_status(job_id)
    result = db.ai.job_result(job_id, timeout_ms=60000)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from particledb.client import ParticleDB


class AI:
    """AI and LLM operations backed by ParticleDB SQL functions.

    Typically accessed via ``ParticleDB.ai`` rather than instantiated directly.

    Groups functionality into:

    - **LLM functions** -- generate, classify, summarize, sentiment, extract,
      translate, embed
    - **Prompt management** -- render_prompt
    - **Tool invocation** -- invoke_tool
    - **Agent execution** -- run_agent, start_agent, agent_status, agent_result,
      agent_cancel
    - **Knowledge base** -- kb_search
    - **Async jobs** -- submit_job, job_status, job_result, job_cancel
    """

    def __init__(self, client: "ParticleDB"):
        self._client = client

    # ------------------------------------------------------------------
    # LLM functions
    # ------------------------------------------------------------------

    def generate(
        self,
        prompt: str,
        context: Optional[str] = None,
        model: Optional[str] = None,
    ) -> str:
        """Generate text from an LLM prompt.

        Parameters
        ----------
        prompt : str
            The prompt to send to the language model.
        context : str, optional
            Additional context to include with the prompt.
        model : str, optional
            Model identifier to use (e.g. ``"my-model"``).
            Uses the server default when ``None``.

        Returns
        -------
        str
            The generated text.

        Example
        -------
        >>> db.ai.generate("Explain quantum computing in simple terms")
        'Quantum computing uses quantum bits (qubits)...'

        >>> db.ai.generate(
        ...     "Summarize this document",
        ...     context="ParticleDB is a high-performance HTAP database...",
        ...     model="my-model",
        ... )
        'ParticleDB combines OLTP and OLAP workloads...'
        """
        params: list[Any] = [prompt]
        placeholders = "$1"

        if context is not None:
            params.append(context)
            placeholders += f", ${len(params)}"

        if model is not None:
            if context is None:
                params.append(None)
                placeholders += f", ${len(params)}"
            params.append(model)
            placeholders += f", ${len(params)}"

        sql = f"SELECT LLM_GENERATE({placeholders}) AS result"
        rows = self._client.query(sql, params)
        return self._extract_string(rows, "result")

    def classify(self, text: str, *labels: str) -> str:
        """Classify text into one of the given labels.

        Parameters
        ----------
        text : str
            The text to classify.
        *labels : str
            Two or more candidate labels.

        Returns
        -------
        str
            The label that best matches the text.

        Raises
        ------
        ValueError
            If fewer than two labels are provided.

        Example
        -------
        >>> db.ai.classify("I love this product!", "positive", "negative", "neutral")
        'positive'
        """
        if len(labels) < 2:
            raise ValueError("classify() requires at least two labels")

        params: list[Any] = [text, *labels]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(params)))
        sql = f"SELECT LLM_CLASSIFY({placeholders}) AS result"
        rows = self._client.query(sql, params)
        return self._extract_string(rows, "result")

    def summarize(self, text: str, max_words: int = 100) -> str:
        """Summarize text to a target length.

        Parameters
        ----------
        text : str
            The text to summarize.
        max_words : int
            Maximum number of words in the summary.  Default: ``100``.

        Returns
        -------
        str
            The summarized text.

        Example
        -------
        >>> db.ai.summarize("ParticleDB is a ... (long text)", max_words=50)
        'ParticleDB is a high-performance database combining...'
        """
        sql = "SELECT LLM_SUMMARIZE($1, $2) AS result"
        rows = self._client.query(sql, [text, max_words])
        return self._extract_string(rows, "result")

    def sentiment(self, text: str) -> str:
        """Analyse the sentiment of text.

        Parameters
        ----------
        text : str
            The text to analyse.

        Returns
        -------
        str
            Sentiment label (e.g. ``"positive"``, ``"negative"``, ``"neutral"``).

        Example
        -------
        >>> db.ai.sentiment("This is the best database I have ever used!")
        'positive'
        """
        sql = "SELECT LLM_SENTIMENT($1) AS result"
        rows = self._client.query(sql, [text])
        return self._extract_string(rows, "result")

    def extract(self, text: str, schema: dict) -> dict:
        """Extract structured data from text according to a JSON schema.

        Parameters
        ----------
        text : str
            The text to extract information from.
        schema : dict
            A JSON-compatible dict describing the fields to extract.  For
            example::

                {"name": "string", "age": "integer", "email": "string"}

        Returns
        -------
        dict
            Extracted data matching the requested schema.

        Example
        -------
        >>> db.ai.extract(
        ...     "John Doe is 30 years old and lives at john@example.com",
        ...     {"name": "string", "age": "integer", "email": "string"},
        ... )
        {'name': 'John Doe', 'age': 30, 'email': 'john@example.com'}
        """
        schema_json = json.dumps(schema)
        sql = "SELECT LLM_EXTRACT($1, $2) AS result"
        rows = self._client.query(sql, [text, schema_json])
        return self._extract_json(rows, "result")

    def translate(self, text: str, target_language: str) -> str:
        """Translate text into a target language.

        Parameters
        ----------
        text : str
            The text to translate.
        target_language : str
            Target language name or code (e.g. ``"French"``, ``"es"``,
            ``"zh-CN"``).

        Returns
        -------
        str
            The translated text.

        Example
        -------
        >>> db.ai.translate("Hello, how are you?", "Spanish")
        'Hola, como estas?'
        """
        sql = "SELECT LLM_TRANSLATE($1, $2) AS result"
        rows = self._client.query(sql, [text, target_language])
        return self._extract_string(rows, "result")

    def embed(self, text: str, model: Optional[str] = None) -> List[float]:
        """Generate a vector embedding for text.

        Parameters
        ----------
        text : str
            The text to embed.
        model : str, optional
            Embedding model identifier.  Uses the server default when ``None``.

        Returns
        -------
        list of float
            The embedding vector.

        Example
        -------
        >>> vec = db.ai.embed("ParticleDB is fast")
        >>> len(vec)
        1536
        """
        if model is not None:
            sql = "SELECT EMBED($1, $2) AS result"
            params: list[Any] = [text, model]
        else:
            sql = "SELECT EMBED($1) AS result"
            params = [text]

        rows = self._client.query(sql, params)
        return _parse_embedding(rows)

    def embed_batch(
        self, texts: List[str], model: Optional[str] = None
    ) -> List[List[float]]:
        """Generate embeddings for many texts, in input order.

        Over the native wire this is ONE prepared ``SELECT EMBED($1[, $2])``
        executed once per text, in order, on one connection; over HTTP it
        calls :meth:`embed` per text. Raises ``ParticleDBError`` if any text
        failed to embed.

        Example
        -------
        >>> vecs = db.ai.embed_batch(["first doc", "second doc"])
        >>> len(vecs)
        2
        """
        if not texts:
            return []
        wire = getattr(self._client, "_wire", None)
        if wire is None:
            return [self.embed(t, model=model) for t in texts]
        if model is not None:
            sql = "SELECT EMBED($1, $2) AS result"
            sets: List[List[Any]] = [[t, model] for t in texts]
        else:
            sql = "SELECT EMBED($1) AS result"
            sets = [[t] for t in texts]
        from particledb.client import ParticleDBError, _wrap_wire_error
        from particledb.pdb_wire import PdbError

        try:
            entries = wire.execute_batch(sql, sets)
        except PdbError as exc:
            raise _wrap_wire_error(exc) from exc
        out: List[List[float]] = []
        for i, entry in enumerate(entries):
            if isinstance(entry, Exception):
                raise ParticleDBError(f"embed_batch: text {i} failed: {entry}")
            out.append(_parse_embedding(entry if isinstance(entry, list) else []))
        if len(out) != len(texts):
            raise ParticleDBError(
                f"embed_batch: server answered {len(out)} of {len(texts)} texts"
            )
        return out

    # ------------------------------------------------------------------
    # Prompt management
    # ------------------------------------------------------------------

    def render_prompt(self, prompt_name: str, **variables: Any) -> str:
        """Render a named prompt template with variable substitution.

        Parameters
        ----------
        prompt_name : str
            The registered prompt template name.
        **variables
            Key-value pairs to substitute into the template.

        Returns
        -------
        str
            The fully rendered prompt string.

        Example
        -------
        >>> db.ai.render_prompt("greet", name="Alice", lang="English")
        'Hello Alice! How can I help you today?'
        """
        variables_json = json.dumps(variables)
        sql = "SELECT PROMPT_RENDER($1, $2) AS result"
        rows = self._client.query(sql, [prompt_name, variables_json])
        return self._extract_string(rows, "result")

    # ------------------------------------------------------------------
    # Tool invocation
    # ------------------------------------------------------------------

    def invoke_tool(self, tool_name: str, args: dict) -> str:
        """Invoke a registered tool by name.

        Parameters
        ----------
        tool_name : str
            The name of the tool to invoke.
        args : dict
            Arguments to pass to the tool as a JSON-compatible dict.

        Returns
        -------
        str
            The tool's output as a string.

        Example
        -------
        >>> db.ai.invoke_tool("web_search", {"query": "ParticleDB benchmarks"})
        '{"results": [...]}'
        """
        args_json = json.dumps(args)
        sql = "SELECT TOOL_INVOKE($1, $2) AS result"
        rows = self._client.query(sql, [tool_name, args_json])
        return self._extract_string(rows, "result")

    # ------------------------------------------------------------------
    # Agent execution
    # ------------------------------------------------------------------

    def run_agent(self, agent_name: str, task: str) -> str:
        """Run an agent synchronously and return its final output.

        Blocks until the agent completes.  For long-running agents, prefer
        :meth:`start_agent` with :meth:`agent_result`.

        Parameters
        ----------
        agent_name : str
            The registered agent name.
        task : str
            The task description or instruction for the agent.

        Returns
        -------
        str
            The agent's final output.

        Example
        -------
        >>> db.ai.run_agent("research_agent", "Find papers on vector databases")
        'Here are the top papers on vector databases...'
        """
        sql = "SELECT AGENT_RUN($1, $2) AS result"
        rows = self._client.query(sql, [agent_name, task])
        return self._extract_string(rows, "result")

    def start_agent(self, agent_name: str, task: str) -> str:
        """Start an agent asynchronously and return a run ID.

        Use :meth:`agent_status`, :meth:`agent_result`, and
        :meth:`agent_cancel` to manage the running agent.

        Parameters
        ----------
        agent_name : str
            The registered agent name.
        task : str
            The task description or instruction for the agent.

        Returns
        -------
        str
            A unique run ID for tracking the agent execution.

        Example
        -------
        >>> run_id = db.ai.start_agent("research_agent", "Analyse competitors")
        >>> status = db.ai.agent_status(run_id)
        """
        sql = "SELECT AGENT_START($1, $2) AS run_id"
        rows = self._client.query(sql, [agent_name, task])
        return self._extract_string(rows, "run_id")

    def agent_status(self, run_id: str) -> dict:
        """Check the status of a running agent.

        Parameters
        ----------
        run_id : str
            The run ID returned by :meth:`start_agent`.

        Returns
        -------
        dict
            Status information with keys such as ``"status"``
            (``"running"``, ``"completed"``, ``"failed"``),
            ``"progress"``, and ``"elapsed_ms"``.

        Example
        -------
        >>> db.ai.agent_status(run_id)
        {'status': 'running', 'progress': 0.45, 'elapsed_ms': 12340}
        """
        sql = "SELECT AGENT_STATUS($1) AS result"
        rows = self._client.query(sql, [run_id])
        return self._extract_json(rows, "result")

    def agent_result(self, run_id: str, timeout_ms: int = 30000) -> str:
        """Wait for an agent to finish and return its result.

        Parameters
        ----------
        run_id : str
            The run ID returned by :meth:`start_agent`.
        timeout_ms : int
            Maximum time to wait in milliseconds.  Default: ``30000`` (30 s).

        Returns
        -------
        str
            The agent's final output.

        Example
        -------
        >>> result = db.ai.agent_result(run_id, timeout_ms=60000)
        """
        sql = "SELECT AGENT_RESULT($1, $2) AS result"
        rows = self._client.query(sql, [run_id, timeout_ms])
        return self._extract_string(rows, "result")

    def agent_cancel(self, run_id: str) -> bool:
        """Cancel a running agent.

        Parameters
        ----------
        run_id : str
            The run ID returned by :meth:`start_agent`.

        Returns
        -------
        bool
            ``True`` if the agent was successfully cancelled.

        Example
        -------
        >>> db.ai.agent_cancel(run_id)
        True
        """
        sql = "SELECT AGENT_CANCEL($1) AS cancelled"
        rows = self._client.query(sql, [run_id])
        if not rows:
            return False
        val = rows[0].get("cancelled")
        return val is True or val == "true"

    # ------------------------------------------------------------------
    # Knowledge base
    # ------------------------------------------------------------------

    def kb_search(
        self, kb_name: str, query: str, top_k: int = 5
    ) -> List[dict]:
        """Search a knowledge base for relevant documents.

        Parameters
        ----------
        kb_name : str
            The name of the knowledge base to search.
        query : str
            The search query (natural language).
        top_k : int
            Maximum number of results to return.  Default: ``5``.

        Returns
        -------
        list of dict
            Each dict contains at minimum ``"content"`` and ``"score"`` keys,
            plus any metadata fields stored in the knowledge base.

        Example
        -------
        >>> results = db.ai.kb_search("product_docs", "How to configure backups?")
        >>> for r in results:
        ...     print(r["score"], r["content"][:80])
        0.92 To configure backups, navigate to Settings > Backup and select...
        """
        sql = "SELECT KB_SEARCH($1, $2, $3) AS result"
        rows = self._client.query(sql, [kb_name, query, top_k])
        if not rows or rows[0].get("result") is None:
            return []
        raw = rows[0]["result"]
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        return []

    # ------------------------------------------------------------------
    # Async jobs
    # ------------------------------------------------------------------

    def submit_job(self, task: str, model: Optional[str] = None) -> str:
        """Submit an asynchronous AI job.

        Parameters
        ----------
        task : str
            The task description or prompt for the job.
        model : str, optional
            Model identifier.  Uses the server default when ``None``.

        Returns
        -------
        str
            A unique job ID for tracking.

        Example
        -------
        >>> job_id = db.ai.submit_job("Summarize all customer feedback from Q1")
        >>> db.ai.job_status(job_id)
        {'status': 'running', 'progress': 0.1}
        """
        if model is not None:
            sql = "SELECT AI_SUBMIT($1, $2) AS job_id"
            params: list[Any] = [task, model]
        else:
            sql = "SELECT AI_SUBMIT($1) AS job_id"
            params = [task]

        rows = self._client.query(sql, params)
        return self._extract_string(rows, "job_id")

    def job_status(self, job_id: str) -> dict:
        """Check the status of an async AI job.

        Parameters
        ----------
        job_id : str
            The job ID returned by :meth:`submit_job`.

        Returns
        -------
        dict
            Status information with keys such as ``"status"``
            (``"pending"``, ``"running"``, ``"completed"``, ``"failed"``),
            ``"progress"``, and ``"elapsed_ms"``.

        Example
        -------
        >>> db.ai.job_status(job_id)
        {'status': 'completed', 'progress': 1.0, 'elapsed_ms': 5432}
        """
        sql = "SELECT AI_STATUS($1) AS result"
        rows = self._client.query(sql, [job_id])
        return self._extract_json(rows, "result")

    def job_result(self, job_id: str, timeout_ms: int = 30000) -> str:
        """Wait for a job to finish and return its result.

        Parameters
        ----------
        job_id : str
            The job ID returned by :meth:`submit_job`.
        timeout_ms : int
            Maximum time to wait in milliseconds.  Default: ``30000`` (30 s).

        Returns
        -------
        str
            The job's output.

        Example
        -------
        >>> result = db.ai.job_result(job_id, timeout_ms=60000)
        """
        sql = "SELECT AI_RESULT($1, $2) AS result"
        rows = self._client.query(sql, [job_id, timeout_ms])
        return self._extract_string(rows, "result")

    def job_cancel(self, job_id: str) -> bool:
        """Cancel a pending or running AI job.

        Parameters
        ----------
        job_id : str
            The job ID returned by :meth:`submit_job`.

        Returns
        -------
        bool
            ``True`` if the job was successfully cancelled.

        Example
        -------
        >>> db.ai.job_cancel(job_id)
        True
        """
        sql = "SELECT AI_CANCEL($1) AS cancelled"
        rows = self._client.query(sql, [job_id])
        if not rows:
            return False
        val = rows[0].get("cancelled")
        return val is True or val == "true"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_string(rows: List[Dict[str, Any]], key: str) -> str:
        """Extract a string value from a single-row query result."""
        if not rows or rows[0].get(key) is None:
            return ""
        return str(rows[0][key])

    @staticmethod
    def _extract_json(rows: List[Dict[str, Any]], key: str) -> dict:
        """Extract and parse a JSON object from a single-row query result."""
        if not rows or rows[0].get(key) is None:
            return {}
        raw = rows[0][key]
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    def __repr__(self) -> str:
        return f"AI(client={self._client!r})"


def _parse_embedding(rows: List[Dict[str, Any]]) -> List[float]:
    """Decode the ``result`` column of a ``SELECT EMBED(...)`` result."""
    if not rows or rows[0].get("result") is None:
        return []
    raw = rows[0]["result"]
    if isinstance(raw, list):
        return [float(v) for v in raw]
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [float(v) for v in parsed]
    return []
