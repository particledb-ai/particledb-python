"""RAG (Retrieval-Augmented Generation) operations for ParticleDB.

Provides methods to execute RAG queries that combine vector similarity search
with text retrieval, and to manage named RAG pipelines.

Example::

    from particledb import ParticleDB

    db = ParticleDB()

    # Create a pipeline
    db.rag.create_pipeline(
        "qa",
        table="documents",
        vector_col="embedding",
        text_col="content",
        k=5,
        mode="hybrid",
    )

    # Query it
    answer = db.rag.query("What is ParticleDB?", pipeline="qa")
    print(answer)
"""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

if TYPE_CHECKING:
    from particledb.client import ParticleDB


class RAG:
    """RAG operations backed by ParticleDB SQL functions.

    Typically accessed via ``ParticleDB.rag`` rather than instantiated directly.
    """

    def __init__(self, client: "ParticleDB"):
        self._client = client

    def query(
        self,
        question: str,
        *,
        pipeline: Optional[str] = None,
        table: str = "documents",
        vector_col: str = "embedding",
        text_col: str = "content",
        k: int = 5,
    ) -> Optional[str]:
        """Execute a RAG query.

        You can either specify a named *pipeline* or provide the table/column
        details inline.

        Parameters
        ----------
        question : str
            The natural-language question to answer.
        pipeline : str, optional
            Named pipeline to use.  If provided, the ``table``/``vector_col``/
            ``text_col``/``k`` arguments are ignored.
        table : str
            Table to query.  Default: ``"documents"``.
        vector_col : str
            Vector column name.  Default: ``"embedding"``.
        text_col : str
            Text column name.  Default: ``"content"``.
        k : int
            Number of nearest neighbors to retrieve.  Default: ``5``.

        Returns
        -------
        str or None
            The generated answer, or ``None`` if no answer was produced.

        Example
        -------
        >>> db.rag.query("What is ParticleDB?", pipeline="qa")
        'ParticleDB is a high-performance HTAP database...'
        """
        if pipeline:
            rows = self._client.query(
                "SELECT RAG($1, $2) AS answer",
                [question, pipeline],
            )
        else:
            rows = self._client.query(
                "SELECT RAG_QUERY($1, $2, $3, $4, $5) AS answer",
                [question, table, vector_col, text_col, k],
            )

        if not rows:
            return None
        return rows[0].get("answer")

    def ingest(
        self,
        table: str,
        content: str,
        embedding: Sequence[float],
        vector_column: str = "embedding",
        text_column: str = "content",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Ingest a document into a RAG table.

        Stores the text content alongside its vector embedding for later
        retrieval.

        Parameters
        ----------
        table : str
            Target table name.
        content : str
            The text content to ingest.
        embedding : list of float
            The vector embedding for the content.
        vector_column : str
            Vector column name.  Default: ``"embedding"``.
        text_column : str
            Text column name.  Default: ``"content"``.
        metadata : dict, optional
            Additional column values to store alongside the document.

        Example
        -------
        >>> db.rag.ingest(
        ...     "documents",
        ...     "ParticleDB is a fast database.",
        ...     [0.1, 0.2, 0.3],
        ...     metadata={"source": "docs", "page": 1},
        ... )
        """
        data: Dict[str, Any] = dict(metadata) if metadata else {}
        data[text_column] = content
        data[vector_column] = "[" + ",".join(str(v) for v in embedding) + "]"

        col_names = list(data.keys())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(col_names)))
        col_list = ", ".join(col_names)

        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        self._client.execute(sql, list(data.values()))

    def create_pipeline(
        self,
        name: str,
        *,
        table: str,
        vector_col: str,
        text_col: str,
        k: int = 5,
        mode: str = "vector",
    ) -> None:
        """Create a named RAG pipeline for reuse.

        Parameters
        ----------
        name : str
            Unique pipeline name.
        table : str
            Table containing document vectors and text.
        vector_col : str
            Column with vector embeddings.
        text_col : str
            Column with raw text.
        k : int
            Number of nearest neighbors.  Default: ``5``.
        mode : str
            RAG mode (``"vector"`` or ``"hybrid"``).
            Default: ``"vector"``. ``"simple"`` is a compatibility alias for ``"vector"``.

        Example
        -------
        >>> db.rag.create_pipeline(
        ...     "docs_pipeline",
        ...     table="documents",
        ...     vector_col="embedding",
        ...     text_col="content",
        ...     k=5,
        ...     mode="hybrid",
        ... )
        """
        for value, qualified in ((name, False), (table, True), (vector_col, False), (text_col, False)):
            pattern = r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*" if qualified else r"[A-Za-z_][A-Za-z0-9_]*"
            if not isinstance(value, str) or not re.fullmatch(pattern, value):
                raise ValueError("pipeline names and columns must be simple SQL identifiers")
        if isinstance(k, bool) or not isinstance(k, int) or k <= 0:
            raise ValueError("k must be a positive integer")
        if mode == "simple":
            mode = "vector"
        if mode not in ("vector", "hybrid"):
            raise ValueError("mode must be vector or hybrid")
        self._client.execute(
            f"CREATE RAG PIPELINE {name} TABLE {table} VECTOR_COLUMN {vector_col} "
            f"TEXT_COLUMN {text_col} TOP_K {k} MODE {mode}"
        )

    def __repr__(self) -> str:
        return f"RAG(client={self._client!r})"
