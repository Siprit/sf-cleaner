"""Vector DB abstraction — supports pgvector and Pinecone backends."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod


class VectorStore(ABC):
    @abstractmethod
    async def upsert(self, vector_id: str, embedding: list[float], metadata: dict) -> None: ...

    @abstractmethod
    async def query(
        self, embedding: list[float], top_k: int = 1, min_score: float = 0.0
    ) -> list[dict]: ...


# ── pgvector backend ──────────────────────────────────────────────────────────

class PgVectorStore(VectorStore):
    def __init__(self):
        import psycopg2
        from pgvector.psycopg2 import register_vector

        dsn = (
            f"host={os.environ['POSTGRES_HOST']} "
            f"port={os.environ.get('POSTGRES_PORT', '5432')} "
            f"dbname={os.environ['POSTGRES_DB']} "
            f"user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']}"
        )
        self._conn = psycopg2.connect(dsn)
        register_vector(self._conn)
        self._ensure_table()

    def _ensure_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lead_vectors (
                    id          TEXT PRIMARY KEY,
                    embedding   vector(1536),
                    metadata    JSONB,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS lead_vectors_embedding_idx ON lead_vectors USING ivfflat (embedding vector_cosine_ops)")
        self._conn.commit()

    async def upsert(self, vector_id: str, embedding: list[float], metadata: dict) -> None:
        import json
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO lead_vectors (id, embedding, metadata)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, metadata = EXCLUDED.metadata, created_at = NOW()
            """, (vector_id, embedding, json.dumps(metadata)))
        self._conn.commit()

    async def query(self, embedding: list[float], top_k: int = 1, min_score: float = 0.0) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT id, metadata, 1 - (embedding <=> %s::vector) AS score
                FROM lead_vectors
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """, (embedding, embedding, top_k))
            rows = cur.fetchall()

        return [
            {"id": row[0], "metadata": row[1], "score": float(row[2])}
            for row in rows
            if float(row[2]) >= min_score
        ]


# ── Pinecone backend ──────────────────────────────────────────────────────────

class PineconeStore(VectorStore):
    def __init__(self):
        from pinecone import Pinecone

        pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
        self._index = pc.Index(os.environ["PINECONE_INDEX"])

    async def upsert(self, vector_id: str, embedding: list[float], metadata: dict) -> None:
        self._index.upsert(vectors=[{"id": vector_id, "values": embedding, "metadata": metadata}])

    async def query(self, embedding: list[float], top_k: int = 1, min_score: float = 0.0) -> list[dict]:
        result = self._index.query(vector=embedding, top_k=top_k, include_metadata=True)
        return [
            {"id": m["id"], "metadata": m.get("metadata", {}), "score": m["score"]}
            for m in result["matches"]
            if m["score"] >= min_score
        ]


# ── Factory ───────────────────────────────────────────────────────────────────

def get_vector_store() -> VectorStore:
    backend = os.getenv("VECTOR_BACKEND", "pgvector")
    if backend == "pinecone":
        return PineconeStore()
    return PgVectorStore()
