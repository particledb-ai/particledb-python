import os
import datetime
import unittest
import particledb


@unittest.skipUnless(os.environ.get("PDB_NATIVE_PORT"), "PDB_NATIVE_PORT required")
class PipelineDefinition(unittest.TestCase):
    def test_pipeline_ddl_and_invalid_identifiers(self):
        with particledb.ParticleDB(host="127.0.0.1", pdb_port=int(os.environ["PDB_NATIVE_PORT"]), transport="pdb_wire") as db:
            name = f"python_pipeline_{os.getpid()}"
            db.rag.create_pipeline(name, table="documents", vector_col="embedding", text_col="content")
            db.execute(f"DROP RAG PIPELINE {name}")
            for kwargs in ({"name": "bad; SELECT 1"}, {"mode": "rerank"}, {"k": 0}):
                args = dict(name=name, table="documents", vector_col="embedding", text_col="content")
                args.update(kwargs)
                with self.assertRaises(ValueError):
                    db.rag.create_pipeline(**args)

    def test_time_values_bind_and_decode_with_microseconds(self):
        expected = datetime.time(23, 59, 59, 123456)
        with particledb.connect(host="127.0.0.1", port=int(os.environ["PDB_NATIVE_PORT"])) as db:
            self.assertEqual(db.scalar("SELECT '23:59:59.123456'::time"), expected)
            self.assertEqual(db.scalar("SELECT $1::time", [expected]), expected)
            self.assertEqual(db.scalar("SELECT '24:00:00'::time"), datetime.time(0, 0))
        from particledb import dbapi
        with dbapi.connect(host="127.0.0.1", port=int(os.environ["PDB_NATIVE_PORT"])) as db:
            self.assertEqual(db.execute("SELECT %s::time", (expected,)).fetchone(), (expected,))
