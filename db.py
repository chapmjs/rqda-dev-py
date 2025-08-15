\
from __future__ import annotations

import os
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy import (
    create_engine, text
)
from sqlalchemy.engine import Engine, Row

def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def get_engine() -> Engine:
    host = _env("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    user = _env("DB_USER")
    pwd  = _env("DB_PASS")
    db   = _env("DB_NAME")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
    eng = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    return eng

def init_db(engine: Engine) -> None:
    with engine.begin() as conn:
        # minimal schema for Phase 0
        conn.execute(text("""\
CREATE TABLE IF NOT EXISTS documents (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  filename VARCHAR(255) NOT NULL,
  content LONGTEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_filename (filename)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""))
        conn.execute(text("""\
CREATE TABLE IF NOT EXISTS codes (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(190) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_code_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""))
        conn.execute(text("""\
CREATE TABLE IF NOT EXISTS coded_segments (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  document_id BIGINT NOT NULL,
  code_id BIGINT NOT NULL,
  start_offset INT NOT NULL,
  end_offset   INT NOT NULL,
  selected_text TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
  FOREIGN KEY (code_id) REFERENCES codes(id) ON DELETE CASCADE,
  KEY doc_idx (document_id),
  KEY code_idx (code_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""))
        # seed a default code if empty
        res = conn.execute(text("SELECT COUNT(*) AS n FROM codes"))
        if res.scalar_one() == 0:
            conn.execute(text("INSERT INTO codes(name) VALUES(:n)"), {"n": "Example"})

def upsert_document(engine: Engine, filename: str, content: str) -> int:
    with engine.begin() as conn:
        # Try update
        r = conn.execute(text("SELECT id FROM documents WHERE filename=:f"), {"f": filename}).first()
        if r:
            conn.execute(text("UPDATE documents SET content=:c WHERE id=:id"), {"c": content, "id": r.id})
            return int(r.id)
        else:
            res = conn.execute(
                text("INSERT INTO documents(filename, content) VALUES (:f, :c)"),
                {"f": filename, "c": content}
            )
            return int(res.lastrowid)

def get_document(engine: Engine, doc_id: int) -> dict | None:
    with engine.begin() as conn:
        r = conn.execute(text("SELECT id, filename, content FROM documents WHERE id=:id"), {"id": doc_id}).first()
        return dict(r._mapping) if r else None

def find_document_by_name(engine: Engine, filename: str) -> dict | None:
    with engine.begin() as conn:
        r = conn.execute(text("SELECT id, filename, content FROM documents WHERE filename=:f"), {"f": filename}).first()
        return dict(r._mapping) if r else None

def list_codes(engine: Engine) -> list[dict]:
    with engine.begin() as conn:
        res = conn.execute(text("SELECT id, name FROM codes ORDER BY name"))
        return [dict(r._mapping) for r in res.fetchall()]

def create_code(engine: Engine, name: str) -> int:
    with engine.begin() as conn:
        # on duplicate ignore
        conn.execute(text("INSERT IGNORE INTO codes(name) VALUES(:n)"), {"n": name})
        r = conn.execute(text("SELECT id FROM codes WHERE name=:n"), {"n": name}).first()
        return int(r.id)

def insert_segment(engine: Engine, document_id: int, code_id: int, start: int, end: int, selected_text: str) -> int:
    with engine.begin() as conn:
        res = conn.execute(text("""\
INSERT INTO coded_segments(document_id, code_id, start_offset, end_offset, selected_text)
VALUES (:doc, :code, :s, :e, :t)
"""), {"doc": document_id, "code": code_id, "s": start, "e": end, "t": selected_text})
        return int(res.lastrowid)

def list_segments(engine: Engine, document_id: int) -> list[dict]:
    with engine.begin() as conn:
        res = conn.execute(text("""\
SELECT id, document_id, code_id, start_offset, end_offset, selected_text, created_at
FROM coded_segments
WHERE document_id=:doc
ORDER BY start_offset ASC
"""), {"doc": document_id})
        return [dict(r._mapping) for r in res.fetchall()]
