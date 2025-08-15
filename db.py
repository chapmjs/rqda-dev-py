from __future__ import annotations

import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import (
    create_engine, text, event
)
from sqlalchemy.engine import Engine, Row
from sqlalchemy.exc import IntegrityError, OperationalError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    
    # Enhanced connection string with SSL and better handling
    connection_args = {
        'charset': 'utf8mb4',
        'ssl_disabled': False,  # Enable SSL by default
        'connect_timeout': 30,
        'read_timeout': 30,
        'write_timeout': 30,
    }
    
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    
    # Enhanced engine configuration
    eng = create_engine(
        url,
        connect_args=connection_args,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        echo=False  # Set to True for SQL debugging
    )
    
    # Test connection on startup
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Successfully connected to database at {host}:{port}")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise RuntimeError(f"Database connection failed: {e}")
    
    return eng

# Add connection event listeners for better error handling
@event.listens_for(Engine, "connect")
def set_mysql_strict_mode(dbapi_connection, connection_record):
    """Set MySQL to strict mode for better data integrity."""
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("SET sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
        cursor.execute("SET SESSION time_zone = '+00:00'")  # Use UTC
        cursor.close()
    except Exception as e:
        logger.warning(f"Could not set MySQL strict mode: {e}")

def init_db(engine: Engine) -> None:
    """Initialize database schema with enhanced constraints and indexing."""
    try:
        with engine.begin() as conn:
            # Documents table with better constraints
            conn.execute(text("""\
CREATE TABLE IF NOT EXISTS documents (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  filename VARCHAR(255) NOT NULL,
  content LONGTEXT NOT NULL,
  content_hash VARCHAR(64) NULL,  -- For deduplication
  file_size BIGINT NOT NULL DEFAULT 0,
  char_count BIGINT NOT NULL DEFAULT 0,
  encoding VARCHAR(50) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_filename (filename),
  KEY idx_created_at (created_at),
  KEY idx_file_size (file_size)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""))

            # Codes table with better indexing
            conn.execute(text("""\
CREATE TABLE IF NOT EXISTS codes (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(190) NOT NULL,
  description TEXT NULL,
  color VARCHAR(7) NULL,  -- For future UI enhancements
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_code_name (name),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""))

            # Coded segments with enhanced constraints
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
  KEY idx_document_offset (document_id, start_offset),
  KEY idx_code_segments (code_id),
  KEY idx_created_at (created_at),
  -- Ensure valid offset ranges
  CONSTRAINT chk_valid_offsets CHECK (start_offset >= 0 AND end_offset > start_offset),
  -- Prevent exact duplicates
  UNIQUE KEY uniq_segment (document_id, code_id, start_offset, end_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""))

            # Add some useful views for analytics (optional)
            conn.execute(text("""\
CREATE OR REPLACE VIEW coding_stats AS
SELECT 
  d.filename,
  c.name as code_name,
  COUNT(cs.id) as segment_count,
  AVG(cs.end_offset - cs.start_offset) as avg_segment_length,
  MIN(cs.created_at) as first_coded,
  MAX(cs.created_at) as last_coded
FROM documents d
LEFT JOIN coded_segments cs ON d.id = cs.document_id
LEFT JOIN codes c ON cs.code_id = c.id
GROUP BY d.id, d.filename, c.id, c.name
ORDER BY d.filename, c.name;
"""))

            # Seed default data if tables are empty
            res = conn.execute(text("SELECT COUNT(*) AS n FROM codes"))
            if res.scalar_one() == 0:
                conn.execute(text("""\
INSERT INTO codes(name, description) VALUES
('Important', 'Key findings or important information'),
('Question', 'Areas that need further investigation'),
('Theme', 'Recurring themes or patterns')
"""))
                logger.info("Initialized database with default codes")
                
        logger.info("Database schema initialized successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

def _calculate_content_hash(content: str) -> str:
    """Calculate SHA-256 hash of content for deduplication."""
    import hashlib
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def upsert_document(engine: Engine, filename: str, content: str) -> int:
    """
    Insert or update document with proper race condition handling.
    Uses INSERT ... ON DUPLICATE KEY UPDATE for atomic operation.
    """
    content_hash = _calculate_content_hash(content)
    file_size = len(content.encode('utf-8'))
    char_count = len(content)
    
    try:
        with engine.begin() as conn:
            # Use INSERT ... ON DUPLICATE KEY UPDATE for atomic upsert
            result = conn.execute(text("""\
INSERT INTO documents (filename, content, content_hash, file_size, char_count)
VALUES (:filename, :content, :content_hash, :file_size, :char_count)
ON DUPLICATE KEY UPDATE
  content = VALUES(content),
  content_hash = VALUES(content_hash),
  file_size = VALUES(file_size),
  char_count = VALUES(char_count),
  updated_at = CURRENT_TIMESTAMP
"""), {
                "filename": filename,
                "content": content,
                "content_hash": content_hash,
                "file_size": file_size,
                "char_count": char_count
            })
            
            # Get the document ID
            if result.lastrowid:  # New insert
                doc_id = result.lastrowid
                logger.info(f"Created new document: {filename} (ID: {doc_id})")
            else:  # Update case
                doc_result = conn.execute(
                    text("SELECT id FROM documents WHERE filename = :filename"),
                    {"filename": filename}
                )
                doc_id = doc_result.scalar_one()
                logger.info(f"Updated existing document: {filename} (ID: {doc_id})")
                
            return int(doc_id)
            
    except Exception as e:
        logger.error(f"Error upserting document {filename}: {e}")
        raise

def get_document(engine: Engine, doc_id: int) -> Optional[Dict[str, Any]]:
    """Get full document by ID with error handling."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""\
SELECT id, filename, content, content_hash, file_size, char_count, 
       encoding, created_at, updated_at
FROM documents 
WHERE id = :id
"""), {"id": doc_id})
            
            row = result.first()
            return dict(row._mapping) if row else None
            
    except Exception as e:
        logger.error(f"Error getting document {doc_id}: {e}")
        raise

def get_document_preview(engine: Engine, doc_id: int, max_chars: int = 1000000) -> str:
    """
    Get document preview for large documents.
    Uses MySQL's LEFT function for efficient substring extraction.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""\
SELECT LEFT(content, :max_chars) as preview
FROM documents 
WHERE id = :id
"""), {"id": doc_id, "max_chars": max_chars})
            
            row = result.first()
            return row.preview if row else ""
            
    except Exception as e:
        logger.error(f"Error getting document preview {doc_id}: {e}")
        raise

def find_document_by_name(engine: Engine, filename: str) -> Optional[Dict[str, Any]]:
    """Find document by exact filename match."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""\
SELECT id, filename, content, content_hash, file_size, char_count,
       encoding, created_at, updated_at
FROM documents 
WHERE filename = :filename
"""), {"filename": filename})
            
            row = result.first()
            return dict(row._mapping) if row else None
            
    except Exception as e:
        logger.error(f"Error finding document by name {filename}: {e}")
        raise

def list_codes(engine: Engine) -> List[Dict[str, Any]]:
    """List all codes with enhanced information."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""\
SELECT c.id, c.name, c.description, c.color,
       COUNT(cs.id) as usage_count,
       c.created_at, c.updated_at
FROM codes c
LEFT JOIN coded_segments cs ON c.id = cs.code_id
GROUP BY c.id, c.name, c.description, c.color, c.created_at, c.updated_at
ORDER BY c.name
"""))
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Error listing codes: {e}")
        raise

def create_code(engine: Engine, name: str, description: str = None, color: str = None) -> int:
    """
    Create new code with proper race condition handling.
    Uses INSERT IGNORE to handle concurrent creation attempts.
    """
    try:
        with engine.begin() as conn:
            # First, try to insert the new code
            result = conn.execute(text("""\
INSERT IGNORE INTO codes (name, description, color) 
VALUES (:name, :description, :color)
"""), {
                "name": name, 
                "description": description, 
                "color": color
            })
            
            # Get the code ID (works for both new inserts and existing codes)
            id_result = conn.execute(
                text("SELECT id FROM codes WHERE name = :name"), 
                {"name": name}
            )
            code_id = id_result.scalar_one()
            
            if result.rowcount > 0:
                logger.info(f"Created new code: {name} (ID: {code_id})")
            else:
                logger.info(f"Code already exists: {name} (ID: {code_id})")
                
            return int(code_id)
            
    except Exception as e:
        logger.error(f"Error creating code {name}: {e}")
        raise

def insert_segment(engine: Engine, document_id: int, code_id: int, 
                  start: int, end: int, selected_text: str) -> int:
    """
    Insert coded segment with validation and duplicate prevention.
    """
    # Validate inputs
    if start < 0 or end <= start:
        raise ValueError(f"Invalid offset range: {start}-{end}")
    
    if not selected_text.strip():
        raise ValueError("Selected text cannot be empty")
    
    try:
        with engine.begin() as conn:
            # Check if document and code exist
            doc_check = conn.execute(
                text("SELECT id FROM documents WHERE id = :id"), 
                {"id": document_id}
            ).first()
            if not doc_check:
                raise ValueError(f"Document {document_id} not found")
                
            code_check = conn.execute(
                text("SELECT id FROM codes WHERE id = :id"), 
                {"id": code_id}
            ).first()
            if not code_check:
                raise ValueError(f"Code {code_id} not found")
            
            # Insert segment (UNIQUE constraint will prevent exact duplicates)
            try:
                result = conn.execute(text("""\
INSERT INTO coded_segments (document_id, code_id, start_offset, end_offset, selected_text)
VALUES (:doc_id, :code_id, :start, :end, :text)
"""), {
                    "doc_id": document_id,
                    "code_id": code_id, 
                    "start": start,
                    "end": end,
                    "text": selected_text
                })
                
                segment_id = result.lastrowid
                logger.info(f"Created segment: doc={document_id}, code={code_id}, {start}-{end}")
                return int(segment_id)
                
            except IntegrityError as e:
                if "uniq_segment" in str(e).lower():
                    # Duplicate segment - return existing ID
                    existing = conn.execute(text("""\
SELECT id FROM coded_segments 
WHERE document_id = :doc_id AND code_id = :code_id 
  AND start_offset = :start AND end_offset = :end
"""), {
                        "doc_id": document_id,
                        "code_id": code_id,
                        "start": start, 
                        "end": end
                    }).first()
                    
                    if existing:
                        logger.info(f"Segment already exists: {existing.id}")
                        return int(existing.id)
                    
                raise  # Re-raise if it's a different integrity error
                
    except Exception as e:
        logger.error(f"Error inserting segment: {e}")
        raise

def list_segments(engine: Engine, document_id: int) -> List[Dict[str, Any]]:
    """
    List all coded segments for a document with code information.
    Optimized query to reduce memory usage for large documents.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""\
SELECT 
  cs.id, cs.document_id, cs.code_id, 
  cs.start_offset, cs.end_offset, 
  cs.selected_text, cs.created_at,
  c.name as code_name,
  c.color as code_color
FROM coded_segments cs
JOIN codes c ON cs.code_id = c.id
WHERE cs.document_id = :doc_id
ORDER BY cs.start_offset ASC, cs.created_at ASC
"""), {"doc_id": document_id})
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Error listing segments for document {document_id}: {e}")
        raise

def delete_segment(engine: Engine, segment_id: int) -> bool:
    """Delete a coded segment by ID."""
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM coded_segments WHERE id = :id"),
                {"id": segment_id}
            )
            
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"Deleted segment: {segment_id}")
            else:
                logger.warning(f"Segment not found: {segment_id}")
                
            return deleted
            
    except Exception as e:
        logger.error(f"Error deleting segment {segment_id}: {e}")
        raise

def get_document_stats(engine: Engine, document_id: int) -> Dict[str, Any]:
    """Get statistics for a document."""
    try:
        with engine.connect() as conn:
            # Get document info
            doc_result = conn.execute(text("""\
SELECT filename, char_count, file_size, created_at
FROM documents WHERE id = :id
"""), {"id": document_id}).first()
            
            if not doc_result:
                return {}
            
            # Get coding stats
            stats_result = conn.execute(text("""\
SELECT 
  COUNT(cs.id) as total_segments,
  COUNT(DISTINCT cs.code_id) as unique_codes,
  AVG(cs.end_offset - cs.start_offset) as avg_segment_length,
  MIN(cs.start_offset) as first_coded_position,
  MAX(cs.end_offset) as last_coded_position
FROM coded_segments cs
WHERE cs.document_id = :id
"""), {"id": document_id}).first()
            
            return {
                "filename": doc_result.filename,
                "char_count": doc_result.char_count,
                "file_size": doc_result.file_size,
                "created_at": doc_result.created_at,
                "total_segments": stats_result.total_segments or 0,
                "unique_codes": stats_result.unique_codes or 0,
                "avg_segment_length": float(stats_result.avg_segment_length or 0),
                "first_coded_position": stats_result.first_coded_position,
                "last_coded_position": stats_result.last_coded_position,
                "coding_coverage": (
                    (stats_result.last_coded_position - stats_result.first_coded_position) / doc_result.char_count * 100
                    if stats_result.first_coded_position is not None and doc_result.char_count > 0
                    else 0
                )
            }
            
    except Exception as e:
        logger.error(f"Error getting document stats {document_id}: {e}")
        raise

def cleanup_orphaned_segments(engine: Engine) -> int:
    """
    Clean up any orphaned segments (shouldn't happen with foreign keys, but useful for maintenance).
    Returns number of segments cleaned up.
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""\
DELETE cs FROM coded_segments cs
LEFT JOIN documents d ON cs.document_id = d.id
LEFT JOIN codes c ON cs.code_id = c.id
WHERE d.id IS NULL OR c.id IS NULL
"""))
            
            cleaned_count = result.rowcount
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} orphaned segments")
                
            return cleaned_count
            
    except Exception as e:
        logger.error(f"Error cleaning up orphaned segments: {e}")
        raise

def get_database_stats(engine: Engine) -> Dict[str, Any]:
    """Get overall database statistics."""
    try:
        with engine.connect() as conn:
            # Document stats
            doc_stats = conn.execute(text("""\
SELECT 
  COUNT(*) as document_count,
  SUM(char_count) as total_characters,
  SUM(file_size) as total_file_size,
  AVG(char_count) as avg_document_length
FROM documents
""")).first()
            
            # Code stats
            code_stats = conn.execute(text("""\
SELECT COUNT(*) as code_count FROM codes
""")).first()
            
            # Segment stats
            segment_stats = conn.execute(text("""\
SELECT 
  COUNT(*) as segment_count,
  AVG(end_offset - start_offset) as avg_segment_length
FROM coded_segments
""")).first()
            
            return {
                "documents": {
                    "count": doc_stats.document_count or 0,
                    "total_characters": doc_stats.total_characters or 0,
                    "total_file_size": doc_stats.total_file_size or 0,
                    "avg_length": float(doc_stats.avg_document_length or 0)
                },
                "codes": {
                    "count": code_stats.code_count or 0
                },
                "segments": {
                    "count": segment_stats.segment_count or 0,
                    "avg_length": float(segment_stats.avg_segment_length or 0)
                }
            }
            
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise
