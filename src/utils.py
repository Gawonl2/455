import hashlib
import numpy as np
from datetime import datetime, timezone


def sha256_hex(s: str) -> str:
    """sha256 of utf-8 encoded string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def compute_content_hash(cleaned_text: str) -> str:
    """sha256 of cleaned_text.strip()."""
    return sha256_hex(cleaned_text.strip())


def compute_chunk_id(doc_id, content_hash, start_tok, end_tok) -> str:
    """sha256 of f'{doc_id}|{content_hash}|{start_tok}|{end_tok}'."""
    key = f"{doc_id}|{content_hash}|{start_tok}|{end_tok}"
    return sha256_hex(key)


def compute_chunk_hash(chunk_text: str) -> str:
    """sha256 of chunk_text."""
    return sha256_hex(chunk_text)


def compute_embedding_hash(vector: np.ndarray) -> str:
    """sha256 of vector.tobytes()."""
    return hashlib.sha256(vector.tobytes()).hexdigest()


def now_utc() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def ts_str(dt: datetime) -> str:
    """Return isoformat string of datetime."""
    return dt.isoformat()
