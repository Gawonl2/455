import torch

EMBEDDING_MODEL = "intfloat/e5-base-v2"


def get_embedding_device() -> str:
    """Prefer NVIDIA CUDA, then Apple MPS; otherwise CPU."""
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"
EMBEDDING_DIM = 768
PASSAGE_PREFIX = "passage: "
QUERY_PREFIX = "query: "

CHUNK_TOKENS = 384
OVERLAP = 64
STRIDE = CHUNK_TOKENS - OVERLAP  # 320
MIN_CHUNK_LEN = 32

IVF_NLIST = 100
IVF_NPROBE = 10

INGEST_BATCH_SIZE = 500
EMBED_BATCH_SIZE = 32

RANDOM_SEED = 42
