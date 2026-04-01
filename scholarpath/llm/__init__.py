"""ScholarPath LLM integration layer.

Provides a Z.AI / OpenAI-compatible async client and prompt templates
for profile extraction, intent classification, query decomposition,
entity alignment, conflict detection, school evaluation, strategy
advice, causal narrative, and Go/No-Go reporting.
"""

from scholarpath.llm.client import LLMClient, get_llm_client
from scholarpath.llm.embeddings import EmbeddingService, get_embedding_service

__all__ = [
    "LLMClient",
    "get_llm_client",
    "EmbeddingService",
    "get_embedding_service",
]
