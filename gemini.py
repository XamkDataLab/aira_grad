"""
Legacy GeminiClient module - now imports from llm_client for backwards compatibility.
New code should use LLMClient from llm_client.py directly.
"""

from llm_client import LLMClient, GeminiClient, DelphiPanel

# Re-export for backwards compatibility
__all__ = ['GeminiClient', 'LLMClient', 'DelphiPanel']
