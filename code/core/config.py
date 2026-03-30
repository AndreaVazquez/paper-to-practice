"""
Helpers for reading role-specific LLM configuration from .env.
All consumers should use these helpers rather than calling decouple directly,
so that .env key naming is centralised here.
"""
from decouple import config


def get_role_provider(role: str) -> str:
    return config(f"{role}_PROVIDER", default="groq").lower()


def get_role_model(role: str) -> str:
    return config(f"{role}_MODEL")


def get_role_api_key(role: str, index: int = 1) -> str:
    """
    For roles with multiple keys (e.g. REASONING_API_KEY_1, REASONING_API_KEY_2),
    pass index=1 or index=2. For single-key roles, index is ignored and
    <ROLE>_API_KEY is read.
    """
    # Try indexed key first (e.g. REASONING_API_KEY_1)
    indexed_key = config(f"{role}_API_KEY_{index}", default=None)
    if indexed_key:
        return indexed_key
    # Fall back to plain key (e.g. TEXT_API_KEY)
    return config(f"{role}_API_KEY", default="")


def get_role_fallback_model(role: str) -> str | None:
    return config(f"{role}_FALLBACK_MODEL", default=None)


def get_role_fallback_provider(role: str) -> str | None:
    val = config(f"{role}_FALLBACK_PROVIDER", default=None)
    return val.lower() if val else None


def get_role_rpm(role: str) -> int:
    return config(f"{role}_RPM", default=30, cast=int)


def get_role_rpd(role: str) -> int:
    return config(f"{role}_RPD", default=14400, cast=int)


def get_role_concurrency(role: str) -> int:
    return config(f"{role}_CONCURRENCY", default=1, cast=int)
