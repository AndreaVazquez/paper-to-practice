"""
Token-bucket rate limiter for LLM roles.

Each role has its own bucket, seeded from .env:
  <ROLE>_RPM   — max requests per minute  (refill rate)
  <ROLE>_RPD   — max requests per day     (daily hard cap)

Every call to call_llm() passes through RateLimiter.acquire(role) before
touching any external API. If RPM is exhausted, the call sleeps until a
token is available. If RPD is exhausted, raises RateLimitExhausted.

Thread-safe: all state is protected by per-role locks.
"""

import logging
import threading
import time
from datetime import date

from core.config import get_role_rpm, get_role_rpd

logger = logging.getLogger(__name__)


class RateLimitExhausted(Exception):
    """Raised when the daily request quota for a role is exhausted."""


class _RoleBucket:
    """Token-bucket state for a single role."""

    def __init__(self, rpm: int, rpd: int) -> None:
        self.rpm = max(rpm, 1)
        self.rpd = max(rpd, 1)
        self._lock = threading.Lock()

        # Token bucket
        self._tokens: float = float(self.rpm)
        self._last_refill: float = time.monotonic()

        # Daily counter
        self._day_count: int = 0
        self._day_date: date = date.today()

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time (up to rpm capacity)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * (self.rpm / 60.0)
        self._tokens = min(self._tokens + new_tokens, float(self.rpm))
        self._last_refill = now

    def _reset_day_if_needed(self) -> None:
        today = date.today()
        if today != self._day_date:
            self._day_count = 0
            self._day_date = today

    def acquire(self) -> None:
        """
        Block until a token is available, then consume it.
        Raises RateLimitExhausted if the daily quota is gone.
        """
        while True:
            with self._lock:
                self._reset_day_if_needed()
                if self._day_count >= self.rpd:
                    raise RateLimitExhausted(
                        f"Daily quota of {self.rpd} requests exhausted."
                    )
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._day_count += 1
                    return
                # Calculate sleep needed for next token
                deficit = 1.0 - self._tokens
                sleep_secs = deficit / (self.rpm / 60.0)

            logger.debug("Rate limiter: sleeping %.2fs for next token", sleep_secs)
            time.sleep(sleep_secs)


class RateLimiter:
    """
    Singleton registry of per-role token buckets.
    Lazily initialises a bucket the first time a role is seen.
    """

    _instance: "RateLimiter | None" = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "RateLimiter":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._buckets: dict[str, _RoleBucket] = {}
                    inst._bucket_lock = threading.Lock()
                    cls._instance = inst
        return cls._instance

    def _get_bucket(self, role: str) -> _RoleBucket:
        with self._bucket_lock:
            if role not in self._buckets:
                rpm = get_role_rpm(role)
                rpd = get_role_rpd(role)
                self._buckets[role] = _RoleBucket(rpm, rpd)
                logger.debug(
                    "RateLimiter: created bucket for %s (rpm=%d, rpd=%d)",
                    role, rpm, rpd,
                )
            return self._buckets[role]

    def acquire(self, role: str) -> None:
        """Acquire a token for the given role. Blocks if RPM is exceeded."""
        self._get_bucket(role).acquire()


# Module-level singleton — import and call rate_limiter.acquire(role)
rate_limiter = RateLimiter()
