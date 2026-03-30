"""
LLM Client — Layer 2 of the three-layer LLM separation (§5.4).

Single public function: call_llm(role, prompt, ...)

Responsibilities:
  - Read role configuration from .env via core.config
  - Acquire a rate-limit token via core.rate_limiter
  - Route to the correct provider SDK (groq / gemini)
  - Handle retries and fallback models
  - Return plain text (or parsed JSON dict if response_format="json")

This module has NO knowledge of what any prompt means or what pipeline
step is calling it. It knows only: provider → SDK → text out.

Supported providers:
  groq   — OpenAI-compatible endpoint via openai SDK
  gemini — Google GenAI SDK (google-genai, the new unified SDK)
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Literal

from core.config import (
    get_role_api_key,
    get_role_fallback_model,
    get_role_fallback_provider,
    get_role_model,
    get_role_provider,
)
from core.rate_limiter import RateLimitExhausted, rate_limiter

logger = logging.getLogger(__name__)

# ── Provider constants ──────────────────────────────────────────────────────────
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
_MAX_RETRIES = 3
_RETRY_SLEEP_SECS = 5


# ── Internal helpers ────────────────────────────────────────────────────────────

def _build_groq_client(api_key: str):
    """Return an openai.OpenAI client pointed at Groq's endpoint."""
    from openai import OpenAI
    return OpenAI(api_key=api_key, base_url=GROQ_BASE_URL)


def _call_groq(
    model: str,
    api_key: str,
    prompt: str,
    image_path: str | None,
    response_format: Literal["text", "json"],
    system_prompt: str | None,
) -> str:
    """Make a single call to Groq via the OpenAI-compatible SDK."""
    client = _build_groq_client(api_key)

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    if image_path:
        import base64
        img_data = Path(image_path).read_bytes()
        b64 = base64.b64encode(img_data).decode("utf-8")
        # Determine MIME type from extension
        ext = Path(image_path).suffix.lower()
        mime = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".png": "image/png", ".webp": "image/webp"}.get(ext, "image/png")
        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ],
        })
    else:
        messages.append({"role": "user", "content": prompt})

    kwargs: dict[str, Any] = {"model": model, "messages": messages}
    if response_format == "json":
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)
    return response.choices[0].message.content or ""


def _call_gemini(
    model: str,
    api_key: str,
    prompt: str,
    image_path: str | None,
    response_format: Literal["text", "json"],
    system_prompt: str | None,
    gemini_file_id: str | None = None,
) -> str:
    """Make a single call to Gemini via the google-genai SDK."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)

    config_kwargs: dict[str, Any] = {}
    if system_prompt:
        config_kwargs["system_instruction"] = system_prompt
    if response_format == "json":
        config_kwargs["response_mime_type"] = "application/json"

    gen_config = types.GenerateContentConfig(**config_kwargs) if config_kwargs else None

    # Build contents
    if image_path and not gemini_file_id:
        # Upload once and use inline bytes for small images
        img_bytes = Path(image_path).read_bytes()
        ext = Path(image_path).suffix.lower().lstrip(".")
        mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                "png": "image/png", "webp": "image/webp"}.get(ext, "image/png")
        contents = [
            types.Part.from_bytes(data=img_bytes, mime_type=mime),
            prompt,
        ]
    elif gemini_file_id:
        # Reference a previously uploaded Gemini File API file
        contents = [
            types.Part.from_uri(file_uri=gemini_file_id, mime_type="image/png"),
            prompt,
        ]
    else:
        contents = [prompt]

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=gen_config,
    )
    return response.text or ""


def _upload_to_gemini_file_api(image_path: str, api_key: str) -> str:
    """
    Upload an image to the Gemini File API and return the file URI.
    Store the returned URI in paper_figures.gemini_file_id to avoid re-uploading.
    """
    from google import genai

    client = genai.Client(api_key=api_key)
    uploaded = client.files.upload(path=image_path)
    return uploaded.uri


def _extract_json(text: str) -> dict | list:
    """Parse JSON from model output, stripping markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        # Strip ```json ... ``` fences
        lines = text.splitlines()
        # Remove first and last fence lines
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()
    return json.loads(text)


# ── Public API ─────────────────────────────────────────────────────────────────

def call_llm(
    role: str,
    prompt: str,
    image_path: str | None = None,
    response_format: Literal["text", "json"] = "text",
    system_prompt: str | None = None,
    gemini_file_id: str | None = None,
    _use_fallback: bool = False,
) -> str | dict | list:
    """
    Make an LLM call for the given functional role.

    Args:
        role:            One of TEXT, IMAGE, IMAGE_FALLBACK, REASONING,
                         REASONING_FALLBACK, QUERY.
        prompt:          The user-turn prompt string.
        image_path:      Local path to an image file (for multimodal calls).
        response_format: "text" (default) or "json" (returns parsed dict/list).
        system_prompt:   Optional system message.
        gemini_file_id:  Gemini File API URI to reference instead of re-uploading.
        _use_fallback:   Internal flag — do not set manually.

    Returns:
        str if response_format="text", dict/list if response_format="json".

    Raises:
        RateLimitExhausted: If the daily quota is exhausted.
        Exception:          If all retries fail and no fallback is available.
    """
    # Acquire rate-limit token
    try:
        rate_limiter.acquire(role)
    except RateLimitExhausted:
        logger.error("Rate limit exhausted for role %s", role)
        raise

    provider = get_role_provider(role)
    model = get_role_model(role)
    api_key = get_role_api_key(role)

    logger.debug("call_llm: role=%s provider=%s model=%s", role, provider, model)

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            t0 = time.monotonic()

            if provider == "groq":
                raw = _call_groq(model, api_key, prompt, image_path,
                                 response_format, system_prompt)
            elif provider == "gemini":
                # Gemini has two API keys; rotate on 429
                raw = _call_gemini(model, api_key, prompt, image_path,
                                   response_format, system_prompt, gemini_file_id)
            else:
                raise ValueError(f"Unknown provider '{provider}' for role {role}")

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            logger.debug("call_llm: %s/%s responded in %dms", provider, model, elapsed_ms)

            if response_format == "json":
                return _extract_json(raw)
            return raw

        except Exception as exc:
            last_exc = exc
            exc_str = str(exc)

            # Gemini 429 — rotate API key and retry immediately
            if provider == "gemini" and "429" in exc_str:
                key2 = get_role_api_key(role, index=2)
                if key2 and key2 != api_key:
                    logger.warning(
                        "Gemini 429 for role %s, rotating to key_2 (attempt %d)",
                        role, attempt,
                    )
                    api_key = key2
                    continue

            # IMAGE role 429 — switch to IMAGE_FALLBACK transparently
            if role == "IMAGE" and "429" in exc_str:
                fallback_model = get_role_fallback_model(role)
                if fallback_model:
                    logger.warning("IMAGE 429 — switching to fallback model %s", fallback_model)
                    # Temporarily override model for remaining attempts
                    model = fallback_model
                    continue

            logger.warning(
                "call_llm attempt %d/%d failed for role %s: %s",
                attempt, _MAX_RETRIES, role, exc_str,
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_SLEEP_SECS * attempt)

    # All retries failed — try the role-level fallback if configured
    fallback_provider = get_role_fallback_provider(role)
    fallback_model = get_role_fallback_model(role)
    if fallback_provider and fallback_model and not _use_fallback:
        logger.warning(
            "All retries failed for role %s. Trying fallback %s/%s",
            role, fallback_provider, fallback_model,
        )
        # Build a synthetic fallback role name and call recursively
        fallback_role = f"{role}_FALLBACK"
        return call_llm(
            role=fallback_role,
            prompt=prompt,
            image_path=image_path,
            response_format=response_format,
            system_prompt=system_prompt,
            gemini_file_id=gemini_file_id,
            _use_fallback=True,
        )

    raise RuntimeError(
        f"call_llm: all {_MAX_RETRIES} attempts failed for role {role}"
    ) from last_exc


def upload_image_for_gemini(image_path: str) -> str:
    """
    Upload an image to the Gemini File API. Returns the file URI.
    Use the returned URI as gemini_file_id in subsequent call_llm() calls
    to avoid re-uploading the same image.
    """
    api_key = get_role_api_key("REASONING")
    return _upload_to_gemini_file_api(image_path, api_key)
