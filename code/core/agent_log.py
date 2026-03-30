"""
Agent transparency layer.

emit() is the single call pipeline code makes to log activity.
It writes to:
  1. An in-memory queue.Queue consumed by the SSE view at /activity/stream/
  2. The standard Django logger (visible in terminal)

Usage — one import, one line:
    from core.agent_log import emit
    emit(agent="classify_figures", status="running",
         message=f"Classified figure {fig.id} as {vis_type}",
         record_id=fig.id, progress=[current, total])
"""

import collections
import json
import logging
import queue
import threading
from datetime import datetime, timezone
from typing import Literal

logger = logging.getLogger(__name__)

# All SSE subscribers share a single broadcast queue.
# The SSE view drains this queue and fans out to connected clients.
_event_queue: queue.Queue = queue.Queue(maxsize=3000)

# Lock protecting the subscriber list and recent-event buffer
_subscriber_lock = threading.Lock()
_subscribers: list[queue.Queue] = []

# Rolling buffer of the last 60 events — replayed to clients that connect
# mid-run or reconnect after a brief drop (e.g. browser SSE auto-reconnect).
_recent_events: collections.deque = collections.deque(maxlen=60)

# Monotonically-increasing event counter — used as SSE event id so browsers
# can send Last-Event-ID on reconnect and we can replay from that point.
_event_counter: int = 0
_counter_lock = threading.Lock()

# Latest known status per agent — used by the /activity/status/ polling endpoint
# so the JS can recover even if it missed the done event entirely.
_agent_status: dict[str, dict] = {}


StatusLiteral = Literal["started", "running", "done", "skipped", "error"]
LevelLiteral = Literal["info", "warning", "error"]


def emit(
    agent: str,
    status: StatusLiteral,
    message: str,
    record_id: int | None = None,
    progress: list[int] | None = None,
    level: LevelLiteral = "info",
    # Optional debug fields
    model_used: str | None = None,
    api_response_ms: int | None = None,
    fallback_activated: bool = False,
) -> None:
    """
    Emit a pipeline event.

    Args:
        agent:              Name of the pipeline agent (matches command name).
        status:             One of started | running | done | skipped | error.
        message:            Human-readable description of what happened.
        record_id:          Primary key of the record being processed (optional).
        progress:           [current, total] — e.g. [42, 318] (optional).
        level:              info | warning | error.
        model_used:         Model string that made the call (DEBUG only).
        api_response_ms:    API round-trip time in ms (DEBUG only).
        fallback_activated: Whether the fallback model was used (DEBUG only).
    """
    event = {
        "agent": agent,
        "status": status,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
    }
    if record_id is not None:
        event["record_id"] = record_id
    if progress is not None:
        event["progress"] = progress

    # Include debug fields when set
    if model_used:
        event["model_used"] = model_used
    if api_response_ms is not None:
        event["api_response_ms"] = api_response_ms
    if fallback_activated:
        event["fallback_activated"] = True

    # Log to terminal
    log_fn = logger.error if level == "error" else (logger.warning if level == "warning" else logger.info)
    log_fn("[%s] %s — %s", agent, status.upper(), message)

    # Assign a monotonic event id (used in SSE id: field for resume-on-reconnect)
    global _event_counter
    with _counter_lock:
        _event_counter += 1
        event["event_id"] = _event_counter

    # Track latest status per agent for the polling fallback endpoint
    _agent_status[agent] = {"status": status, "message": message,
                             "progress": progress, "event_id": event["event_id"]}

    # Fan out to all SSE subscribers + add to recent-event buffer
    payload = json.dumps(event)
    with _subscriber_lock:
        _recent_events.append(payload)  # always buffered regardless of subscribers
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                try:
                    q.get_nowait()   # drop oldest
                    q.put_nowait(payload)
                except Exception:
                    dead.append(q)
        for q in dead:
            _subscribers.remove(q)

    # Also push to the global broadcast queue (for any other consumers)
    try:
        _event_queue.put_nowait(payload)
    except queue.Full:
        pass  # Drop oldest events silently if queue is full


def subscribe() -> queue.Queue:
    """
    Register a new SSE subscriber. Returns a Queue that will receive
    JSON-encoded event strings as they are emitted.
    Call unsubscribe(q) when the client disconnects.

    Recent events are replayed immediately into the new queue so that
    a client connecting mid-run (or reconnecting after a brief drop)
    sees the current agent states without waiting for the next emit().
    """
    q: queue.Queue = queue.Queue(maxsize=2000)
    with _subscriber_lock:
        # Replay recent events so reconnecting clients see current state
        for payload in _recent_events:
            try:
                q.put_nowait(payload)
            except queue.Full:
                break
        _subscribers.append(q)
    return q


def unsubscribe(q: queue.Queue) -> None:
    """Remove a subscriber queue."""
    with _subscriber_lock:
        try:
            _subscribers.remove(q)
        except ValueError:
            pass


def get_agent_status() -> dict:
    """
    Return a snapshot of the latest known status for every agent that has
    emitted at least one event. Used by the /activity/status/ polling endpoint
    so the browser can recover from missed SSE events.
    """
    with _subscriber_lock:
        return dict(_agent_status)


def get_last_event_id() -> int:
    """Return the current event counter value."""
    with _counter_lock:
        return _event_counter
