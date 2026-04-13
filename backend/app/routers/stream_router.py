"""
SSE streaming endpoint for real-time Kafka events.
"""

import asyncio
import json
import subprocess
import sys

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from app.kafka.consumer import consume_events

router = APIRouter()

# Track demo process so we don't spawn duplicates
_demo_process = None


@router.get("/api/v1/stream/events")
async def stream_events():
    async def event_generator():
        async for event in consume_events():
            if event is None:
                yield {"event": "heartbeat", "data": "{}"}
            else:
                yield {"event": "disease-event", "data": json.dumps(event)}
            await asyncio.sleep(0.01)

    return EventSourceResponse(event_generator())


@router.post("/api/v1/stream/demo")
async def start_demo(limit: int = 500):
    """Start the Kafka producer in the background to demo live streaming."""
    global _demo_process

    # If a demo is already running, report status
    if _demo_process is not None and _demo_process.poll() is None:
        return {"status": "already_running", "message": "Demo producer is already running", "pid": _demo_process.pid}

    # Run the kafka producer as a background subprocess
    _demo_process = subprocess.Popen(
        [sys.executable, "-u", "/app/etl/kafka_producer.py", "--limit", str(limit), "--delay", "50"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    return {"status": "started", "message": f"Demo producer started with {limit} events", "pid": _demo_process.pid}


@router.get("/api/v1/stream/demo/status")
async def demo_status():
    """Check if the demo producer is still running."""
    global _demo_process
    if _demo_process is None:
        return {"status": "idle", "running": False}

    poll = _demo_process.poll()
    if poll is None:
        return {"status": "running", "running": True, "pid": _demo_process.pid}
    else:
        return {"status": "finished", "running": False, "exit_code": poll}
