"""
ws_manager.py
=============
Central WebSocket connection registry.

Two kinds of rooms:
  - health_{animal_id}   → subscribed by the animal detail page
  - alerts_{user_id}     → subscribed by every page the farmer is on
                           (drives the AlertBanner that slides in)

Thread/async safety: asyncio is single-threaded by default in FastAPI,
so a plain dict is safe here.  If you later move to multi-worker (Gunicorn
+ multiple Uvicorn workers) you MUST replace this with a Redis Pub/Sub
broadcast layer (see NOTE at the bottom of this file).
"""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Maintains two dictionaries of active WebSocket connections:

        _health_rooms : { animal_id (int) → set[WebSocket] }
        _alert_rooms  : { user_id   (int) → set[WebSocket] }

    Any number of browser tabs can be subscribed to the same room.
    """

    def __init__(self) -> None:
        # animal_id → set of websockets watching that animal's live data
        self._health_rooms: dict[int, set[WebSocket]] = defaultdict(set)

        # user_id → set of websockets receiving that farmer's alert stream
        self._alert_rooms: dict[int, set[WebSocket]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect_health(self, websocket: WebSocket, animal_id: int) -> None:
        """Accept the connection and register it in the animal's health room."""
        await websocket.accept()
        self._health_rooms[animal_id].add(websocket)
        logger.info(
            "WS connected  | health room animal_id=%d | total=%d",
            animal_id,
            len(self._health_rooms[animal_id]),
        )

    async def connect_alerts(self, websocket: WebSocket, user_id: int) -> None:
        """Accept the connection and register it in the user's alert room."""
        await websocket.accept()
        self._alert_rooms[user_id].add(websocket)
        logger.info(
            "WS connected  | alerts room user_id=%d | total=%d",
            user_id,
            len(self._alert_rooms[user_id]),
        )

    def disconnect_health(self, websocket: WebSocket, animal_id: int) -> None:
        self._health_rooms[animal_id].discard(websocket)
        logger.info(
            "WS disconnect | health room animal_id=%d | remaining=%d",
            animal_id,
            len(self._health_rooms[animal_id]),
        )

    def disconnect_alerts(self, websocket: WebSocket, user_id: int) -> None:
        self._alert_rooms[user_id].discard(websocket)
        logger.info(
            "WS disconnect | alerts room user_id=%d | remaining=%d",
            user_id,
            len(self._alert_rooms[user_id]),
        )

    # ------------------------------------------------------------------
    # Broadcasting helpers
    # ------------------------------------------------------------------

    async def push_to_animal(self, animal_id: int, payload: dict[str, Any]) -> None:
        """
        Broadcast a health reading to everyone watching this animal.

        Called by alert_service after a POST /health-data is processed.
        payload example:
            {
                "type": "health_reading",
                "animal_id": 21,
                "temperature": 39.8,
                "heart_rate": 95,
                "activity_level": 18.5,
                "rumination_score": 42.0,
                "timestamp": "2026-03-06T00:22:00Z"
            }
        """
        await self._broadcast(self._health_rooms[animal_id], payload)

    async def push_to_user(self, user_id: int, payload: dict[str, Any]) -> None:
        """
        Broadcast a new alert to every browser tab the farmer has open.

        Called by alert_service when a threshold is breached.
        payload example:
            {
                "type": "new_alert",
                "alert_id": 99,
                "animal_id": 21,
                "animal_name": "Bessie",
                "alert_type": "fever",
                "severity": "critical",
                "message": "Temperature 39.8°C exceeds critical threshold",
                "created_at": "2026-03-06T00:22:00Z"
            }
        """
        await self._broadcast(self._alert_rooms[user_id], payload)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _broadcast(
        self, connections: set[WebSocket], payload: dict[str, Any]
    ) -> None:
        """
        Send JSON to every socket in the set.
        Dead sockets are collected and removed after the loop so we
        don't mutate the set while iterating it.
        """
        if not connections:
            return

        message = json.dumps(payload, default=str)  # default=str handles datetime
        dead: list[WebSocket] = []

        # Send to all connections concurrently
        results = await asyncio.gather(
            *[ws.send_text(message) for ws in connections],
            return_exceptions=True,
        )

        for ws, result in zip(list(connections), results):
            if isinstance(result, Exception):
                logger.warning("WS send failed (%s) — removing dead connection", result)
                dead.append(ws)

        for ws in dead:
            connections.discard(ws)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "health_rooms": {
                str(k): len(v) for k, v in self._health_rooms.items() if v
            },
            "alert_rooms": {
                str(k): len(v) for k, v in self._alert_rooms.items() if v
            },
        }


# ---------------------------------------------------------------------------
# Singleton — import this everywhere:
#   from app.services.ws_manager import manager
# ---------------------------------------------------------------------------
manager = ConnectionManager()


# ---------------------------------------------------------------------------
# NOTE — Multi-worker scaling
# ---------------------------------------------------------------------------
# This in-memory approach works perfectly for a single Uvicorn process.
# If you run:  gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app
# ...then each worker has its OWN manager dict, so a browser connected to
# worker-1 won't get messages broadcast from worker-2.
#
# Fix: replace _broadcast() with Redis Pub/Sub:
#
#   import redis.asyncio as aioredis
#   redis = aioredis.from_url("redis://localhost")
#
#   # Publisher (in alert_service):
#   await redis.publish(f"health:{animal_id}", json.dumps(payload))
#
#   # Subscriber (in websocket.py, per connection):
#   async with redis.pubsub() as ps:
#       await ps.subscribe(f"health:{animal_id}")
#       async for msg in ps.listen():
#           await websocket.send_text(msg["data"])
# ---------------------------------------------------------------------------
