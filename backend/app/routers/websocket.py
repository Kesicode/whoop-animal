"""
routers/websocket.py
====================
Two WebSocket endpoints:

  WS /ws/health/{animal_id}  — streams live health readings for one animal
  WS /ws/alerts/{user_id}    — streams new alerts for a farmer's animals

Authentication strategy
-----------------------
Browser WebSocket API cannot send custom headers, so tokens are
passed as a query parameter:

    ws://host/ws/health/21?token=<access_jwt>

The router decodes and validates the JWT the same way as REST endpoints.
Farmers can only subscribe to their own animals/alerts.
Admins can subscribe to anything.
"""

import logging

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import decode_access_token
from app.models.animal import Animal
from app.models.user import User
from app.services.ws_manager import manager

logger = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


# ---------------------------------------------------------------------------
# Utility: authenticate WebSocket caller from query-param token
# ---------------------------------------------------------------------------

async def _ws_get_user(token: str, db: AsyncSession) -> User | None:
    """
    Returns the User if token is valid, else None.
    We return None instead of raising so the caller can send a close frame.
    """
    payload = decode_access_token(token)
    if payload is None:
        return None
    user_id: int | None = payload.get("sub")
    if user_id is None:
        return None
    result = await db.execute(select(User).where(User.id == int(user_id)))
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# WS /ws/health/{animal_id}
# ---------------------------------------------------------------------------

@router.websocket("/ws/health/{animal_id}")
async def ws_health(
    websocket: WebSocket,
    animal_id: int,
    token: str = Query(..., description="JWT access token"),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Stream live health readings to the animal detail page.

    Message format (JSON):
    {
        "type":             "health_reading",
        "animal_id":        21,
        "temperature":      39.8,
        "heart_rate":       95,
        "activity_level":   18.5,
        "rumination_score": 42.0,
        "timestamp":        "2026-03-06T00:22:00+00:00"
    }

    The server sends a `connected` handshake after accepting,
    then stays open.  The client only sends pings (optional).

    Connection is closed with 4001 if token is invalid.
    Connection is closed with 4003 if farmer tries to watch another farmer's animal.
    """
    # --- Auth ---
    user = await _ws_get_user(token, db)
    if user is None:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    # --- Ownership check ---
    result = await db.execute(select(Animal).where(Animal.id == animal_id))
    animal = result.scalar_one_or_none()

    if animal is None:
        await websocket.close(code=4004, reason="Animal not found")
        return

    # Admin can watch any animal; farmer can only watch their own
    if user.role != "admin" and animal.farmer_id != user.id:
        await websocket.close(code=4003, reason="Forbidden")
        return

    # --- Accept & register ---
    await manager.connect_health(websocket, animal_id)

    try:
        # Send initial handshake so client knows the connection is live
        await websocket.send_json({
            "type":      "connected",
            "animal_id": animal_id,
            "message":   f"Streaming live data for {animal.name}",
        })

        # Keep the connection open — we only RECEIVE pings from the client
        # All DATA flows server→client, pushed by alert_service.
        while True:
            # This await blocks until client sends something (ping) or disconnects.
            # We don't care about the content — just need to detect disconnects.
            data = await websocket.receive_text()
            logger.debug("WS health ping from user %d: %s", user.id, data)

    except WebSocketDisconnect:
        logger.info("WS health disconnect: animal_id=%d user=%d", animal_id, user.id)
    finally:
        manager.disconnect_health(websocket, animal_id)


# ---------------------------------------------------------------------------
# WS /ws/alerts/{user_id}
# ---------------------------------------------------------------------------

@router.websocket("/ws/alerts/{user_id}")
async def ws_alerts(
    websocket: WebSocket,
    user_id: int,
    token: str = Query(..., description="JWT access token"),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Stream new alert events to the farmer's browser (all tabs).

    Used by the AlertBanner component that slides in from the top of every page.

    Message format (JSON):
    {
        "type":        "new_alert",
        "alert_id":    99,
        "animal_id":   21,
        "animal_name": "Bessie",
        "alert_type":  "fever",
        "severity":    "critical",
        "message":     "Temperature 39.8°C exceeds critical threshold",
        "created_at":  "2026-03-06T00:22:00+00:00"
    }

    Connection is rejected with 4003 if the authenticated user ≠ user_id
    (farmers can only open their own alert stream; admins may open any).
    """
    # --- Auth ---
    user = await _ws_get_user(token, db)
    if user is None:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    # Farmer can only subscribe to their own stream
    if user.role != "admin" and user.id != user_id:
        await websocket.close(code=4003, reason="Forbidden")
        return

    # --- Accept & register ---
    await manager.connect_alerts(websocket, user_id)

    try:
        await websocket.send_json({
            "type":    "connected",
            "user_id": user_id,
            "message": "Alert stream connected",
        })

        while True:
            data = await websocket.receive_text()
            logger.debug("WS alerts ping from user %d: %s", user_id, data)

    except WebSocketDisconnect:
        logger.info("WS alerts disconnect: user_id=%d", user_id)
    finally:
        manager.disconnect_alerts(websocket, user_id)
