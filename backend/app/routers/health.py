"""
routers/health.py
=================
Handles:
  POST /health-data        ← IoT device endpoint (X-Device-Serial auth)
  GET  /animals/{id}/health        ← paginated history
  GET  /animals/{id}/health/latest ← single most recent
  GET  /animals/{id}/health/stats  ← min/max/avg for a period
"""

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.dependencies import get_current_farmer_or_admin
from app.models.animal import Animal
from app.models.device import Device
from app.models.health_reading import HealthReading
from app.schemas.health import HealthReadingCreate, HealthReadingResponse, HealthStats
from app.services.alert_service import check_and_broadcast

router = APIRouter(tags=["health"])


# ---------------------------------------------------------------------------
# IoT endpoint — POST /health-data
# ---------------------------------------------------------------------------

@router.post(
    "/health-data",
    status_code=status.HTTP_201_CREATED,
    summary="IoT device posts a health reading",
    description=(
        "Authenticated by X-Device-Serial header.\n"
        "After saving the reading:\n"
        "  1. Alert thresholds are evaluated\n"
        "  2. New alerts are created in the DB\n"
        "  3. The health reading is pushed via WS to /ws/health/{animal_id}\n"
        "  4. Any new alerts are pushed via WS to /ws/alerts/{farmer_id}\n"
    ),
)
async def ingest_health_data(
    payload: HealthReadingCreate,
    background_tasks: BackgroundTasks,
    x_device_serial: Annotated[str, Header(alias="X-Device-Serial")],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Step-by-step of what happens inside this endpoint:

    1. Look up the device by X-Device-Serial header
    2. Verify the device is assigned to an animal
    3. Save HealthReading row
    4. Update device.last_seen + device.status = 'online'
    5. In a BackgroundTask: run alert_service.check_and_broadcast()
       ↳ This does threshold checking + WS push WITHOUT blocking the 200 response

    The ESP32 gets a fast 201 response.
    The WebSocket push happens ~milliseconds later in the background.
    """

    # 1. Authenticate device ---------------------------------------------------
    result = await db.execute(
        select(Device).where(Device.device_serial == x_device_serial)
    )
    device: Device | None = result.scalar_one_or_none()

    if device is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unknown device serial",
        )

    # 2. Ensure device is assigned to an animal --------------------------------
    if device.animal_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Device is not assigned to any animal",
        )

    # 3. Fetch the animal (needed for threshold checks + WS push) -------------
    animal_result = await db.execute(
        select(Animal).where(Animal.id == device.animal_id)
    )
    animal: Animal | None = animal_result.scalar_one_or_none()

    if animal is None:
        raise HTTPException(status_code=500, detail="Animal record missing")

    # 4. Save the reading -----------------------------------------------------
    reading = HealthReading(
        animal_id=animal.id,
        temperature=payload.temperature,
        heart_rate=payload.heart_rate,
        activity_level=payload.activity_level,
        rumination_score=payload.rumination_score,
        raw_payload=payload.raw_payload,
        timestamp=datetime.now(timezone.utc),
    )
    db.add(reading)

    # 5. Update device heartbeat ----------------------------------------------
    device.last_seen = datetime.now(timezone.utc)
    device.status = "online"
    if payload.battery_pct is not None:
        device.battery_pct = payload.battery_pct

    await db.commit()
    await db.refresh(reading)

    # 6. Background: threshold check + WebSocket push -------------------------
    #
    #   WHY BackgroundTasks and not just await?
    #   ----------------------------------------
    #   check_and_broadcast() iterates over WebSocket connections and
    #   does additional DB queries.  We don't want the ESP32 to wait for
    #   all of that.  BackgroundTasks runs AFTER the HTTP response is sent
    #   but BEFORE the ASGI server closes the connection — so it's still
    #   in the same request lifecycle (same event loop, no threading issues).
    #
    background_tasks.add_task(check_and_broadcast, db, reading, animal)

    return {
        "status":     "ok",
        "reading_id": reading.id,
        "animal_id":  animal.id,
        "timestamp":  reading.timestamp.isoformat(),
    }


# ---------------------------------------------------------------------------
# Farmer: GET /animals/{id}/health
# ---------------------------------------------------------------------------

@router.get(
    "/animals/{animal_id}/health",
    response_model=list[HealthReadingResponse],
)
async def get_health_history(
    animal_id: int,
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(100, le=1000),
    offset: int = 0,
    current_user=Depends(get_current_farmer_or_admin),
    db: AsyncSession = Depends(get_db),
) -> list[HealthReading]:
    await _assert_animal_access(db, animal_id, current_user)

    q = select(HealthReading).where(HealthReading.animal_id == animal_id)
    if from_ts:
        q = q.where(HealthReading.timestamp >= from_ts)
    if to_ts:
        q = q.where(HealthReading.timestamp <= to_ts)
    q = q.order_by(HealthReading.timestamp.desc()).limit(limit).offset(offset)

    result = await db.execute(q)
    return result.scalars().all()


# ---------------------------------------------------------------------------
# Farmer: GET /animals/{id}/health/latest
# ---------------------------------------------------------------------------

@router.get(
    "/animals/{animal_id}/health/latest",
    response_model=HealthReadingResponse,
)
async def get_latest_health(
    animal_id: int,
    current_user=Depends(get_current_farmer_or_admin),
    db: AsyncSession = Depends(get_db),
) -> HealthReading:
    await _assert_animal_access(db, animal_id, current_user)

    result = await db.execute(
        select(HealthReading)
        .where(HealthReading.animal_id == animal_id)
        .order_by(HealthReading.timestamp.desc())
        .limit(1)
    )
    reading = result.scalar_one_or_none()
    if reading is None:
        raise HTTPException(status_code=404, detail="No readings found")
    return reading


# ---------------------------------------------------------------------------
# Farmer: GET /animals/{id}/health/stats
# ---------------------------------------------------------------------------

@router.get(
    "/animals/{animal_id}/health/stats",
    response_model=HealthStats,
)
async def get_health_stats(
    animal_id: int,
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    current_user=Depends(get_current_farmer_or_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    await _assert_animal_access(db, animal_id, current_user)

    q = select(
        func.min(HealthReading.temperature).label("temp_min"),
        func.max(HealthReading.temperature).label("temp_max"),
        func.avg(HealthReading.temperature).label("temp_avg"),
        func.min(HealthReading.heart_rate).label("hr_min"),
        func.max(HealthReading.heart_rate).label("hr_max"),
        func.avg(HealthReading.heart_rate).label("hr_avg"),
        func.min(HealthReading.activity_level).label("activity_min"),
        func.max(HealthReading.activity_level).label("activity_max"),
        func.avg(HealthReading.activity_level).label("activity_avg"),
        func.min(HealthReading.rumination_score).label("rum_min"),
        func.max(HealthReading.rumination_score).label("rum_max"),
        func.avg(HealthReading.rumination_score).label("rum_avg"),
        func.count(HealthReading.id).label("total_readings"),
    ).where(HealthReading.animal_id == animal_id)

    if from_ts:
        q = q.where(HealthReading.timestamp >= from_ts)
    if to_ts:
        q = q.where(HealthReading.timestamp <= to_ts)

    result = await db.execute(q)
    row = result.one()
    return row._asdict()


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------

async def _assert_animal_access(db: AsyncSession, animal_id: int, user) -> Animal:
    """Raises 404 if animal doesn't exist, 403 if farmer doesn't own it."""
    result = await db.execute(select(Animal).where(Animal.id == animal_id))
    animal = result.scalar_one_or_none()
    if animal is None:
        raise HTTPException(status_code=404, detail="Animal not found")
    if user.role != "admin" and animal.farmer_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return animal
