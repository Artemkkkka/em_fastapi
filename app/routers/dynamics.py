from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_response
from app.crud import get_dynamics
from app.schemas import DynamicsResponse
from app.db import get_session


router = APIRouter()

@router.get("/dynamics", response_model=DynamicsResponse)
@cache_response("dynamics")
async def dynamics(
    start_date: date,
    end_date: date,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
):
    return await get_dynamics(
        session=session,
        start_date=start_date,
        end_date=end_date,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
    )