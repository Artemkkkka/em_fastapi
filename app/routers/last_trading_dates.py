from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_response
from app.crud import get_last_trading_dates
from app.schemas import LastTradingDatesResponse
from app.db import get_session


router = APIRouter()

@router.get("/last_trading_dates", response_model=LastTradingDatesResponse)
@cache_response("last_trading_dates")
async def last_trading_dates(
    count: int = 5,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
):
    return await get_last_trading_dates(
        session=session,
        count=count,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
    )