from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_response
from app.crud import get_trading_results
from app.schemas import TradingResultsResponse
from app.db import get_session


router = APIRouter()

@router.get("/trading_results", response_model=TradingResultsResponse)
@cache_response("trading_results")
async def trading_results(
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
):
    return await get_trading_results(
        session=session,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
        limit=limit,
    )