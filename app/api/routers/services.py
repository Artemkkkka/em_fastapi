from datetime import date
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.crud import (
    get_distinct_dates,
    get_trading_results_by_date_range,
    get_latest_trading_results
)
from api.entities.schemas import (
    LastTradingDatesResponse,
    DynamicsResponse,
    TradingResultsResponse,
    TradingResultDetail,
)


async def get_last_trading_dates_service(
    session: AsyncSession,
    count: int = 5,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> LastTradingDatesResponse:
    dates = await get_distinct_dates(
        session,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id
    )
    return LastTradingDatesResponse(dates=dates[:count])

async def get_dynamics_service(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> DynamicsResponse:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    
    records = await get_trading_results_by_date_range(
        session,
        start_date=start_date,
        end_date=end_date,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id
    )
    return DynamicsResponse(trades=[TradingResultDetail.from_orm(r) for r in records])

async def get_trading_results_service(
    session: AsyncSession,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    limit: int = 100,
) -> TradingResultsResponse:
    if not any([oil_id, delivery_type_id, delivery_basis_id]):
        raise HTTPException(
            status_code=400,
            detail="Укажите хотя бы один из фильтров: oil_id, delivery_type_id или delivery_basis_id"
        )
    
    records = await get_latest_trading_results(
        session,
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id
    )
    return TradingResultsResponse(results=[TradingResultDetail.from_orm(r) for r in records[:limit]])
