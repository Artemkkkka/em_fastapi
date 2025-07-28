from datetime import date
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select, func, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import TradingResult
from app.schemas import (
    LastTradingDatesResponse,
    DynamicsResponse,
    TradingResultsResponse,
    TradingResultDetail,
)

async def get_last_trading_dates(
    session: AsyncSession,
    count: int = 5,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> LastTradingDatesResponse:
    stmt = select(TradingResult.date).distinct()
    if oil_id:
        stmt = stmt.where(TradingResult.oil_id == oil_id)
    if delivery_type_id:
        stmt = stmt.where(TradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        stmt = stmt.where(TradingResult.delivery_basis_id == delivery_basis_id)
    stmt = stmt.order_by(desc(TradingResult.date)).limit(count)

    rows = await session.execute(stmt)
    dates = [r[0] for r in rows.fetchall()]
    return LastTradingDatesResponse(dates=dates)

async def get_dynamics(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> DynamicsResponse:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    stmt = select(TradingResult).where(
        TradingResult.date.between(start_date, end_date)
    )
    if oil_id:
        stmt = stmt.where(TradingResult.oil_id == oil_id)
    if delivery_type_id:
        stmt = stmt.where(TradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        stmt = stmt.where(TradingResult.delivery_basis_id == delivery_basis_id)
    stmt = stmt.order_by(asc(TradingResult.date))

    rows = await session.execute(stmt)
    records = rows.scalars().all()
    return DynamicsResponse(trades=[TradingResultDetail.from_orm(r) for r in records])

async def get_trading_results(
    session: AsyncSession,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    limit: int = 100,
) -> TradingResultsResponse:
    filters = []
    if oil_id:
        filters.append(TradingResult.oil_id == oil_id)
    if delivery_type_id:
        filters.append(TradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        filters.append(TradingResult.delivery_basis_id == delivery_basis_id)
    if not filters:
        raise HTTPException(
            status_code=400,
            detail="Укажите хотя бы один из фильтров: oil_id, delivery_type_id или delivery_basis_id"
        )

    max_date = (await session.execute(
        select(func.max(TradingResult.date)).where(*filters)
    )).scalar_one()
    stmt = (
        select(TradingResult)
        .where(TradingResult.date == max_date, *filters)
        .order_by(desc(TradingResult.id))
        .limit(limit)
    )
    rows = await session.execute(stmt)
    records = rows.scalars().all()
    return TradingResultsResponse(results=[TradingResultDetail.from_orm(r) for r in records])
