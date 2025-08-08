from datetime import date
from typing import List, Optional

from sqlalchemy import select, func, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession

from .models import TradingResult


async def get_distinct_dates(
    session: AsyncSession,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> List[date]:
    stmt = select(TradingResult.date).distinct()
    if oil_id:
        stmt = stmt.where(TradingResult.oil_id == oil_id)
    if delivery_type_id:
        stmt = stmt.where(TradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        stmt = stmt.where(TradingResult.delivery_basis_id == delivery_basis_id)
    stmt = stmt.order_by(desc(TradingResult.date))

    rows = await session.execute(stmt)
    return [r[0] for r in rows.fetchall()]


async def get_trading_results_by_date_range(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> List[TradingResult]:
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
    return rows.scalars().all()


async def get_latest_trading_results(
    session: AsyncSession,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
) -> List[TradingResult]:
    filters = []
    if oil_id:
        filters.append(TradingResult.oil_id == oil_id)
    if delivery_type_id:
        filters.append(TradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        filters.append(TradingResult.delivery_basis_id == delivery_basis_id)

    max_date = (await session.execute(
        select(func.max(TradingResult.date)).where(*filters)
    )).scalar_one()

    stmt = (
        select(TradingResult)
        .where(TradingResult.date == max_date, *filters)
        .order_by(desc(TradingResult.id))
    )
    rows = await session.execute(stmt)
    return rows.scalars().all()
