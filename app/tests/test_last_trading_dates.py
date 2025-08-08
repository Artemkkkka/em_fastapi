from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

from api.models.crud import get_distinct_dates
from api.routers.services import get_last_trading_dates_service
from api.entities.schemas import LastTradingDatesResponse


@pytest.mark.asyncio
async def test_get_distinct_dates_no_filters():
    rows = Mock()
    rows.fetchall.return_value = [
        (date(2025, 8, 5),),
        (date(2025, 8, 1),),
    ]
    session = AsyncMock()
    session.execute.return_value = rows
    result = await get_distinct_dates(session)
    assert result == [date(2025, 8, 5), date(2025, 8, 1)]
    session.execute.assert_awaited()


@pytest.mark.asyncio
async def test_get_distinct_dates_with_filters():
    rows = Mock()
    rows.fetchall.return_value = [
        (date(2025, 7, 15),),
    ]
    session = AsyncMock()
    session.execute.return_value = rows
    result = await get_distinct_dates(
        session,
        oil_id="OIL123",
        delivery_type_id="TYPE1",
        delivery_basis_id="BASIS42"
    )
    assert result == [date(2025, 7, 15)]
    session.execute.assert_awaited()


@pytest.mark.asyncio
async def test_get_last_trading_dates_service(mocker):
    session = None
    mock_dates = [
        date(2025, 8, 1),
        date(2025, 7, 31),
        date(2025, 7, 30),
        date(2025, 7, 29),
        date(2025, 7, 28),
        date(2025, 7, 27),
    ]
    mock_get_distinct_dates = mocker.patch(
        "api.routers.services.get_distinct_dates", new_callable=AsyncMock
    )
    mock_get_distinct_dates.return_value = mock_dates
    response = await get_last_trading_dates_service(
        session,
        count=3,
        oil_id="oil1",
        delivery_type_id="dt1",
        delivery_basis_id="db1"
    )
    mock_get_distinct_dates.assert_awaited_once_with(
        session,
        oil_id="oil1",
        delivery_type_id="dt1",
        delivery_basis_id="db1"
    )
    assert isinstance(response, LastTradingDatesResponse)
    assert response.dates == mock_dates[:3]
