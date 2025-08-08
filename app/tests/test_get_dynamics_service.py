
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from fastapi import HTTPException
import pytest

from api.entities.schemas import DynamicsResponse
from api.models.crud import get_trading_results_by_date_range
from api.routers.services import get_dynamics_service


class DummyTradingResult:
    id = 1
    exchange_product_id = "ex_prod_1"
    exchange_product_name = "Product 1"
    oil_id = "oil_123"
    delivery_basis_id = "basis_1"
    delivery_basis_name = "Basis Name"
    delivery_type_id = "type_1"
    volume = Decimal("100.5")
    total = Decimal("2000.75")
    count = 5
    date = date(2023, 8, 8)
    created_on = datetime(2023, 8, 1, 12, 0, 0)
    updated_on = datetime(2023, 8, 5, 12, 0, 0)


@pytest.mark.asyncio
async def test_get_trading_results_by_date_range(mocker):
    mock_rows = MagicMock()
    mock_rows.scalars.return_value.all.return_value = ["record1", "record2"]

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_rows

    results = await get_trading_results_by_date_range(
        mock_session,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 31),
        oil_id="oil1"
    )

    assert results == ["record1", "record2"]


@pytest.mark.asyncio
async def test_get_dynamics_service_valid_and_invalid_dates(mocker):
    session = AsyncMock()

    mock_records = [DummyTradingResult(), DummyTradingResult()]
    mocker.patch(
        "api.routers.services.get_trading_results_by_date_range",
        return_value=mock_records
    )
    with pytest.raises(HTTPException) as exc_info:
        await get_dynamics_service(session, date(2023, 5, 2), date(2023, 5, 1))
    assert exc_info.value.status_code == 400

    result = await get_dynamics_service(session, date(2023, 5, 1), date(2023, 5, 2), oil_id="123")
    assert isinstance(result, DynamicsResponse)

    trade = result.trades[0]
    assert trade.exchange_product_id == "ex_prod_1"
    assert isinstance(trade.volume, Decimal)
    assert trade.date == date(2023, 8, 8)
