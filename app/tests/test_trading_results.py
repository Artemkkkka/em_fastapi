from unittest.mock import patch, AsyncMock, MagicMock
from datetime import date, datetime

from fastapi import HTTPException
import pytest

from api.entities.schemas import TradingResultsResponse
from api.models.crud import get_latest_trading_results
from api.models.models import TradingResult
from api.routers.services import get_trading_results_service


@pytest.mark.asyncio
async def test_get_latest_trading_results():
    max_date = date(2023, 8, 1)
    mock_result_max_date = AsyncMock()
    mock_result_max_date.scalar_one = MagicMock(return_value=max_date)
    tr = TradingResult(
        id=123,
        oil_id="oil1",
        delivery_type_id="dt1",
        delivery_basis_id="db1",
        date=max_date,
        volume=10,
        total=100,
        count=1,
    )
    mock_scalars = MagicMock()
    mock_scalars.all = MagicMock(return_value=[tr])
    mock_result_rows = AsyncMock()
    mock_result_rows.scalars = MagicMock(return_value=mock_scalars)
    session = AsyncMock()
    session.execute = AsyncMock(
        side_effect=[mock_result_max_date, mock_result_rows]
    )
    results = await get_latest_trading_results(
        session=session,
        oil_id="oil1",
        delivery_type_id="dt1",
        delivery_basis_id="db1",
    )

    assert len(results) == 1
    assert results[0].id == 123
    assert results[0].date == max_date


@pytest.mark.asyncio
async def test_get_trading_results_service_no_filters_raises():
    with pytest.raises(HTTPException) as excinfo:
        await get_trading_results_service(session=AsyncMock())
    assert excinfo.value.status_code == 400
    assert "Укажите хотя бы один из фильтров" in excinfo.value.detail


@pytest.mark.asyncio
async def test_get_trading_results_service_calls_get_latest_trading_results():
    fake_tr = TradingResult(
        id=1,
        oil_id="oil1",
        delivery_type_id="dt1",
        delivery_basis_id="db1",
        date=date.today(),
        volume=10,
        total=100,
        count=5,
        exchange_product_id="prod123",
        exchange_product_name="Oil Product",
        delivery_basis_name="Basis Name",
        created_on=datetime.now(),
        updated_on=datetime.now(),
    )

    with patch(
        "api.routers.services.get_latest_trading_results", new_callable=AsyncMock
    ) as mock_get_latest:
        mock_get_latest.return_value = [fake_tr]

        response = await get_trading_results_service(
            session=AsyncMock(),
            oil_id="oil1",
            delivery_type_id="dt1",
            delivery_basis_id="db1",
            limit=10
        )

        mock_get_latest.assert_awaited_once()
        assert isinstance(response, TradingResultsResponse)
        assert len(response.results) == 1
        assert response.results[0].id == fake_tr.id
