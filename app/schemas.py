from datetime import date, datetime
from decimal import Decimal
from typing import List

from pydantic import BaseModel, Field, ConfigDict
from typing_extensions import Annotated


class LastTradingDatesResponse(BaseModel):
    dates: Annotated[List[date], Field(description="Список дат последних торговых дней")]

    model_config = ConfigDict(from_attributes=True)


class TradingResultDetail(BaseModel):
    id: Annotated[int, Field(description="Уникальный идентификатор записи")]
    exchange_product_id: Annotated[str, Field(description="Идентификатор продукта на бирже")]
    exchange_product_name: Annotated[str, Field(description="Название продукта на бирже")]
    oil_id: Annotated[str, Field(description="Идентификатор сырья")]
    delivery_basis_id: Annotated[str, Field(description="Идентификатор условий поставки")]
    delivery_basis_name: Annotated[str, Field(description="Название условий поставки")]
    delivery_type_id: Annotated[str, Field(description="Тип поставки")]
    volume: Annotated[Decimal, Field(description="Объем торгов")]
    total: Annotated[Decimal, Field(description="Общая сумма торгов")]
    count: Annotated[int, Field(description="Количество сделок")]
    date: Annotated[date, Field(description="Дата торгов")]
    created_on: Annotated[datetime, Field(description="Время создания записи")]
    updated_on: Annotated[datetime, Field(description="Время последнего обновления записи")]

    model_config = ConfigDict(from_attributes=True)


class DynamicsResponse(BaseModel):
    trades: Annotated[List[TradingResultDetail], Field(description="Список записей торгов за период")]

    model_config = ConfigDict(from_attributes=True)


class TradingResultsResponse(BaseModel):
    results: Annotated[List[TradingResultDetail], Field(description="Список результатов последних торгов")]

    model_config = ConfigDict(from_attributes=True)
