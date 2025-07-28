from sqlalchemy import Column, Integer, Numeric, Text, Date, DateTime
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class TradingResult(Base):
    __tablename__ = 'spimex_trading_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(Text)
    exchange_product_name = Column(Text)
    oil_id = Column(Text)
    delivery_basis_id = Column(Text)
    delivery_basis_name = Column(Text)
    delivery_type_id = Column(Text)
    volume = Column(Numeric)
    total = Column(Numeric)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)
