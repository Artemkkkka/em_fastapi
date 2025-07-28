from fastapi import FastAPI

from app.routers import last_trading_dates, dynamics, trading_results


app = FastAPI(
    title="SPIMEX Trading Results API",
    version="1.0.0",
)

app.include_router(
    last_trading_dates.router,
    prefix="",
    tags=["Last Trading Dates"],
)
app.include_router(
    dynamics.router,
    prefix="",
    tags=["Dynamics"],
)
app.include_router(
    trading_results.router,
    prefix="",
    tags=["Trading Results"],
)