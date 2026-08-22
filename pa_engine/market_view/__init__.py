"""G021 shared candles and immutable market-view API."""

from .builder import CandleBuilder, MarketViewBuild, MarketViewBuilder, SharedCandleEngine, SharedMarketViewBuilder, build_market_view, build_shared_market_view, join_history_live, make_shared_market_view
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef
from .models import (
    ARTIFACT_KIND,
    SCHEMA_VERSION,
    Candle,
    InsufficientHistoryError,
    InvalidJoinError,
    MarketViewError,
    MarketViewValidationError,
    Readiness,
    SecondReference,
    SharedMarketView,
)
from .publisher import PublishedView, Publisher, SharedMarketViewPublisher, SharedMarketViewStore
from .validation import SharedMarketViewValidation, validate_shared_market_view, validate_shared_view, validate_view, verify_shared_market_view

__all__ = [
    "ARTIFACT_KIND",
    "ALPACA_IEX_FEED",
    "SCHEMA_VERSION",
    "Candle",
    "CandleBuilder",
    "InsufficientHistoryError",
    "InvalidJoinError",
    "MarketViewBuild",
    "MarketViewBuilder",
    "MarketViewError",
    "MarketFeedRef",
    "MarketViewValidationError",
    "PublishedView",
    "Publisher",
    "Readiness",
    "SecondReference",
    "SharedMarketView",
    "SharedCandleEngine",
    "SharedMarketViewBuilder",
    "SharedMarketViewPublisher",
    "SharedMarketViewStore",
    "SharedMarketViewValidation",
    "build_market_view",
    "build_shared_market_view",
    "join_history_live",
    "make_shared_market_view",
    "validate_shared_market_view",
    "validate_shared_view",
    "validate_view",
    "verify_shared_market_view",
]
