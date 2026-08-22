"""G015 canonical market validation and deterministic observations."""

from .feed import (
    ALPACA_IEX_FEED,
    ALPACA_IEX_MARKET_FEED,
    MarketFeedRef,
    extract_market_feed,
    extract_market_feed_ref,
    feed_from_artifact,
    market_feed_from_artifact,
    market_feed_from_extensions,
    validate_feed_ref,
    validate_market_feed_ref,
)
from .observations import FORMULA_VERSION, build_observation_frame
from .validation import (
    MarketAssessment,
    MarketDataError,
    PolicyOutcome,
    assess_market,
    assess_market_bundle,
    validate_analysis_policy,
    validate_analysis_request,
    validate_closed_bar_window,
    validate_market_snapshot,
)

__all__ = [
    "ALPACA_IEX_FEED",
    "ALPACA_IEX_MARKET_FEED",
    "FORMULA_VERSION",
    "MarketAssessment",
    "MarketDataError",
    "MarketFeedRef",
    "PolicyOutcome",
    "assess_market",
    "assess_market_bundle",
    "build_observation_frame",
    "extract_market_feed",
    "extract_market_feed_ref",
    "feed_from_artifact",
    "market_feed_from_artifact",
    "market_feed_from_extensions",
    "validate_analysis_policy",
    "validate_analysis_request",
    "validate_closed_bar_window",
    "validate_feed_ref",
    "validate_market_feed_ref",
    "validate_market_snapshot",
]
