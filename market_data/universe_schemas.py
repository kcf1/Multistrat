"""Universe schemas (asset-only universe + resolver contracts).

Universe is base-asset only. Tradability/pricing resolvability are resolved elsewhere
(e.g. from `oms.symbols` and price-feed capabilities).
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class UniverseReadMode(str):
    current_only = "current_only"
    ever_seen = "ever_seen"


class UniverseAssetState(BaseModel):
    model_config = {"frozen": True}

    base_asset: str = Field(..., min_length=1)
    source: str = Field(default="cmc_top100", min_length=1)

    first_seen_date: date | None = None
    last_seen_date: date | None = None
    current_rank: int | None = None

    is_current_top100: bool = False
    was_ever_top100: bool = False

    updated_at: datetime | None = None

    @field_validator("base_asset")
    @classmethod
    def normalize_base_asset(cls, v: str) -> str:
        return v.strip().upper()


class UniverseTopNMember(BaseModel):
    model_config = {"frozen": True}

    base_asset: str = Field(..., min_length=1)
    rank: int = Field(..., ge=1)

    @field_validator("base_asset")
    @classmethod
    def normalize_base_asset(cls, v: str) -> str:
        return v.strip().upper()


class CmcTopNFetchResult(BaseModel):
    model_config = {"frozen": True}

    as_of_date: date
    members: list[UniverseTopNMember]


class UniverseRefreshResult(BaseModel):
    """Summary of a successful refresh/upsert run (not a DB row)."""

    model_config = {"frozen": True}

    as_of_date: date
    requested: int
    valid_members: int
    newly_added_base_assets: int
    dropped_base_assets: int


class SymbolResolutionResult(BaseModel):
    """Result of mapping base assets -> tradable symbols for a dataset/venue."""

    model_config = {"frozen": True}

    base_assets: list[str]
    symbols: list[str]
    skipped_base_assets: dict[str, str] = Field(default_factory=dict)  # base_asset -> reason


class PmsAdmissionDecision(BaseModel):
    model_config = {"frozen": True}

    base_asset: str
    admitted: bool
    reason: str | None = None

