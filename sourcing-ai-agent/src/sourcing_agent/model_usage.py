"""Provider-neutral immutable model token-usage values."""

from __future__ import annotations

from dataclasses import dataclass


class ModelUsageValidationError(ValueError):
    """Raised when a model usage field is not a non-negative integer."""


@dataclass(frozen=True, slots=True)
class ModelUsage:
    """Canonical five-field token-usage value shared by model runtimes."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_input_tokens",
            "reasoning_output_tokens",
        ):
            value = getattr(self, field_name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ModelUsageValidationError(f"model_usage_value_invalid:{field_name}")

    def to_record(self) -> dict[str, int]:
        return {
            key: value
            for key, value in (
                ("input_tokens", self.input_tokens),
                ("output_tokens", self.output_tokens),
                ("total_tokens", self.total_tokens),
                ("cached_input_tokens", self.cached_input_tokens),
                ("reasoning_output_tokens", self.reasoning_output_tokens),
            )
            if value is not None
        }


__all__ = ["ModelUsage", "ModelUsageValidationError"]
