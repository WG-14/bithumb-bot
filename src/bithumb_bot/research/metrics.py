from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchMetrics:
    return_pct: float
    max_drawdown_pct: float
    profit_factor: float | None
    trade_count: int
    win_rate: float
    avg_win: float | None
    avg_loss: float | None
    fee_total: float
    slippage_total: float
    max_consecutive_losses: int
    single_trade_dependency_score: float | None
    parameter_stability_score: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "return_pct": self.return_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "profit_factor": self.profit_factor,
            "trade_count": self.trade_count,
            "win_rate": self.win_rate,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "fee_total": self.fee_total,
            "slippage_total": self.slippage_total,
            "max_consecutive_losses": self.max_consecutive_losses,
            "single_trade_dependency_score": self.single_trade_dependency_score,
            "parameter_stability_score": self.parameter_stability_score,
        }
