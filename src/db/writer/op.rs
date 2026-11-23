use super::Op;

impl Op {
    /// Written with a full sync: found blocks and payouts cannot be read back from the chain.
    pub(super) const fn durable(&self) -> bool {
        matches!(
            self,
            Self::BlockProgress { .. }
                | Self::Shutdown(_)
                | Self::BlockOutcome { .. }
                | Self::Credit { .. }
                | Self::BlockFound { .. }
                | Self::AcceptWork { .. }
                | Self::EnsureMiner { .. }
                | Self::PayoutCreate { .. }
                | Self::PayoutAttempt { .. }
                | Self::PayoutSent { .. }
                | Self::PayoutNeverSigned { .. }
                | Self::PayoutClosed { .. }
        )
    }

    /// Ops that end the batch at once. Found blocks are batched: at low difficulty every share is one.
    pub(super) const fn urgent(&self) -> bool {
        matches!(
            self,
            Self::BlockProgress { .. }
                | Self::BlockOutcome { .. }
                | Self::Credit { .. }
                | Self::PayoutCreate { .. }
                | Self::PayoutAttempt { .. }
                | Self::PayoutSent { .. }
                | Self::PayoutNeverSigned { .. }
                | Self::PayoutClosed { .. }
                | Self::Shutdown(_)
                | Self::Flush(_)
        )
    }

    pub(super) const fn kind(&self) -> &'static str {
        match self {
            Self::AcceptWork { .. } => "accepted work",
            Self::ReplayWork { .. } => "receipt replay",
            Self::EnsureMiner { .. } => "ensure miner",
            Self::Miner { .. } => "miner",
            #[cfg(test)]
            Self::Worker { .. } => "worker",
            Self::Share { .. } => "share",
            Self::Telemetry(record) => record.kind(),
            Self::BlockFound { .. } => "found",
            Self::BlockProgress { .. } => "block progress",
            Self::BlockOutcome { .. } => "durable block outcome",
            Self::BlockState { .. } => "state",
            Self::Credit { .. } => "credit",
            Self::CutHeight(_) => "cut height",
            Self::PayoutCreate { .. }
            | Self::PayoutAttempt { .. }
            | Self::PayoutSent { .. }
            | Self::PayoutNeverSigned { .. }
            | Self::PayoutClosed { .. } => "payout",
            Self::PruneAccepted { .. } => "bounded prune",
            Self::Prune { .. } => "prune",
            Self::TrimBlocks { .. } => "trim",
            Self::Setting { .. } => "setting",
            Self::Flush(_) => "flush",
            Self::Shutdown(_) => "shutdown",
        }
    }
}
