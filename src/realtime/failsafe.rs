use crate::evidence::metrics::percentiles_us;
use serde::Serialize;
use std::{collections::VecDeque, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FailSafeState {
    Normal,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailSafeTransition {
    None,
    EnteredDegraded,
    RecoveredNormal,
}

#[derive(Debug)]
pub struct FailSafeController {
    state: FailSafeState,
    jitter_threshold_ns: u128,
    window_size: usize,
    stable_windows_required: usize,
    stable_windows: usize,
    deadline_miss_limit: usize,
    processing_window: VecDeque<u128>,
    deadline_window: VecDeque<bool>,
}

impl FailSafeController {
    pub fn new(jitter_threshold: Duration, window_size: usize, deadline_miss_limit: usize) -> Self {
        Self {
            state: FailSafeState::Normal,
            jitter_threshold_ns: jitter_threshold.as_nanos(),
            window_size: window_size.max(4),
            stable_windows_required: 3,
            stable_windows: 0,
            deadline_miss_limit,
            processing_window: VecDeque::new(),
            deadline_window: VecDeque::new(),
        }
    }

    pub fn state(&self) -> FailSafeState {
        self.state
    }

    pub fn is_degraded(&self) -> bool {
        self.state == FailSafeState::Degraded
    }

    pub fn observe(&mut self, processing_ns: u128, deadline_missed: bool) -> FailSafeTransition {
        self.processing_window.push_back(processing_ns);
        self.deadline_window.push_back(deadline_missed);

        while self.processing_window.len() > self.window_size {
            self.processing_window.pop_front();
        }
        while self.deadline_window.len() > self.window_size {
            self.deadline_window.pop_front();
        }

        let p99_ns = percentiles_us(&self.processing_window.iter().copied().collect::<Vec<_>>())
            .p99_us
            * 1_000.0;
        let misses = self
            .deadline_window
            .iter()
            .filter(|missed| **missed)
            .count();

        match self.state {
            FailSafeState::Normal
                if p99_ns as u128 > self.jitter_threshold_ns
                    || misses >= self.deadline_miss_limit =>
            {
                self.state = FailSafeState::Degraded;
                self.stable_windows = 0;
                FailSafeTransition::EnteredDegraded
            }
            FailSafeState::Degraded => {
                let stable = p99_ns as u128 <= self.jitter_threshold_ns / 2 && misses == 0;
                if stable {
                    self.stable_windows += 1;
                } else {
                    self.stable_windows = 0;
                }

                if self.stable_windows >= self.stable_windows_required {
                    self.state = FailSafeState::Normal;
                    self.stable_windows = 0;
                    FailSafeTransition::RecoveredNormal
                } else {
                    FailSafeTransition::None
                }
            }
            _ => FailSafeTransition::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enters_and_recovers_degraded_mode() {
        let mut fail_safe = FailSafeController::new(Duration::from_micros(1_000), 4, 2);

        assert_eq!(
            fail_safe.observe(2_500_000, true),
            FailSafeTransition::EnteredDegraded
        );
        assert!(fail_safe.is_degraded());

        for _ in 0..7 {
            fail_safe.observe(100_000, false);
        }

        assert_eq!(fail_safe.state(), FailSafeState::Normal);
    }
}
