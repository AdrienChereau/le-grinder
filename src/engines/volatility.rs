//! Moteur de volatilité (J4).
//!
//! Calcule un sigma glissant sur les log-returns du micro-price sur une fenêtre
//! courte (~2s), exprimé en **volatilité annualisée** (cohérent avec le pricing
//! Black-Scholes), avec un **plancher** (`VOLATILITY_FLOOR`) pour éviter qu'un
//! marché momentanément figé ne produise un sigma nul (et un pricing dégénéré).

use std::collections::VecDeque;

const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 3600.0;

pub struct VolatilityEngine {
    window_ms: u64,
    floor: f64,
    history: VecDeque<(u64, f64)>, // (ts_ms, micro_price)
}

impl VolatilityEngine {
    pub fn new(window_ms: u64, floor: f64) -> Self {
        Self {
            window_ms,
            floor,
            history: VecDeque::with_capacity(256),
        }
    }

    /// Enregistre un nouveau point de prix et purge la fenêtre glissante.
    pub fn update(&mut self, ts_ms: u64, micro_price: f64) {
        if micro_price <= 0.0 {
            return;
        }
        self.history.push_back((ts_ms, micro_price));
        while let Some((ts, _)) = self.history.front() {
            if ts_ms.saturating_sub(*ts) > self.window_ms {
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Volatilité annualisée courante (jamais sous le plancher).
    ///
    /// Variance réalisée = Σ log-returns² sur la fenêtre ; ramenée par seconde en
    /// divisant par la durée totale, puis annualisée par √(secondes/an).
    pub fn annualized_sigma(&self) -> f64 {
        let n = self.history.len();
        if n < 2 {
            return self.floor;
        }

        let first_ts = self.history.front().unwrap().0;
        let last_ts = self.history.back().unwrap().0;
        let span_sec = (last_ts.saturating_sub(first_ts)) as f64 / 1000.0;
        if span_sec <= 0.0 {
            return self.floor;
        }

        let mut realized_var = 0.0;
        let mut prev: Option<f64> = None;
        for (_, p) in &self.history {
            if let Some(pp) = prev {
                let r = (p / pp).ln();
                realized_var += r * r;
            }
            prev = Some(*p);
        }

        let var_per_sec = realized_var / span_sec;
        let sigma_annual = (var_per_sec * SECONDS_PER_YEAR).sqrt();
        sigma_annual.max(self.floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_series_returns_floor() {
        let mut v = VolatilityEngine::new(2000, 0.40);
        for i in 0..20 {
            v.update(i * 100, 100.0);
        }
        assert!((v.annualized_sigma() - 0.40).abs() < 1e-9);
    }

    #[test]
    fn insufficient_data_returns_floor() {
        let v = VolatilityEngine::new(2000, 0.40);
        assert_eq!(v.annualized_sigma(), 0.40);
    }

    #[test]
    fn moving_series_exceeds_floor() {
        let mut v = VolatilityEngine::new(2000, 0.40);
        // Oscillation marquée → vol annualisée largement au-dessus du plancher.
        let mut p = 60000.0;
        for i in 0..20 {
            p *= if i % 2 == 0 { 1.001 } else { 0.999 };
            v.update(i * 100, p);
        }
        assert!(v.annualized_sigma() > 0.40);
    }

    #[test]
    fn window_purges_old_points() {
        let mut v = VolatilityEngine::new(1000, 0.40);
        for i in 0..50 {
            v.update(i * 100, 100.0 + i as f64);
        }
        // Fenêtre 1000ms à 100ms/point → au plus ~11 points conservés.
        assert!(v.history.len() <= 12);
    }
}
