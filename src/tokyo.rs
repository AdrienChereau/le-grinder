//! Garde Tokyo locale : agrège les moteurs drift / volatilité / radar sur le
//! flux Binance et publie un état de garde consommé par le Grinder.
//!
//! C'est la SEULE sécurité de la stratégie : tant que la garde est verte, la
//! position court jusqu'à la résolution ; si elle passe au rouge (KILL radar,
//! marge z insuffisante, drift à contre-sens), le Grinder vend en catastrophe.

use crate::config::Config;
use crate::engines::drift::DriftEngine;
use crate::engines::radar::RadarEngine;
use crate::engines::volatility::VolatilityEngine;
use crate::types::BookUpdate;

const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 3600.0;

/// État instantané publié par la garde après chaque tick Binance.
#[derive(Debug, Clone, Copy, Default)]
pub struct GuardState {
    pub ts_ms: u64,
    pub spot: f64,      // micro-price Binance
    pub drift: f64,     // drift log PAR SECONDE (échelle ~1e-5, cf. doctrine)
    pub sigma: f64,     // volatilité annualisée EWMA
    pub obi: f64,       // order book imbalance [-1, 1]
    pub velocity: f64,  // déplacement $ du micro-price sur 1 s
    pub kill: bool,     // détecteur d'emballement radar (flash-crash)
}

impl GuardState {
    /// Volatilité par seconde (dé-annualisée), pour les scores z sur horizon court.
    pub fn sigma_per_sec(&self) -> f64 {
        self.sigma / SECONDS_PER_YEAR.sqrt()
    }

    /// Score z de la marge du côté joué : distance log du spot au strike,
    /// normalisée par le mouvement attendu sur le temps restant.
    /// `dir` = +1.0 si on tient Up (il faut spot > strike), -1.0 si Down.
    /// Négatif = le spot est du MAUVAIS côté du strike.
    pub fn margin_z(&self, strike: f64, dir: f64, remaining_s: f64) -> f64 {
        if strike <= 0.0 || self.spot <= 0.0 {
            return 0.0;
        }
        let edge = (self.spot / strike).ln() * dir;
        let denom = self.sigma_per_sec() * remaining_s.max(1.0).sqrt();
        if denom <= 0.0 {
            return 0.0;
        }
        edge / denom
    }

    /// Le flux est-il frais ? (données Binance < `max_age_ms`)
    pub fn is_fresh(&self, now_ms: u64, max_age_ms: u64) -> bool {
        self.ts_ms > 0 && now_ms.saturating_sub(self.ts_ms) <= max_age_ms
    }
}

/// Agrégat des trois moteurs, mis à jour tick par tick.
pub struct TokyoGuard {
    drift: DriftEngine,
    vol: VolatilityEngine,
    radar: RadarEngine,
    state: GuardState,
    kill_latch_until_ms: u64, // un KILL reste actif quelques secondes (pas de rebond)
    kill_latch_ms: u64,
}

impl TokyoGuard {
    pub fn new(cfg: &Config) -> Self {
        Self {
            drift: DriftEngine::new(cfg.drift_halflife_s),
            vol: VolatilityEngine::new(cfg.vol_window_ms, cfg.vol_floor),
            radar: RadarEngine::new(
                cfg.obi_depth_levels,
                cfg.obi_threshold,
                cfg.velocity_threshold,
            ),
            state: GuardState::default(),
            kill_latch_until_ms: 0,
            kill_latch_ms: cfg.kill_latch_ms,
        }
    }

    /// Intègre un snapshot de carnet Binance et retourne l'état de garde.
    pub fn update(&mut self, u: &BookUpdate) -> GuardState {
        let raw_kill = self.radar.tick(u.ts_ms, &u.book);
        if raw_kill {
            self.kill_latch_until_ms = u.ts_ms + self.kill_latch_ms;
        }
        if let Some(micro) = u.book.calculate_micro_price() {
            self.drift.update(u.ts_ms, micro);
            self.vol.update(u.ts_ms, micro);
            self.state.spot = micro;
        }
        self.state.ts_ms = u.ts_ms;
        self.state.drift = self.drift.per_sec();
        self.state.sigma = self.vol.annualized_sigma();
        self.state.obi = self.radar.calculate_obi(&u.book);
        self.state.velocity = self.radar.velocity();
        self.state.kill = u.ts_ms < self.kill_latch_until_ms;
        self.state
    }

    pub fn state(&self) -> GuardState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BinanceOrderBook, OrderedFloat};
    use std::cmp::Reverse;

    fn update(ts_ms: u64, bid: (f64, f64), ask: (f64, f64)) -> BookUpdate {
        let mut book = BinanceOrderBook::new();
        book.bids.insert(Reverse(OrderedFloat(bid.0)), bid.1);
        book.asks.insert(OrderedFloat(ask.0), ask.1);
        BookUpdate { book, ts_ms }
    }

    #[test]
    fn margin_z_sign_follows_side() {
        let s = GuardState {
            spot: 60100.0,
            sigma: 0.5,
            ts_ms: 1,
            ..Default::default()
        };
        // Spot au-dessus du strike : marge positive pour Up, négative pour Down.
        assert!(s.margin_z(60000.0, 1.0, 60.0) > 0.0);
        assert!(s.margin_z(60000.0, -1.0, 60.0) < 0.0);
    }

    #[test]
    fn kill_latches_for_configured_duration() {
        let cfg = Config::default_for_tests();
        let mut g = TokyoGuard::new(&cfg);
        // Calme.
        g.update(&update(0, (100000.0, 10.0), (100001.0, 10.0)));
        // Crash : OBI extrême + chute violente en 500 ms.
        g.update(&update(500, (99000.0, 1.0), (99001.0, 99.0)));
        let st = g.update(&update(600, (98900.0, 1.0), (98901.0, 99.0)));
        assert!(st.kill, "kill doit être actif pendant le crash");
        // Toujours latché juste après…
        let st = g.update(&update(600 + cfg.kill_latch_ms - 1, (98900.0, 10.0), (98901.0, 10.0)));
        assert!(st.kill, "kill doit rester latché");
        // …et relâché après la fenêtre de latch.
        let st = g.update(&update(700 + cfg.kill_latch_ms, (98900.0, 10.0), (98901.0, 10.0)));
        assert!(!st.kill, "kill doit être relâché après le latch");
    }
}
