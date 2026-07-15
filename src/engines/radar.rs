//! Détecteur d'emballement (flash-crash / cascade de liquidation), porté du
//! Radar Tokyo du monolith : Order Book Imbalance (OBI) + vélocité du
//! micro-price sur un buffer circulaire glissant de 1000 ms. OBI extrême ET
//! vélocité violente corrélés → KILL (ici un simple `bool`, la garde locale
//! du Grinder décide de la vente catastrophe).

use std::collections::VecDeque;

use crate::types::BinanceOrderBook;

pub struct RadarEngine {
    lookback_ms: u64,
    history: VecDeque<(u64, f64)>, // (timestamp_ms, micro_price)
    obi_depth_levels: usize,
    obi_threshold: f64,
    velocity_threshold: f64,
}

impl RadarEngine {
    pub fn new(obi_depth_levels: usize, obi_threshold: f64, velocity_threshold: f64) -> Self {
        Self {
            lookback_ms: 1000,
            history: VecDeque::with_capacity(128),
            obi_depth_levels,
            obi_threshold,
            velocity_threshold,
        }
    }

    /// Order Book Imbalance sur `obi_depth_levels` niveaux de profondeur.
    pub fn calculate_obi(&self, book: &BinanceOrderBook) -> f64 {
        let bid_volume: f64 = book.bids.values().take(self.obi_depth_levels).sum();
        let ask_volume: f64 = book.asks.values().take(self.obi_depth_levels).sum();

        if bid_volume + ask_volume == 0.0 {
            return 0.0;
        }
        (bid_volume - ask_volume) / (bid_volume + ask_volume)
    }

    /// Vélocité du micro-price sur la fenêtre glissante ($ par lookback).
    pub fn velocity(&self) -> f64 {
        match (self.history.front(), self.history.back()) {
            (Some((_, oldest)), Some((_, newest))) => newest - oldest,
            _ => 0.0,
        }
    }

    /// Enregistre le micro-price courant, met à jour le buffer glissant et
    /// renvoie `true` (KILL) si OBI extrême ET vélocité violente sont corrélés.
    pub fn tick(&mut self, current_time_ms: u64, book: &BinanceOrderBook) -> bool {
        let Some(current_micro_price) = book.calculate_micro_price() else {
            return false;
        };
        self.history.push_back((current_time_ms, current_micro_price));

        // Purge du buffer : ne garder que les 1000 dernières ms.
        while let Some((ts, _)) = self.history.front() {
            if current_time_ms.saturating_sub(*ts) > self.lookback_ms {
                self.history.pop_front();
            } else {
                break;
            }
        }

        if self.history.len() < 2 {
            return false;
        }

        let velocity = self.velocity();
        let obi = self.calculate_obi(book);

        obi.abs() >= self.obi_threshold && velocity.abs() >= self.velocity_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OrderedFloat;
    use std::cmp::Reverse;

    fn book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> BinanceOrderBook {
        let mut b = BinanceOrderBook::new();
        for (p, q) in bids {
            b.bids.insert(Reverse(OrderedFloat(*p)), *q);
        }
        for (p, q) in asks {
            b.asks.insert(OrderedFloat(*p), *q);
        }
        b
    }

    #[test]
    fn obi_balanced_is_zero() {
        let r = RadarEngine::new(5, 0.8, 1.0);
        let b = book(&[(100.0, 10.0)], &[(101.0, 10.0)]);
        assert!((r.calculate_obi(&b)).abs() < 1e-9);
    }

    #[test]
    fn kill_fires_on_imbalance_plus_velocity() {
        let mut r = RadarEngine::new(5, 0.8, 1.0);
        // t=0 : prix ~100, fortement déséquilibré côté ask (vente massive).
        let b0 = book(&[(99.0, 5.0)], &[(100.0, 95.0)]);
        assert!(!r.tick(0, &b0)); // un seul point
        // t=500 : le prix s'est effondré, OBI toujours extrême.
        let b1 = book(&[(90.0, 5.0)], &[(91.0, 95.0)]);
        assert!(r.tick(500, &b1));
    }

    #[test]
    fn no_kill_when_calm() {
        let mut r = RadarEngine::new(5, 0.8, 1.0);
        let b0 = book(&[(100.0, 10.0)], &[(101.0, 10.0)]);
        let b1 = book(&[(100.0, 11.0)], &[(101.0, 10.0)]);
        r.tick(0, &b0);
        assert!(!r.tick(500, &b1));
    }
}
