//! Structures de données centrales du Grinder (carnet L2 Binance).
//! Version allégée des types du monolith — pas de signal wire ici :
//! la garde Tokyo tourne en local dans le même process (mode paper).

use std::cmp::Reverse;
use std::collections::BTreeMap;

/// Wrapper `f64` totalement ordonné, utilisable comme clé de `BTreeMap`.
/// NaN est traité comme égal (jamais produit par les flux de prix).
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Carnet d'ordres L2 local de Binance.
/// Les bids sont triés du plus cher au moins cher via `Reverse`,
/// les asks du moins cher au plus cher.
#[derive(Debug, Clone, Default)]
pub struct BinanceOrderBook {
    pub last_update_id: u64,
    pub bids: BTreeMap<Reverse<OrderedFloat>, f64>,
    pub asks: BTreeMap<OrderedFloat, f64>,
}

impl BinanceOrderBook {
    pub fn new() -> Self {
        Self {
            last_update_id: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Meilleur prix acheteur (plus haut bid).
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next().map(|k| k.0 .0)
    }

    /// Meilleur prix vendeur (plus bas ask).
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|k| k.0)
    }

    /// Micro-price pondéré par la profondeur du top-of-book :
    /// `(bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)`.
    pub fn calculate_micro_price(&self) -> Option<f64> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        let bid_depth = *self.bids.values().next()?;
        let ask_depth = *self.asks.values().next()?;

        if bid_depth + ask_depth == 0.0 {
            return None;
        }
        Some(((best_bid * ask_depth) + (best_ask * bid_depth)) / (bid_depth + ask_depth))
    }
}

/// Snapshot du carnet Binance publié sur un canal `watch` vers la garde Tokyo.
#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub book: BinanceOrderBook,
    pub ts_ms: u64,
}
