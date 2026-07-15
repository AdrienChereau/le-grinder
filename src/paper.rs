//! Simulation d'exécution paper — fidèle aux leçons live du monolith :
//! - entrées et sorties en TAKER (FAK) : on balaie le carnet réel niveau par niveau ;
//! - tailles arrondies à 2 décimales (LOT_SIZE_SCALE=2, constante SDK) ;
//! - frais taker : rate × p(1−p) × parts, appliqués à chaque fill ;
//! - sortie catastrophe : la profondeur affichée est amputée de `panic_haircut`
//!   (pendant un crash, une partie des bids a déjà disparu quand l'ordre arrive).

use crate::connectors::polymarket::PolyBook;

/// Arrondi taille Polymarket : 2 décimales, PAR DÉFAUT (jamais plus — doctrine).
pub fn round_size(sz: f64) -> f64 {
    (sz * 100.0).floor() / 100.0
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Fill {
    pub shares: f64,    // parts obtenues (achat) ou vendues (vente)
    pub notional: f64,  // $ dépensés (achat) ou récupérés (vente), AVANT frais
    pub fees: f64,      // frais taker simulés
    pub avg_price: f64, // prix moyen d'exécution
}

fn taker_fee(rate: f64, price: f64, shares: f64) -> f64 {
    rate * price * (1.0 - price) * shares
}

/// Achat taker : balaie les asks du meilleur au pire, tant que le prix reste
/// ≤ `price_cap` et qu'il reste du cash. Retourne le fill agrégé.
pub fn sweep_buy(book: &PolyBook, cash: f64, price_cap: f64, fee_rate: f64) -> Fill {
    let mut asks: Vec<_> = book.asks.iter().filter(|l| l.price <= price_cap).collect();
    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

    let mut remaining_cash = cash;
    let mut shares = 0.0;
    let mut notional = 0.0;
    let mut fees = 0.0;
    for l in asks {
        if remaining_cash <= 1e-9 {
            break;
        }
        let affordable = remaining_cash / l.price;
        let take = round_size(affordable.min(l.size));
        if take <= 0.0 {
            break;
        }
        let cost = take * l.price;
        shares += take;
        notional += cost;
        fees += taker_fee(fee_rate, l.price, take);
        remaining_cash -= cost;
    }
    Fill {
        shares,
        notional,
        fees,
        avg_price: if shares > 0.0 { notional / shares } else { 0.0 },
    }
}

/// Vente catastrophe taker : balaie les bids du meilleur au pire pour écouler
/// `shares`. La profondeur de chaque niveau est multipliée par `haircut`
/// (fraction supposée encore présente pendant le crash). Les parts invendues
/// (carnet épuisé) partent à zéro — c'est le scénario du pire, assumé.
pub fn sweep_sell_panic(book: &PolyBook, shares: f64, haircut: f64, fee_rate: f64) -> Fill {
    let mut bids: Vec<_> = book.bids.iter().collect();
    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());

    let mut remaining = shares;
    let mut sold = 0.0;
    let mut notional = 0.0;
    let mut fees = 0.0;
    for l in bids {
        if remaining <= 1e-9 {
            break;
        }
        let avail = round_size(l.size * haircut.clamp(0.0, 1.0));
        let take = round_size(remaining.min(avail));
        if take <= 0.0 {
            continue;
        }
        sold += take;
        notional += take * l.price;
        fees += taker_fee(fee_rate, l.price, take);
        remaining -= take;
    }
    Fill {
        shares: sold,
        notional,
        fees,
        avg_price: if sold > 0.0 { notional / sold } else { 0.0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::polymarket::Level;

    fn book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> PolyBook {
        PolyBook {
            bids: bids.iter().map(|&(price, size)| Level { price, size }).collect(),
            asks: asks.iter().map(|&(price, size)| Level { price, size }).collect(),
        }
    }

    #[test]
    fn buy_sweeps_cheapest_first_and_respects_cap() {
        let b = book(&[], &[(0.97, 100.0), (0.96, 2.0), (1.00, 500.0)]);
        // Cap à 0.99 : le niveau 1.00 est ignoré ; 0.96 consommé avant 0.97.
        let f = sweep_buy(&b, 10.0, 0.99, 0.0);
        assert!(f.shares > 10.0 && f.shares < 10.5, "shares={}", f.shares);
        assert!(f.avg_price > 0.96 && f.avg_price < 0.97);
        assert!((f.notional - 10.0).abs() < 0.05, "tout le cash est déployé");
    }

    #[test]
    fn buy_limited_by_depth() {
        let b = book(&[], &[(0.95, 3.0)]);
        let f = sweep_buy(&b, 100.0, 0.99, 0.0);
        assert!((f.shares - 3.0).abs() < 1e-9, "profondeur = 3 parts max");
    }

    #[test]
    fn panic_sell_applies_haircut_and_leaves_residual() {
        let b = book(&[(0.90, 10.0), (0.50, 4.0)], &[]);
        // haircut 0.5 : 5 parts à 0.90, 2 parts à 0.50, reste 3 invendues (→ 0).
        let f = sweep_sell_panic(&b, 10.0, 0.5, 0.0);
        assert!((f.shares - 7.0).abs() < 1e-9, "sold={}", f.shares);
        assert!((f.notional - (5.0 * 0.90 + 2.0 * 0.50)).abs() < 1e-9);
    }

    #[test]
    fn fees_scale_with_p_one_minus_p() {
        let b = book(&[], &[(0.95, 100.0)]);
        let f = sweep_buy(&b, 95.0, 0.99, 0.07);
        // fee = 0.07 × 0.95 × 0.05 × 100 = 0.3325
        assert!((f.fees - 0.3325).abs() < 1e-3, "fees={}", f.fees);
    }

    #[test]
    fn round_size_two_decimals_floor() {
        assert_eq!(round_size(1.0599), 1.05);
        assert_eq!(round_size(0.009), 0.0);
    }
}
