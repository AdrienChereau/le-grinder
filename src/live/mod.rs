//! Exécution LIVE du Grinder — taker FAK uniquement, sig_type 3.
//!
//! Interface unique consommée par la machine à états : mêmes `Fill` que le
//! paper, mais mesurés sur les réponses réelles du CLOB.
//!
//! Séquence BUY  : FAK (cap = entry_max) → refresh allowance CONDITIONAL
//!                 (sans lui, le SELL suivant est rejeté « balance 0 »).
//! Séquence SELL : refresh CONDITIONAL → lire le solde RÉEL (la vérité, pas
//!                 le miroir) → FAK plancher 0.01 → retry si résidu.

pub mod auth;
pub mod orders;

use crate::paper::{round_size, Fill};
use auth::LiveCredentials;
use orders::{place_fak, FakArgs, PlaceResult};

pub struct LiveExec {
    pub creds: LiveCredentials,
    pub armed: bool, // LIVE_ARMED=true sinon dry-run (signé, jamais posté)
}

impl LiveExec {
    /// Auth + sync collatéral. Retourne le solde USDC réel du deposit wallet.
    pub async fn startup() -> anyhow::Result<(Self, f64)> {
        let creds = LiveCredentials::from_env()
            .ok_or_else(|| anyhow::anyhow!("credentials POLY_* incomplètes dans le .env"))?;
        let armed = std::env::var("LIVE_ARMED")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        orders::startup(&creds).await?;
        let collateral = auth::get_collateral_balance(&creds).await?;
        tracing::info!(collateral, armed, "LIVE prêt (deposit wallet)");
        Ok((Self { creds, armed }, collateral))
    }

    /// Achat taker de tout le `cash` disponible, cap de prix `price_cap`.
    /// Fill mesuré sur la réponse CLOB (fees réels inclus dans les montants).
    ///
    /// MONTANTS CLOB (leçon monolith 13 juil.) : un BUY FAK dépense size × prix
    /// et le CLOB exige ce montant à 2 DÉCIMALES — 2,02 × 0,99 = 1,9998 →
    /// « invalid amounts ». Taille ENTIÈRE × prix (2 déc.) = toujours valide.
    pub async fn buy_all(&self, token_id: &str, cash: f64, price_cap: f64) -> anyhow::Result<Fill> {
        let size = (cash / price_cap).floor();
        if size < 1.0 {
            anyhow::bail!("cash {cash:.2} < 1 part entière à {price_cap} (minimum CLOB)");
        }
        let res = place_fak(
            self.armed,
            &self.creds,
            token_id,
            FakArgs { price: price_cap, size, is_sell: false },
        )
        .await?;
        let fill = match res {
            PlaceResult::DryRun => Fill::default(),
            PlaceResult::Placed { filled_size, avg_price, .. } => {
                let shares = filled_size.unwrap_or(0.0);
                let avg = avg_price.unwrap_or(0.0);
                Fill { shares, notional: shares * avg, fees: 0.0, avg_price: avg }
            }
        };
        if fill.shares > 0.0 {
            // Piège connu : sans refresh CONDITIONAL après un BUY, le SELL
            // catastrophe est rejeté « balance 0 ». On le fait TOUT DE SUITE.
            if let Err(e) =
                auth::sync_balance_allowance(&self.creds, "CONDITIONAL", Some(token_id)).await
            {
                tracing::error!(error = %e, "refresh CONDITIONAL post-BUY échoué — retry au SELL");
            }
        }
        Ok(fill)
    }

    /// Vente catastrophe : liquider `shares_hint` parts (le solde on-chain réel
    /// fait autorité s'il est lisible). FAK plancher 0.01 → prend tout le book.
    /// Ne JAMAIS abandonner sur balance 0 : refresh + retry avant de conclure.
    pub async fn sell_all(&self, token_id: &str, shares_hint: f64) -> anyhow::Result<Fill> {
        let mut truth = shares_hint;
        for attempt in 0..2 {
            match auth::get_conditional_balance(&self.creds, token_id).await {
                Ok(b) if b > 0.0 => {
                    truth = b;
                    break;
                }
                Ok(_) if attempt == 0 => {
                    tracing::warn!("balance CONDITIONAL lue à 0 — refresh + retry (doctrine)");
                    let _ = auth::sync_balance_allowance(&self.creds, "CONDITIONAL", Some(token_id))
                        .await;
                }
                Ok(_) => tracing::warn!(shares_hint, "balance toujours 0 — on vend le hint"),
                Err(e) => {
                    tracing::warn!(error = %e, "lecture balance CONDITIONAL échouée");
                    break;
                }
            }
        }
        let mut remaining = round_size(truth);
        let mut agg = Fill::default();
        for attempt in 0..2 {
            if remaining <= 0.0 {
                break;
            }
            let res = place_fak(
                self.armed,
                &self.creds,
                token_id,
                FakArgs { price: 0.01, size: remaining, is_sell: true },
            )
            .await?;
            match res {
                PlaceResult::DryRun => return Ok(Fill::default()),
                PlaceResult::Placed { filled_size, avg_price, .. } => {
                    let sold = filled_size.unwrap_or(0.0);
                    let avg = avg_price.unwrap_or(0.0);
                    agg.shares += sold;
                    agg.notional += sold * avg;
                    remaining = round_size(remaining - sold);
                    if remaining > 0.0 && attempt == 0 {
                        tracing::warn!(remaining, "résidu invendu — 2e FAK");
                    }
                }
            }
        }
        if remaining > 0.0 {
            tracing::error!(remaining, "résidu final invendu — ira à la résolution");
        }
        agg.avg_price = if agg.shares > 0.0 { agg.notional / agg.shares } else { 0.0 };
        Ok(agg)
    }

    pub async fn collateral(&self) -> anyhow::Result<f64> {
        auth::get_collateral_balance(&self.creds).await
    }
}
