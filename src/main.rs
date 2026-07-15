//! Point d'entrée du binaire `le_grinder`.
//!
//! Mode par défaut : PAPER. Le live exige DEUX verrous :
//!   1. compilation `--features live` (sinon TRADING_MODE=live refuse de démarrer),
//!   2. `LIVE_ARMED=true` au runtime (sinon ordres signés + loggés, jamais postés).
//!
//! Un seul process (mode « Tokyo local ») :
//!   Binance WS ──► TokyoGuard (drift / vol / radar)
//!                        │
//!   Polymarket WS+REST ──► Grinder (machine à états) ──► ledger + dashboard
//!
//! Le passage en live (radar Tokyo distant via UDP, ordres réels signés) se fera
//! après validation paper ET la matrice de complétude métier (doctrine).

mod config;
mod connectors;
mod dashboard;
mod engines;
mod grinder;
#[cfg(feature = "live")]
mod live;
mod paper;
mod state;
mod tokyo;
mod types;

use tokio::sync::watch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = config::Config::from_env();
    tracing::info!(
        mode = %cfg.mode, base = cfg.grind_base, entry_min = cfg.entry_min, z_entry = cfg.z_entry,
        "Démarrage Le Grinder"
    );

    // Dashboard.
    let dash = dashboard::shared();
    {
        let (bind, port, st) = (cfg.dashboard_bind.clone(), cfg.dashboard_port, dash.clone());
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(&bind, port, st).await {
                tracing::error!(error = %e, "dashboard arrêté");
            }
        });
    }

    // Flux Binance → garde Tokyo (canal watch, dernier snapshot gagne).
    let (btx, brx) = watch::channel::<Option<types::BookUpdate>>(None);
    {
        let url = cfg.binance_ws_url.clone();
        tokio::spawn(async move {
            if let Err(e) = connectors::binance::run(url, btx).await {
                tracing::error!(error = %e, "connecteur Binance terminé");
            }
        });
    }

    // Boucle Grinder (ne retourne qu'en erreur fatale).
    grinder::run(cfg, brx, dash).await
}
