//! Serveur de monitoring local (même pattern que le monolith : HTTP minimal
//! sans framework, frontend embarqué à la compilation).
//!
//!   - `GET /`       → dashboard (index.html)
//!   - `GET /state`  → snapshot JSON de l'état du bot

use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::state::{DailyStat, WindowRecord};

const INDEX_HTML: &str = include_str!("../frontend/index.html");

/// Snapshot partagé alimenté par la boucle Grinder.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DashState {
    // Garde Tokyo
    pub binance_connected: bool,
    pub spot: f64,
    pub drift: f64,
    pub sigma: f64,
    pub obi: f64,
    pub velocity: f64,
    pub kill: bool,
    pub guard_age_ms: i64,
    // Marché courant
    pub market_slug: String,
    pub remaining_s: i64,
    pub strike: f64,
    pub up_ask: f64,
    pub down_ask: f64,
    // Grinder
    pub phase: String, // "scanning" | "in_position" | "waiting_market"
    pub stack: f64,
    pub streak: u32,
    pub run_id: u64,
    pub best_streak: u32,
    pub best_stack: f64,
    pub windows_played: u64,
    pub wins: u64,
    pub losses: u64,
    pub panic_exits: u64,
    pub realized_pnl: f64,
    pub last_block_reason: String,
    pub mode: String,        // "paper" | "live" | "live (dry-run)"
    pub live_collateral: f64, // USDC réel du wallet (0 en paper)
    pub asset: String,        // "btc", "eth", …
    /// Wallet au début du jour UTC (snapshot) — PnL réel du jour = wallet − open.
    pub wallet_day_open: f64,
    /// Agrégats des 7 derniers jours (ledger).
    pub daily: Vec<DailyStat>,
    pub banked: f64,          // cumul écrémé par le cap Kelly (0 si désactivé)
    // Position ouverte (zéros si aucune)
    pub pos_side: String,
    pub pos_shares: f64,
    pub pos_cost: f64,
    pub pos_entry_price: f64,
    pub pos_z: f64,
    // Historique (ring, plus récent en dernier)
    pub windows: Vec<WindowRecord>,
}

pub type Shared = Arc<RwLock<DashState>>;

pub fn shared() -> Shared {
    Arc::new(RwLock::new(DashState::default()))
}

pub async fn serve(bind: &str, port: u16, state: Shared) -> anyhow::Result<()> {
    let listener = TcpListener::bind((bind, port)).await?;
    tracing::info!(%bind, port, "dashboard en écoute");
    loop {
        let (mut sock, _) = listener.accept().await?;
        let st = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let n = match sock.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req.split_whitespace().nth(1).unwrap_or("/");
            let (status, ctype, body) = match path {
                "/state" => {
                    let snap = st.read().await.clone();
                    (
                        "200 OK",
                        "application/json",
                        serde_json::to_string(&snap).unwrap_or_else(|_| "{}".into()),
                    )
                }
                "/" | "/index.html" => ("200 OK", "text/html; charset=utf-8", INDEX_HTML.to_string()),
                _ => ("404 Not Found", "text/plain", "not found".into()),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}
