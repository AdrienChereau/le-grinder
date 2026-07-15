//! Le Grinder — machine à états de la stratégie.
//!
//! Principe : sur chaque fenêtre BTC Up/Down 5 min, si un côté cote ≥ 95c ET que
//! la garde Tokyo confirme (marge z sur le spot Binance, drift non contraire,
//! pas de KILL radar), on achète ce côté en taker avec TOUT le stack. Si la
//! fenêtre se résout pour nous, le stack composé est remis en jeu sur une
//! fenêtre suivante — exponentiel pur. La seule défense en position est Tokyo :
//! KILL radar, marge z qui fond ou drift qui se retourne → vente catastrophe
//! immédiate (balayage des bids avec haircut). Une fenêtre perdue à la
//! résolution = wipe → retour à la mise de base, nouveau run.
//!
//! Une seule position à la fois, une seule entrée par fenêtre.

use std::time::Duration;

use chrono::Utc;
use tokio::sync::watch;

use crate::config::Config;
use crate::connectors::binance::price_at_window_open;
use crate::connectors::pm_ws::{self, PmWsShared};
use crate::connectors::polymarket::{Market, PolyBook, PolymarketClient};
use crate::dashboard::Shared as DashShared;
#[cfg(feature = "live")]
use crate::live::LiveExec;
use crate::paper::{self, Fill};
use crate::state::{self, GrinderState, WindowRecord};
use crate::tokyo::{GuardState, TokyoGuard};
use crate::types::BookUpdate;

/// Position ouverte (paper).
#[derive(Debug, Clone)]
struct Position {
    side_up: bool,
    token_id: String,
    shares: f64,
    cost: f64,        // notional + frais d'entrée = stack engagé sur la fenêtre
    entry_fees: f64,
    entry_price: f64,
    cash_left: f64,   // reliquat du stack non déployé (profondeur insuffisante)
    z_at_entry: f64,
    spot_at_entry: f64,
    sigma_at_entry: f64,
    remaining_s_at_entry: i64,
    window_ts: i64,
    slug: String,
    strike: f64,
    end_ms: i64,
}

impl Position {
    fn dir(&self) -> f64 {
        if self.side_up { 1.0 } else { -1.0 }
    }
    fn side_str(&self) -> &'static str {
        if self.side_up { "up" } else { "down" }
    }
}

enum Phase {
    /// Pas de position — on cherche une entrée sur la fenêtre courante.
    Scanning,
    /// Position ouverte, garde Tokyo en surveillance continue.
    InPosition(Position),
}

pub struct Grinder {
    cfg: Config,
    client: PolymarketClient,
    guard: TokyoGuard,
    pm_state: PmWsShared,
    tokens_tx: watch::Sender<Vec<String>>,
    dash: DashShared,
    st: GrinderState,
    phase: Phase,
    market: Option<Market>,
    strike: Option<f64>,
    /// Dernière fenêtre jouée (entrée prise) — une seule entrée par fenêtre.
    last_played_window: i64,
    /// Exécuteur live (None = paper). Compilé uniquement avec --features live.
    #[cfg(feature = "live")]
    live: Option<LiveExec>,
}

pub async fn run(
    cfg: Config,
    mut binance_rx: watch::Receiver<Option<BookUpdate>>,
    dash: DashShared,
) -> anyhow::Result<()> {
    let live_requested = cfg.mode == "live";
    #[cfg(not(feature = "live"))]
    if live_requested {
        anyhow::bail!("TRADING_MODE=live mais binaire compilé SANS --features live");
    }
    #[cfg(feature = "live")]
    let live = if live_requested {
        let (exec, collateral) = LiveExec::startup().await?;
        if collateral < cfg.grind_base {
            anyhow::bail!(
                "collatéral {collateral:.2}$ < mise de base {}$ — dépôt requis",
                cfg.grind_base
            );
        }
        Some(exec)
    } else {
        None
    };

    let pm_state: PmWsShared = Default::default();
    let tokens_tx = pm_ws::spawn(pm_state.clone());
    let mut st = state::load(&cfg.state_path, cfg.grind_base);
    // Live : le stack ne peut jamais excéder le collatéral réel du wallet.
    #[cfg(feature = "live")]
    if let Some(l) = &live {
        if let Ok(c) = l.collateral().await {
            if st.stack > c {
                tracing::warn!(stack = st.stack, collateral = c, "stack plafonné au collatéral");
                st.stack = c;
            }
        }
    }
    tracing::info!(stack = st.stack, run = st.run_id, streak = st.streak, "état Grinder chargé");
    {
        let mut d = dash.write().await;
        d.windows = state::tail_windows(&cfg.windows_path, 50);
        apply_state(&mut d, &st);
        d.phase = "waiting_market".into();
        d.stack = st.stack;
    }

    let mut g = Grinder {
        guard: TokyoGuard::new(&cfg),
        client: PolymarketClient::new(),
        pm_state,
        tokens_tx,
        dash,
        st,
        phase: Phase::Scanning,
        market: None,
        strike: None,
        last_played_window: 0,
        #[cfg(feature = "live")]
        live,
        cfg,
    };

    let mut housekeeping = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            changed = binance_rx.changed() => {
                if changed.is_err() {
                    anyhow::bail!("flux Binance fermé");
                }
                let update = binance_rx.borrow().clone();
                if let Some(u) = update {
                    let gs = g.guard.update(&u);
                    // Chemin chaud : la position est réévaluée à CHAQUE tick (10 Hz).
                    g.check_exits(gs).await;
                }
            }
            _ = housekeeping.tick() => {
                g.housekeeping().await;
            }
        }
    }
}

impl Grinder {
    /// Boucle lente (2 Hz) : rollover marché, strike, résolution, entrée, dashboard.
    async fn housekeeping(&mut self) {
        let now_ms = Utc::now().timestamp_millis();

        // 1. Résolution d'une position arrivée à échéance.
        if let Phase::InPosition(pos) = &self.phase {
            if now_ms >= pos.end_ms {
                let pos = pos.clone();
                self.resolve(&pos).await;
                self.phase = Phase::Scanning;
            }
        }

        // 1bis. Garde aveugle : flux Binance mort EN POSITION → sortie de
        // sécurité (check_exits ne tourne que sur tick, donc jamais si le flux
        // est mort — ce chemin-ci est cadencé par le housekeeping 2 Hz).
        if self.cfg.guard_stale_exit_s > 0 {
            if let Phase::InPosition(pos) = &self.phase {
                let gs = self.guard.state();
                let max_age_ms = (self.cfg.guard_stale_exit_s * 1000) as u64;
                if now_ms < pos.end_ms && !gs.is_fresh(now_ms as u64, max_age_ms) {
                    let pos = pos.clone();
                    tracing::warn!("garde AVEUGLE (flux Binance périmé) — sortie de sécurité");
                    let remaining_s = ((pos.end_ms - now_ms) as f64 / 1000.0).max(0.0);
                    let z = gs.margin_z(pos.strike, pos.dir(), remaining_s);
                    self.panic_exit(&pos, "guard_stale", z, gs).await;
                    self.phase = Phase::Scanning;
                }
            }
        }

        // 2. Rollover / découverte du marché courant.
        let need_market = match &self.market {
            None => true,
            Some(m) => m.time_remaining_sec() <= 0,
        };
        if need_market {
            match self.client.get_current_btc_5m_market().await {
                Ok(Some(m)) => {
                    tracing::info!(slug = %m.slug, "nouvelle fenêtre");
                    let _ = self
                        .tokens_tx
                        .send(vec![m.up_token_id.clone(), m.down_token_id.clone()]);
                    self.strike = None;
                    self.market = Some(m);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(error = %e, "résolution marché échouée"),
            }
        }

        // 3. Strike de la fenêtre (open Binance 1m — proxy du strike Chainlink).
        if self.strike.is_none() {
            if let Some(m) = &self.market {
                // L'open n'existe qu'une fois la fenêtre démarrée.
                if Utc::now().timestamp() >= m.window_ts {
                    match price_at_window_open(m.window_ts).await {
                        Ok(px) => {
                            tracing::info!(strike = px, window = m.window_ts, "strike fixé");
                            self.strike = Some(px);
                        }
                        Err(e) => tracing::debug!(error = %e, "strike indisponible, retry"),
                    }
                }
            }
        }

        // 4. Tentative d'entrée.
        if matches!(self.phase, Phase::Scanning) {
            self.try_enter().await;
        }

        // 5. Dashboard.
        self.publish_dash(now_ms as u64).await;
    }

    /// Conditions d'entrée : fenêtre jouable, un côté ≥ ENTRY_MIN, garde verte.
    async fn try_enter(&mut self) {
        let gs = self.guard.state();
        let now_ms = Utc::now().timestamp_millis() as u64;
        let Some(market) = self.market.clone() else { return };
        let Some(strike) = self.strike else {
            self.block("strike indisponible").await;
            return;
        };
        if market.window_ts == self.last_played_window {
            return; // une seule entrée par fenêtre
        }
        let remaining = market.time_remaining_sec();
        if remaining < self.cfg.min_remaining_s || remaining > self.cfg.max_remaining_s {
            return;
        }
        if !gs.is_fresh(now_ms, self.cfg.guard_max_age_ms) {
            self.block("garde Tokyo aveugle (flux Binance périmé)").await;
            return;
        }
        if gs.kill {
            self.block("KILL radar actif").await;
            return;
        }

        let mut any_in_band = false;
        for (side_up, token) in [
            (true, market.up_token_id.clone()),
            (false, market.down_token_id.clone()),
        ] {
            let dir = if side_up { 1.0 } else { -1.0 };
            let Some(book) = self.best_book(&token).await else { continue };
            let Some(ask) = book.best_ask() else { continue };
            if ask < self.cfg.entry_min || ask > self.cfg.entry_max {
                continue;
            }
            any_in_band = true;
            // Le marché dit ≥95c — Tokyo doit CONFIRMER, sinon pas d'entrée.
            let z = gs.margin_z(strike, dir, remaining as f64);
            if z < self.cfg.z_entry {
                self.block(&format!("z insuffisant ({z:.2} < {})", self.cfg.z_entry)).await;
                continue;
            }
            if gs.drift * dir < -self.cfg.drift_exit {
                self.block("drift à contre-sens").await;
                continue;
            }

            // Entrée taker : tout le stack, balayage du carnet réel.
            let Some(fill) = self.exec_buy(&book, &token).await else { continue };
            if fill.shares <= 0.0 {
                self.block("profondeur ask insuffisante (ou dry-run live)").await;
                continue;
            }
            let cash_left = (self.st.stack - fill.notional - fill.fees).max(0.0);
            let pos = Position {
                side_up,
                token_id: token,
                shares: fill.shares,
                cost: self.st.stack,
                entry_fees: fill.fees,
                entry_price: fill.avg_price,
                cash_left,
                z_at_entry: z,
                spot_at_entry: gs.spot,
                sigma_at_entry: gs.sigma,
                remaining_s_at_entry: remaining,
                window_ts: market.window_ts,
                slug: market.slug.clone(),
                strike,
                end_ms: market.end_time.timestamp_millis(),
            };
            tracing::info!(
                side = pos.side_str(), price = fill.avg_price, shares = fill.shares,
                stack = self.st.stack, z, "ENTRÉE Grinder"
            );
            self.last_played_window = market.window_ts;
            self.phase = Phase::InPosition(pos);
            return;
        }
        if !any_in_band {
            self.block(&format!(
                "aucun côté dans [{}, {}]",
                self.cfg.entry_min, self.cfg.entry_max
            ))
            .await;
        }
    }

    /// Chemin chaud : garde Tokyo sur position ouverte, à chaque tick Binance.
    async fn check_exits(&mut self, gs: GuardState) {
        let Phase::InPosition(pos) = &self.phase else { return };
        let now_ms = Utc::now().timestamp_millis();
        if now_ms >= pos.end_ms {
            return; // la résolution appartient au housekeeping
        }
        let remaining_s = ((pos.end_ms - now_ms) as f64 / 1000.0).max(0.0);
        let z = gs.margin_z(pos.strike, pos.dir(), remaining_s);

        let dist_usd = (gs.spot - pos.strike).abs();
        let reason = if gs.kill {
            Some("radar_kill")
        } else if self.cfg.dist_exit_usd > 0.0 && dist_usd < self.cfg.dist_exit_usd {
            // Plancher absolu : à quelques $ du strike, le z ne veut plus rien
            // dire (calibration 15 juil. : le seul vrai crash est sorti à 10 $).
            Some("dist_floor")
        } else if z < self.cfg.z_exit {
            Some("z_floor")
        } else if gs.drift * pos.dir() < -self.cfg.drift_exit {
            Some("drift_against")
        } else {
            None
        };
        let Some(reason) = reason else { return };

        let pos = pos.clone();
        tracing::warn!(reason, z, drift = gs.drift, "VENTE CATASTROPHE");
        self.panic_exit(&pos, reason, z, gs).await;
        self.phase = Phase::Scanning;
    }

    /// Achat : CLOB réel en live, sweep simulé en paper.
    async fn exec_buy(&self, book: &PolyBook, token: &str) -> Option<Fill> {
        #[cfg(feature = "live")]
        if let Some(l) = &self.live {
            return match l.buy_all(token, self.st.stack, self.cfg.entry_max).await {
                Ok(f) => Some(f),
                Err(e) => {
                    tracing::error!(error = %e, "BUY live échoué");
                    None
                }
            };
        }
        let _ = token;
        Some(paper::sweep_buy(book, self.st.stack, self.cfg.entry_max, self.cfg.taker_fee_rate))
    }

    /// Vente catastrophe : FAK plancher 0.01 en live, sweep+haircut en paper.
    async fn exec_sell(&self, token: &str, shares: f64) -> Fill {
        #[cfg(feature = "live")]
        if let Some(l) = &self.live {
            return match l.sell_all(token, shares).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(error = %e, "SELL live échoué — résidu à la résolution");
                    Fill::default()
                }
            };
        }
        let book = self.best_book(token).await.unwrap_or_default();
        paper::sweep_sell_panic(&book, shares, self.cfg.panic_haircut, self.cfg.taker_fee_rate)
    }

    /// Vente catastrophe : balayage des bids avec haircut, le reste part à zéro.
    async fn panic_exit(&mut self, pos: &Position, reason: &str, z: f64, gs: GuardState) {
        let fill = self.exec_sell(&pos.token_id, pos.shares).await;
        let proceeds = (fill.notional - fill.fees).max(0.0) + pos.cash_left;
        let recovered_pct = if pos.cost > 0.0 { 100.0 * proceeds / pos.cost } else { 0.0 };
        tracing::warn!(
            sold = fill.shares, of = pos.shares, avg = fill.avg_price,
            proceeds, recovered_pct, "sortie catastrophe exécutée"
        );
        self.st.panic_exits += 1;
        self.settle(pos, "panic", reason, proceeds, fill.fees, z, gs).await;
    }

    /// Résolution à l'échéance : spot Binance vs strike (proxy Chainlink).
    /// Égalité parfaite → défaite (conservateur).
    async fn resolve(&mut self, pos: &Position) {
        let gs = self.guard.state();
        let won = (gs.spot - pos.strike) * pos.dir() > 0.0;
        let proceeds = if won { pos.shares + pos.cash_left } else { pos.cash_left };
        if won {
            self.st.wins += 1;
        } else {
            self.st.losses += 1;
        }
        let remaining_s = 0.0;
        let z = gs.margin_z(pos.strike, pos.dir(), remaining_s);
        tracing::info!(
            won, spot = gs.spot, strike = pos.strike, proceeds,
            "résolution fenêtre {}", pos.slug
        );
        self.settle(pos, if won { "win" } else { "loss" }, "resolution", proceeds, 0.0, z, gs)
            .await;
    }

    /// Clôture comptable commune (win / loss / panic) : PnL, compoundage ou
    /// reset de run, grand livre, persistance.
    async fn settle(
        &mut self,
        pos: &Position,
        outcome: &str,
        reason: &str,
        proceeds: f64,
        exit_fees: f64,
        z_exit: f64,
        gs: GuardState,
    ) {
        let pnl = proceeds - pos.cost;
        self.st.windows_played += 1;
        self.st.realized_pnl += pnl;

        if outcome == "win" {
            self.st.streak += 1;
            self.st.best_streak = self.st.best_streak.max(self.st.streak);
            self.st.stack = proceeds;
        } else if proceeds >= self.cfg.grind_base {
            // Sortie catastrophe qui sauve plus que la base : on continue le run
            // avec ce qui a été récupéré (la série de wins, elle, est cassée).
            self.st.streak = 0;
            self.st.stack = proceeds;
        } else {
            // Wipe : retour à la mise de base, nouveau run.
            self.st.streak = 0;
            self.st.run_id += 1;
            self.st.stack = self.cfg.grind_base;
        }
        self.st.best_stack = self.st.best_stack.max(self.st.stack);

        let rec = WindowRecord {
            ts: Utc::now().to_rfc3339(),
            window_ts: pos.window_ts,
            slug: pos.slug.clone(),
            side: pos.side_str().into(),
            entry_price: pos.entry_price,
            shares: pos.shares,
            cost: pos.cost,
            fees: pos.entry_fees + exit_fees,
            outcome: outcome.into(),
            reason: reason.into(),
            proceeds,
            pnl,
            stack_after: self.st.stack,
            streak_after: self.st.streak,
            run_id: self.st.run_id,
            strike: pos.strike,
            spot_at_exit: gs.spot,
            z_at_entry: pos.z_at_entry,
            z_at_exit: z_exit,
            drift_at_exit: gs.drift,
            spot_at_entry: pos.spot_at_entry,
            dist_at_entry: (pos.spot_at_entry - pos.strike).abs(),
            sigma_at_entry: pos.sigma_at_entry,
            remaining_s_at_entry: pos.remaining_s_at_entry,
        };
        if let Err(e) = state::append_window(&self.cfg.windows_path, &rec) {
            tracing::error!(error = %e, "écriture grand livre échouée");
        }
        if let Err(e) = state::save(&self.cfg.state_path, &self.st) {
            tracing::error!(error = %e, "sauvegarde état échouée");
        }
        // Live : le collatéral wallet est le SEUL PnL qui fait foi — on le lit
        // après chaque clôture et on l'affiche à côté du ledger interne.
        #[cfg(feature = "live")]
        if let Some(l) = &self.live {
            match l.collateral().await {
                Ok(c) => {
                    tracing::info!(collateral = c, ledger_stack = self.st.stack, "vérité wallet post-settle");
                    self.dash.write().await.live_collateral = c;
                }
                Err(e) => tracing::warn!(error = %e, "lecture collatéral post-settle échouée"),
            }
        }
        let mut d = self.dash.write().await;
        d.windows.push(rec);
        if d.windows.len() > 50 {
            let excess = d.windows.len() - 50;
            d.windows.drain(..excess);
        }
    }

    /// Carnet le plus frais : WS si récent, sinon fallback REST (jamais WS seul).
    async fn best_book(&self, token: &str) -> Option<PolyBook> {
        let now_ms = Utc::now().timestamp_millis() as u64;
        if let Some(b) = pm_ws::fresh_book(&self.pm_state, token, now_ms, self.cfg.book_max_age_ms) {
            return Some(b);
        }
        match self.client.get_book(token).await {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::warn!(error = %e, "carnet REST indisponible");
                None
            }
        }
    }

    async fn block(&self, reason: &str) {
        let mut d = self.dash.write().await;
        d.last_block_reason = reason.to_string();
    }

    async fn publish_dash(&self, now_ms: u64) {
        let gs = self.guard.state();
        let mut d = self.dash.write().await;
        d.binance_connected = gs.is_fresh(now_ms, self.cfg.guard_max_age_ms);
        d.spot = gs.spot;
        d.drift = gs.drift;
        d.sigma = gs.sigma;
        d.obi = gs.obi;
        d.velocity = gs.velocity;
        d.kill = gs.kill;
        d.guard_age_ms = now_ms.saturating_sub(gs.ts_ms) as i64;
        match &self.market {
            Some(m) => {
                d.market_slug = m.slug.clone();
                d.remaining_s = m.time_remaining_sec();
            }
            None => {
                d.market_slug.clear();
                d.remaining_s = 0;
            }
        }
        d.strike = self.strike.unwrap_or(0.0);
        d.mode = self.cfg.mode.clone();
        #[cfg(feature = "live")]
        if let Some(l) = &self.live {
            d.mode = if l.armed { "live".into() } else { "live (dry-run)".into() };
        }
        // Meilleurs asks affichés depuis le cache WS (pas de REST dans le publish).
        if let (Some(m), Ok(g)) = (&self.market, self.pm_state.read()) {
            d.up_ask = g.books.get(&m.up_token_id).and_then(|b| b.best_ask()).unwrap_or(0.0);
            d.down_ask = g.books.get(&m.down_token_id).and_then(|b| b.best_ask()).unwrap_or(0.0);
        }
        apply_state(&mut d, &self.st);
        match &self.phase {
            Phase::Scanning => {
                d.phase = if self.market.is_some() { "scanning".into() } else { "waiting_market".into() };
                d.pos_side.clear();
                d.pos_shares = 0.0;
                d.pos_cost = 0.0;
                d.pos_entry_price = 0.0;
                d.pos_z = 0.0;
            }
            Phase::InPosition(p) => {
                d.phase = "in_position".into();
                d.pos_side = p.side_str().into();
                d.pos_shares = p.shares;
                d.pos_cost = p.cost;
                d.pos_entry_price = p.entry_price;
                let remaining_s =
                    ((p.end_ms - now_ms as i64) as f64 / 1000.0).max(0.0);
                d.pos_z = gs.margin_z(p.strike, p.dir(), remaining_s);
            }
        }
    }
}

fn apply_state(d: &mut crate::dashboard::DashState, st: &GrinderState) {
    d.stack = st.stack;
    d.streak = st.streak;
    d.run_id = st.run_id;
    d.best_streak = st.best_streak;
    d.best_stack = st.best_stack;
    d.windows_played = st.windows_played;
    d.wins = st.wins;
    d.losses = st.losses;
    d.panic_exits = st.panic_exits;
    d.realized_pnl = st.realized_pnl;
}
