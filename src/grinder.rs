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
use crate::remote_guard::{self, RemoteShared};
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
    /// Point de non-retour franchi : plus aucune vente, on va à la résolution.
    ride: bool,
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
    /// Levé par la tâche de réconciliation si le verdict officiel contredit
    /// notre résolution kline → halt au prochain housekeeping.
    reconcile_halt: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Flux radar Tokyo distant (None = garde locale seule).
    remote: Option<RemoteShared>,
    /// Enregistreur des ticks radar EN POSITION (dataset classifieur mèche/crash).
    tick_file: Option<std::fs::File>,
    /// 3 dernières évaluations du wallet total (collatéral lu + produit de la
    /// fenêtre close) : le cap Kelly prend le MAX — les redemptions mettent
    /// 30-60 s à créditer et une lecture instantanée sous-évalue le wallet
    /// (sur-écrémage du 19 juil. : lu 36 $, réel 46 $).
    wallet_evals: std::collections::VecDeque<f64>,
    /// Wallet lu en continu par le poller (max glissant 90 s), 0.0 tant que
    /// rien n'a été lu. Alimente le dashboard et le cap Kelly entre les settles.
    wallet_live: std::sync::Arc<std::sync::RwLock<f64>>,
    /// Dernière mesure du mur de liquidité (ms) — cadence ~2 s en position.
    last_wall_poll_ms: i64,
    /// Dernier mur mesuré : (ts_ms, usdc). Consommé par la sécurisation.
    last_wall: Option<(i64, f64)>,
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

    // Garde distante (radar Tokyo) : optionnelle, repli local automatique.
    let (remote, mut remote_rx, _keep_remote_tx);
    match &cfg.signal_listen {
        Some(addr) => {
            let (st, rx) = remote_guard::spawn(addr.clone(), cfg.kill_latch_ms);
            remote = Some(st);
            remote_rx = rx;
            _keep_remote_tx = None;
        }
        None => {
            let (tx, rx) = watch::channel(0u64);
            remote = None;
            remote_rx = rx;
            _keep_remote_tx = Some(tx); // garde le canal vivant (jamais de pulse)
        }
    }

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

    let wallet_live = std::sync::Arc::new(std::sync::RwLock::new(0.0_f64));
    #[cfg(feature = "live")]
    if let Some(l) = &live {
        // Poller wallet : lecture CLOB ~10 s, max glissant 90 s (redemptions en
        // vol), publication continue au dashboard (leçon monolith : sync ≤10 s).
        let creds = l.creds.clone();
        let shared = wallet_live.clone();
        let dash2 = dash.clone();
        tokio::spawn(async move {
            let mut window = std::collections::VecDeque::with_capacity(9);
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                match crate::live::auth::get_collateral_balance(&creds).await {
                    Ok(c) if c > 0.0 => {
                        window.push_back(c);
                        if window.len() > 9 {
                            window.pop_front();
                        }
                        let m = window.iter().cloned().fold(0.0_f64, f64::max);
                        if let Ok(mut w) = shared.write() {
                            *w = m;
                        }
                        dash2.write().await.live_collateral = m;
                    }
                    Ok(_) => {}
                    Err(e) => tracing::debug!(error = %e, "poller wallet : lecture échouée"),
                }
            }
        });
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
        reconcile_halt: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        remote,
        tick_file: None,
        wallet_evals: std::collections::VecDeque::with_capacity(3),
        wallet_live,
        last_wall_poll_ms: 0,
        last_wall: None,
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
                    g.guard.update(&u); // entretient la garde locale (repli)
                    // Chemin chaud : réévaluation à CHAQUE tick (source fusionnée).
                    let gs = g.guard_now();
                    g.check_exits(gs).await;
                }
            }
            changed = remote_rx.changed() => {
                // Tick radar Tokyo (10 Hz) : même chemin chaud que Binance local.
                if changed.is_ok() {
                    g.record_tick();
                    let gs = g.guard_now();
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
    /// Enregistre le tick radar courant si une position est ouverte — dataset
    /// du futur classifieur mèche vs crash (OFI/impulse au moment critique).
    /// Volume : ~10 Hz × temps en position ≈ 20-30 Mo/jour, en position SEULEMENT.
    fn record_tick(&mut self) {
        let Phase::InPosition(pos) = &self.phase else { return };
        let Some(r) = &self.remote else { return };
        let Ok(t) = r.read() else { return };
        if self.tick_file.is_none() {
            self.tick_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("data/radar_ticks.jsonl")
                .map_err(|e| tracing::warn!(error = %e, "ouverture radar_ticks impossible"))
                .ok();
        }
        if let Some(f) = &mut self.tick_file {
            use std::io::Write as _;
            let line = format!(
                "{{\"t\":{},\"w\":{},\"side\":\"{}\",\"spot\":{:.2},\"sig\":{:.4},\"dr\":{:.3e},\"ofi\":{:.4},\"obi\":{:.4},\"vel\":{:.2},\"imp\":{:.3e},\"kill\":{}}}\n",
                t.ts_ms_local, pos.window_ts, pos.side_str(), t.spot, t.sigma, t.drift,
                t.ofi, t.obi, t.velocity, t.impulse, t.ts_ms_local < t.kill_until_ms
            );
            let _ = f.write_all(line.as_bytes());
        }
    }

    /// Estimation du wallet total : poller live, ou wallet virtuel en paper.
    fn wallet_estimate(&self) -> f64 {
        let polled = self.wallet_live.read().map(|w| *w).unwrap_or(0.0);
        if polled > 0.0 {
            polled
        } else if self.cfg.mode != "live" {
            (self.cfg.paper_wallet0 + self.st.realized_pnl).max(0.0)
        } else {
            0.0
        }
    }

    /// Mesure du mur de liquidité pendant une position (toutes les ~2 s) :
    /// combien d'USDC d'ordres agressifs il faudrait pour pousser le spot
    /// jusqu'au strike. Dataset de calibration anti-aiguille (20 juil.).
    async fn record_wall(&mut self, now_ms: i64) {
        if now_ms - self.last_wall_poll_ms < 2_000 {
            return;
        }
        let Phase::InPosition(pos) = &self.phase else { return };
        self.last_wall_poll_ms = now_ms;
        let gs = self.guard_now();
        let (low, high, use_asks) = if pos.side_up {
            (pos.strike, gs.spot, false) // Up : le danger = vendre les BIDS de spot→strike
        } else {
            (gs.spot, pos.strike, true) // Down : le danger = consommer les ASKS de spot→strike
        };
        let (w, ts, side_up, spot, strike) =
            (pos.window_ts, now_ms, pos.side_up, gs.spot, pos.strike);
        match crate::connectors::binance::depth_wall(&self.cfg.binance_symbol, low.min(high), low.max(high), use_asks).await {
            Ok((usdc, qty, span, lvls)) => {
                self.last_wall = Some((now_ms, usdc));
                let density = usdc / (spot - strike).abs().max(0.01);
                use std::io::Write as _;
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("data/wall_ticks.jsonl")
                {
                    let _ = writeln!(
                        f,
                        "{{\"t\":{ts},\"w\":{w},\"side_up\":{side_up},\"spot\":{spot:.2},\"strike\":{strike:.2},\"wall_usdc\":{usdc:.0},\"wall_qty\":{qty:.4},\"span\":{span:.1},\"levels\":{lvls},\"density\":{density:.0}}}"
                    );
                }
            }
            Err(e) => tracing::debug!(error = %e, "mesure du mur échouée"),
        }
    }

    /// État de garde courant : radar Tokyo si frais, sinon garde locale.
    /// HYBRIDE (16 juil.) : le radar fournit spot/drift/KILL/vélocité (fraîcheur
    /// ~35 ms), mais le SIGMA reste celui de l'estimateur LOCAL — c'est sur lui
    /// que z_entry/z_exit sont calibrés (le radar monolith tourne avec un
    /// plancher 0,80 : l'utiliser divisait nos z par ~2,7 → live frileux à
    /// l'entrée et paniquard en position). KILL local en OR.
    fn guard_now(&self) -> GuardState {
        let now_ms = Utc::now().timestamp_millis() as u64;
        let local = self.guard.state();
        if let Some(r) = &self.remote {
            if let Ok(g) = r.read() {
                let gs = g.as_guard_state(now_ms);
                if gs.is_fresh(now_ms, self.cfg.remote_max_age_ms) {
                    let mut gs = gs;
                    gs.sigma = local.sigma; // calibration z = estimateur local
                    gs.kill = gs.kill || local.kill;
                    return gs;
                }
            }
        }
        local
    }

    /// Boucle lente (2 Hz) : rollover marché, strike, résolution, entrée, dashboard.
    async fn housekeeping(&mut self) {
        let now_ms = Utc::now().timestamp_millis();

        // 1. Résolution d'une position arrivée à échéance.
        // Le verdict officiel Gamma met PLUSIEURS MINUTES à se publier (mesuré
        // 16 juil.) : trop lent pour le compoundage. On résout donc sur la
        // CLÔTURE de la bougie Binance 5m (même source que le strike, finale
        // quelques secondes après l'échéance), puis une réconciliation
        // asynchrone compare au verdict officiel et déclenche un HALT en cas
        // de divergence (plutôt que de composer sur une base fausse).
        if let Phase::InPosition(pos) = &self.phase {
            if now_ms >= pos.end_ms + 3_000 {
                let pos = pos.clone();
                match crate::connectors::binance::price_at_window_close(&self.cfg.binance_symbol, pos.window_ts).await {
                    Ok(Some(close)) => {
                        let up_won = close > pos.strike;
                        self.spawn_reconcile(&pos, up_won);
                        self.resolve(&pos, up_won, "resolution_kline").await;
                        self.phase = Phase::Scanning;
                    }
                    _ if now_ms >= pos.end_ms + 90_000 => {
                        let up_won = self.guard_now().spot > pos.strike;
                        tracing::warn!("bougie Binance indisponible après 90 s — fallback micro-price");
                        self.spawn_reconcile(&pos, up_won);
                        self.resolve(&pos, up_won, "resolution_proxy").await;
                        self.phase = Phase::Scanning;
                    }
                    _ => {} // bougie pas encore close : on réessaie au prochain tick
                }
            }
        }

        // 1ter. Une réconciliation a détecté une divergence avec le verdict
        // officiel : on gèle tout, la comptabilité doit être reprise à la main.
        if self.reconcile_halt.swap(false, std::sync::atomic::Ordering::SeqCst) {
            if self.cfg.mode == "live" {
                self.st.halted = true;
                let _ = state::save(&self.cfg.state_path, &self.st);
                tracing::error!("🛑 HALT : divergence résolution kline vs verdict officiel Polymarket");
            } else {
                // Paper : on ne gèle JAMAIS (consigne) — on trace, la compta
                // simulée peut diverger, c'est acceptable pour un instrument de mesure.
                tracing::error!("⚠️ divergence résolution kline vs officiel (paper — pas de gel)");
            }
        }

        // 1bis. Garde aveugle : flux Binance mort EN POSITION → sortie de
        // sécurité (check_exits ne tourne que sur tick, donc jamais si le flux
        // est mort — ce chemin-ci est cadencé par le housekeeping 2 Hz).
        if self.cfg.guard_stale_exit_s > 0 {
            if let Phase::InPosition(pos) = &self.phase {
                let gs = self.guard_now();
                let max_age_ms = (self.cfg.guard_stale_exit_s * 1000) as u64;
                if now_ms < pos.end_ms && !pos.ride && !gs.is_fresh(now_ms as u64, max_age_ms) {
                    let pos = pos.clone();
                    tracing::warn!("garde AVEUGLE (flux Binance périmé) — sortie de sécurité");
                    let remaining_s = ((pos.end_ms - now_ms) as f64 / 1000.0).max(0.0);
                    let z = gs.margin_z(pos.strike, pos.dir(), remaining_s);
                    if self.panic_exit(&pos, "guard_stale", z, gs).await {
                        self.phase = Phase::Scanning;
                    }
                }
            }
        }

        // 1quater. Mesure du mur de liquidité (dataset anti-aiguille).
        self.record_wall(now_ms).await;

        // 1quinquies. SÉCURISATION DE FIN DE FENÊTRE (anti-aiguille, 20 juil.) :
        // dans les dernières secondes, si le mur vers le strike est tombé sous
        // le seuil (traversée bon marché pour un sniper) et que le bid permet
        // de sortir presque plein, on encaisse au lieu de porter jusqu'à la
        // cloche. Milieu de fenêtre : rien ne change.
        if self.cfg.secure_wall_usdc > 0.0 {
            if let Phase::InPosition(pos) = &self.phase {
                let remaining = (pos.end_ms - now_ms) / 1000;
                let wall_fresh_low = self
                    .last_wall
                    .map(|(t, u)| now_ms - t < 6_000 && u < self.cfg.secure_wall_usdc)
                    .unwrap_or(false);
                if !pos.ride && remaining > 3 && remaining <= self.cfg.secure_last_s && wall_fresh_low {
                    let pos = pos.clone();
                    if let Some(book) = self.best_book(&pos.token_id).await {
                        if let Some(bid) = book.best_bid() {
                            if bid >= self.cfg.secure_min_price {
                                tracing::warn!(
                                    bid,
                                    wall = self.last_wall.map(|(_, u)| u).unwrap_or(0.0),
                                    remaining,
                                    "🛡️ SÉCURISATION fin de fenêtre : mur fragile, on encaisse"
                                );
                                let fill = self.exec_sell(&pos.token_id, pos.shares).await;
                                if fill.shares > 0.0 {
                                    let proceeds =
                                        (fill.notional - fill.fees).max(0.0) + pos.cash_left;
                                    let gs = self.guard_now();
                                    let z = gs.margin_z(pos.strike, pos.dir(), remaining as f64);
                                    self.settle(
                                        &pos, "secure", "secure_wall", proceeds, fill.fees, z, gs,
                                    )
                                    .await;
                                    self.phase = Phase::Scanning;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Rollover / découverte du marché courant.
        let need_market = match &self.market {
            None => true,
            Some(m) => m.time_remaining_sec() <= 0,
        };
        if need_market {
            match self.client.get_current_5m_market(&self.cfg.asset).await {
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
                    match price_at_window_open(&self.cfg.binance_symbol, m.window_ts).await {
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
        if self.st.halted {
            self.block("HALTED après wipe — relance manuelle requise (state.halted)").await;
            return;
        }
        let gs = self.guard_now();
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
                ride: false,
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

        // Grâce post-entrée : les parts ne sont pas vendables on-chain avant
        // ~2-3 s, et les mèches d'une seconde ne sont pas des crashs.
        if now_ms - (pos.end_ms - pos.remaining_s_at_entry * 1000) < 3_000 {
            return;
        }
        let dist_usd = (gs.spot - pos.strike).abs();
        let reason = if gs.kill {
            Some("radar_kill")
        } else if self.cfg.dist_exit_usd > 0.0 && dist_usd < self.cfg.dist_exit_usd && z < 0.5 {
            // Plancher absolu : à quelques $ du strike ET z dégradé — les deux
            // ensemble, sinon un simple frôlement de la bande déclenche à tort
            // (fausse panique live du 15 juil. 17:44 : dist 14 $ mais z 0,69,
            // position jamais menacée). Le vrai crash (z −0,11, dist 10 $)
            // déclenche toujours.
            Some("dist_floor")
        } else if z < self.cfg.z_exit {
            Some("z_floor")
        } else if gs.drift * pos.dir() < -self.cfg.drift_exit {
            Some("drift_against")
        } else {
            None
        };
        let Some(reason) = reason else { return };
        if pos.ride {
            return; // point de non-retour déjà franchi : on tient jusqu'au bout
        }

        let pos = pos.clone();
        // Point de non-retour : sous PANIC_FLOOR, vendre ne récupère que des
        // miettes — on préfère le wipe assumé avec chance de retournement.
        if let Some(book) = self.best_book(&pos.token_id).await {
            if book.best_bid().is_some_and(|b| b < self.cfg.panic_floor) {
                tracing::warn!(
                    reason, bid = book.best_bid(),
                    "sous le point de non-retour ({}) — position tenue jusqu'à résolution",
                    self.cfg.panic_floor
                );
                if let Phase::InPosition(p) = &mut self.phase {
                    p.ride = true;
                }
                return;
            }
        }
        tracing::warn!(reason, z, drift = gs.drift, "VENTE CATASTROPHE");
        if self.panic_exit(&pos, reason, z, gs).await {
            self.phase = Phase::Scanning;
        }
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
    /// Retourne `false` si RIEN n'a pu être vendu (parts pas encore on-chain,
    /// carnet vide, ordre refusé) : la position reste OUVERTE — inscrire un wipe
    /// serait fictif (leçon du 18 juil. 22:49 : halt fantôme, la fenêtre a gagné).
    async fn panic_exit(&mut self, pos: &Position, reason: &str, z: f64, gs: GuardState) -> bool {
        let fill = self.exec_sell(&pos.token_id, pos.shares).await;
        if fill.shares <= 0.0 && pos.shares > 0.0 {
            tracing::warn!(reason, "vente catastrophe SANS AUCUN fill — position conservée, retry au prochain tick");
            return false;
        }
        let proceeds = (fill.notional - fill.fees).max(0.0) + pos.cash_left;
        let recovered_pct = if pos.cost > 0.0 { 100.0 * proceeds / pos.cost } else { 0.0 };
        tracing::warn!(
            sold = fill.shares, of = pos.shares, avg = fill.avg_price,
            proceeds, recovered_pct, "sortie catastrophe exécutée"
        );
        self.st.panic_exits += 1;
        self.settle(pos, "panic", reason, proceeds, fill.fees, z, gs).await;
        true
    }

    /// Vérifie EN ARRIÈRE-PLAN que le verdict officiel Polymarket (publié avec
    /// plusieurs minutes de retard) confirme notre résolution kline. Divergence
    /// → flag partagé → halt (on ne compose pas sur une base fausse).
    fn spawn_reconcile(&self, pos: &Position, booked_up_won: bool) {
        let client = self.client.clone();
        let slug = pos.slug.clone();
        let flag = self.reconcile_halt.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(Duration::from_secs(15)).await;
                match client.get_official_up_won(&slug).await {
                    Ok(Some(official_up)) => {
                        if official_up != booked_up_won {
                            tracing::error!(
                                %slug, booked_up_won, official_up,
                                "⚠️ verdict officiel ≠ résolution comptabilisée — HALT demandé"
                            );
                            flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        return;
                    }
                    _ => continue,
                }
            }
            tracing::warn!(%slug, "réconciliation : verdict officiel non publié en 10 min");
        });
    }

    /// Clôture à l'échéance. `up_won` vient du verdict officiel Gamma (ou du
    /// proxy Binance en fallback — `reason` distingue les deux dans le ledger).
    async fn resolve(&mut self, pos: &Position, up_won: bool, reason: &str) {
        let gs = self.guard_now();
        let won = up_won == pos.side_up;
        let proceeds = if won { pos.shares + pos.cash_left } else { pos.cash_left };
        if won {
            self.st.wins += 1;
        } else {
            self.st.losses += 1;
        }
        let remaining_s = 0.0;
        let z = gs.margin_z(pos.strike, pos.dir(), remaining_s);
        tracing::info!(
            won, up_won, reason, spot = gs.spot, strike = pos.strike, proceeds,
            "résolution fenêtre {}", pos.slug
        );
        self.settle(pos, if won { "win" } else { "loss" }, reason, proceeds, 0.0, z, gs)
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

        if outcome == "win" || outcome == "secure" {
            if outcome == "win" {
                self.st.streak += 1;
                self.st.best_streak = self.st.best_streak.max(self.st.streak);
            }
            self.st.stack = proceeds;
        } else if proceeds >= self.cfg.grind_base {
            // Sortie catastrophe qui sauve plus que la base : on continue le run
            // avec ce qui a été récupéré (la série de wins, elle, est cassée).
            self.st.streak = 0;
            self.st.stack = proceeds;
        } else {
            // Wipe : retour à la mise de base DYNAMIQUE (min(base, 15% wallet) —
            // on ne réarme pas 10 $ fixes dans un wallet qui a saigné), nouveau run.
            self.st.streak = 0;
            self.st.run_id += 1;
            let wallet_est = self.wallet_estimate();
            let base = if wallet_est > 0.0 {
                self.cfg.grind_base.min(0.15 * wallet_est).max(2.0)
            } else {
                self.cfg.grind_base
            };
            self.st.stack = base;
            // Disjoncteur : trop de resets dans la fenêtre → gel (live uniquement).
            let now_s = Utc::now().timestamp();
            self.st.reset_ts.push(now_s);
            self.st.reset_ts.retain(|t| now_s - t < self.cfg.reset_window_s);
            if self.cfg.max_resets > 0
                && self.cfg.mode == "live"
                && self.st.reset_ts.len() as u32 >= self.cfg.max_resets
            {
                self.st.halted = true;
                tracing::error!(
                    resets = self.st.reset_ts.len(),
                    window_s = self.cfg.reset_window_s,
                    "🛑 DISJONCTEUR : {} resets dans la fenêtre — gel, réarmement humain requis",
                    self.st.reset_ts.len()
                );
            }
            // Coupe-circuit (consigne du 15 juil.) : perte totale à résolution
            // ou récupération < 1 $ → plus AUCUNE entrée, intervention humaine.
            if self.cfg.halt_on_wipe && (outcome == "loss" || proceeds < 1.0) {
                self.st.halted = true;
                tracing::error!(
                    outcome, proceeds,
                    "🛑 WIPE — coupe-circuit activé : plus aucune entrée (state.halted=true)"
                );
            }
        }
        self.st.best_stack = self.st.best_stack.max(self.st.stack);

        // Écrémage par gain (spécification Adrien) : 30 % de chaque gain de
        // WIN part en réserve, 70 % compose. Prioritaire sur le cap dynamique.
        if self.cfg.stack_skim_gain > 0.0 && outcome == "win" && pnl > 0.0 {
            let skim = self.cfg.stack_skim_gain * pnl;
            self.st.banked += skim;
            self.st.stack -= skim;
            tracing::info!(skim, gain = pnl, banked = self.st.banked, "écrémage 30% du gain");
        }

        // Cap Kelly PAPER : wallet virtuel = PAPER_WALLET0 + PnL réalisé.
        // Même mécanique que le live, base simulée.
        if self.cfg.stack_cap_fraction > 0.0 && self.cfg.mode != "live" {
            let wallet = self.cfg.paper_wallet0 + self.st.realized_pnl;
            let cap = self.cfg.stack_cap_fraction * wallet;
            if wallet > 0.0 && self.st.stack > cap {
                let skim = self.st.stack - cap;
                self.st.banked += skim;
                self.st.stack = cap;
                tracing::info!(
                    skim, cap, banked = self.st.banked, wallet,
                    "cap Kelly (paper) : excédent écrémé"
                );
            }
        }

        // Cap Kelly (live) : le stack ne dépasse jamais STACK_CAP_FRACTION du
        // collatéral wallet réel ; l'excédent est écrémé (il reste au wallet,
        // simplement retiré de la table de jeu — compté dans `banked`).
        #[cfg(feature = "live")]
        if self.cfg.stack_cap_fraction > 0.0 {
            if let Some(l) = &self.live {
                match l.collateral().await {
                    Ok(c) if c > 0.0 => {
                        // Wallet total = max glissant des 3 dernières évaluations
                        // (collatéral lu + produit de la fenêtre close) : les
                        // redemptions en vol font sous-évaluer toute lecture
                        // instantanée (19 juil. : lu 36 $, réel 46 $).
                        self.wallet_evals.push_back(c + proceeds);
                        if self.wallet_evals.len() > 3 {
                            self.wallet_evals.pop_front();
                        }
                        let polled = self.wallet_live.read().map(|w| *w).unwrap_or(0.0);
                        let wallet = self.wallet_evals.iter().cloned().fold(polled, f64::max);
                        self.dash.write().await.live_collateral = wallet;
                        let cap = self.cfg.stack_cap_fraction * wallet;
                        if self.st.stack > cap {
                            let skim = self.st.stack - cap;
                            self.st.banked += skim;
                            self.st.stack = cap;
                            tracing::info!(
                                skim, cap, banked = self.st.banked, collateral = c,
                                "cap Kelly : excédent écrémé vers le wallet"
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "cap Kelly : collatéral illisible, pas d'écrémage ce tour"),
                }
            }
        }

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
        let gs = self.guard_now();
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
    d.banked = st.banked;
}
