//! Configuration du Grinder — tout vient de l'environnement (`.env`),
//! avec des défauts sûrs alignés sur les leçons du monolith.

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    // --- Stratégie Grinder ---
    /// Mise de départ ($) et valeur de reset après un wipe. Exponentiel pur :
    /// tout le stack est remis en jeu à chaque fenêtre.
    pub grind_base: f64,
    /// Prix d'entrée minimal du favori (la définition de la stratégie : ≥ 95c).
    pub entry_min: f64,
    /// Prix d'entrée maximal — au-delà, le gain restant ne paie plus le risque.
    pub entry_max: f64,
    /// Marge z minimale (spot vs strike, normalisée vol×√t) exigée POUR ENTRER.
    pub z_entry: f64,
    /// Marge z sous laquelle on VEND EN CATASTROPHE une position ouverte.
    pub z_exit: f64,
    /// Drift log/s à CONTRE-SENS au-delà duquel on sort (échelle 1e-5, cf. doctrine).
    pub drift_exit: f64,
    /// Plancher de marge ABSOLUE en $ : |spot−strike| < seuil → panique,
    /// quel que soit le z. 0 = désactivé (défaut — activé en paper seulement
    /// pour l'instant, calibration nuit du 15 juil.).
    pub dist_exit_usd: f64,
    /// Flux Binance périmé > N s EN POSITION → sortie de sécurité (garde
    /// aveugle). 0 = désactivé (défaut).
    pub guard_stale_exit_s: i64,
    /// Point de non-retour : si le meilleur bid de notre côté est SOUS ce prix,
    /// on ne vend plus — la position court jusqu'à résolution (vendre à 10c ne
    /// protège plus rien, autant espérer le retournement — consigne 19 juil.).
    pub panic_floor: f64,
    /// On n'entre plus s'il reste moins que ça (secondes) — pas le temps de sortir.
    pub min_remaining_s: i64,
    /// On n'entre pas s'il reste plus que ça (le 95c trop tôt est suspect).
    pub max_remaining_s: i64,

    // --- Simulation paper ---
    /// Fraction de la profondeur du carnet supposée ENCORE LÀ pendant un crash
    /// (0.5 = la moitié des bids affichés a disparu quand notre vente arrive).
    pub panic_haircut: f64,
    /// Taux de frais taker Polymarket : fee = rate × p(1−p) × parts.
    pub taker_fee_rate: f64,
    /// Âge maximal du carnet WS avant fallback REST (ms).
    pub book_max_age_ms: u64,

    // --- Garde Tokyo (locale) ---
    pub drift_halflife_s: f64,
    pub vol_window_ms: u64,
    pub vol_floor: f64,
    pub obi_depth_levels: usize,
    pub obi_threshold: f64,
    pub velocity_threshold: f64,
    /// Durée pendant laquelle un KILL radar reste actif (anti-rebond).
    pub kill_latch_ms: u64,
    /// Âge maximal du flux Binance : au-delà, garde aveugle → sortie de sécurité.
    pub guard_max_age_ms: u64,

    // --- Mode d'exécution ---
    /// "paper" (défaut) ou "live" (exige le binaire compilé --features live).
    pub mode: String,
    /// Coupe-circuit : un wipe total à résolution (ou récupération < 1 $) gèle
    /// définitivement les entrées jusqu'à intervention humaine (état persistant).
    pub halt_on_wipe: bool,
    /// Cap Kelly : le stack ne dépasse jamais cette fraction du wallet —
    /// collatéral réel en live, wallet virtuel (PAPER_WALLET0 + PnL réalisé)
    /// en paper. L'excédent est écrémé dans `banked`. 0 = désactivé (défaut).
    pub stack_cap_fraction: f64,
    /// Wallet virtuel de départ du paper (base du cap Kelly en simulation).
    pub paper_wallet0: f64,
    /// Écrémage par gain (Adrien, 19 juil.) : à chaque WIN, cette fraction du
    /// GAIN part en réserve, le reste compose. CUMULABLE avec le plafond.
    pub stack_skim_gain: f64,
    /// Disjoncteur (20 juil., resserré) : N resets de run dans la fenêtre
    /// RESET_WINDOW_S → gel (réarmement humain). 0 = désactivé. Live uniquement.
    pub max_resets: u32,
    /// Fenêtre du disjoncteur en secondes (défaut 3600 = 1 h).
    pub reset_window_s: i64,
    /// Adresse UDP d'écoute du flux radar Tokyo (WireTick). None = garde locale.
    pub signal_listen: Option<String>,
    /// Âge max du tick distant avant repli sur la garde locale (ms).
    pub remote_max_age_ms: u64,

    // --- Actif ---
    /// Préfixe de slug Polymarket ("btc" → btc-updown-5m-<ts>, "eth", …).
    pub asset: String,
    /// Symbole Binance pour le strike/la clôture (BTCUSDT, ETHUSDT, …).
    pub binance_symbol: String,

    // --- Infra ---
    pub binance_ws_url: String,
    pub dashboard_bind: String,
    pub dashboard_port: u16,
    pub state_path: String,
    pub windows_path: String,
}

fn f(key: &str, default: f64) -> f64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn i(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn s(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    pub fn from_env() -> Self {
        let mode = s("TRADING_MODE", "paper").to_lowercase();
        let live = mode == "live";
        let asset = s("ASSET", "btc").to_lowercase();
        let binance_symbol = s("BINANCE_SYMBOL", &format!("{}USDT", asset.to_uppercase()));
        let default_ws = format!(
            "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
            binance_symbol.to_lowercase()
        );
        Self {
            grind_base: f("GRIND_BASE", 1.0),
            entry_min: f("ENTRY_MIN", 0.95),
            entry_max: f("ENTRY_MAX", 0.99),
            z_entry: f("Z_ENTRY", 1.5),
            z_exit: f("Z_EXIT", 0.5),
            drift_exit: f("DRIFT_EXIT", 3e-5),
            dist_exit_usd: f("DIST_EXIT_USD", 0.0),
            guard_stale_exit_s: i("GUARD_STALE_EXIT_S", 0),
            panic_floor: f("PANIC_FLOOR", 0.10),
            min_remaining_s: i("MIN_REMAINING_S", 15),
            max_remaining_s: i("MAX_REMAINING_S", 240),
            panic_haircut: f("PANIC_HAIRCUT", 0.5),
            taker_fee_rate: f("TAKER_FEE_RATE", 0.07),
            book_max_age_ms: i("BOOK_MAX_AGE_MS", 5_000) as u64,
            drift_halflife_s: f("DRIFT_HALFLIFE_S", 25.0),
            vol_window_ms: i("VOL_WINDOW_MS", 2_000) as u64,
            vol_floor: f("VOL_FLOOR", 0.30),
            obi_depth_levels: i("OBI_DEPTH_LEVELS", 10) as usize,
            obi_threshold: f("OBI_THRESHOLD", 0.8),
            // 45 $ / s : leçon du paper monolith (VELOCITY_THRESHOLD 45).
            velocity_threshold: f("VELOCITY_THRESHOLD", 45.0),
            kill_latch_ms: i("KILL_LATCH_MS", 5_000) as u64,
            guard_max_age_ms: i("GUARD_MAX_AGE_MS", 3_000) as u64,
            binance_ws_url: s("BINANCE_WS_URL", &default_ws),
            asset,
            binance_symbol,
            // 0.0.0.0 : dashboards consultés via Tailscale (doctrine infra).
            dashboard_bind: s("DASH_BIND", "0.0.0.0"),
            dashboard_port: i("DASH_PORT", 8095) as u16,
            // Doctrine split paper/live : jamais les mêmes fichiers d'état.
            state_path: s("STATE_PATH", if live { "data/grinder_state_live.json" } else { "data/grinder_state.json" }),
            windows_path: s("WINDOWS_PATH", if live { "data/grinder_windows_live.jsonl" } else { "data/grinder_windows.jsonl" }),
            mode,
            halt_on_wipe: env::var("HALT_ON_WIPE")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false),
            stack_cap_fraction: f("STACK_CAP_FRACTION", 0.0),
            paper_wallet0: f("PAPER_WALLET0", 32.0),
            stack_skim_gain: f("STACK_SKIM_GAIN", 0.0),
            max_resets: i("MAX_RESETS", 0) as u32,
            reset_window_s: i("RESET_WINDOW_S", 3_600),
            signal_listen: env::var("SIGNAL_LISTEN").ok().filter(|v| !v.trim().is_empty()),
            remote_max_age_ms: i("REMOTE_MAX_AGE_MS", 1_500) as u64,
        }
    }

    #[cfg(test)]
    pub fn default_for_tests() -> Self {
        Self {
            grind_base: 1.0,
            entry_min: 0.95,
            entry_max: 0.99,
            z_entry: 1.5,
            z_exit: 0.5,
            drift_exit: 3e-5,
            dist_exit_usd: 0.0,
            guard_stale_exit_s: 0,
            panic_floor: 0.10,
            min_remaining_s: 15,
            max_remaining_s: 240,
            panic_haircut: 0.5,
            taker_fee_rate: 0.07,
            book_max_age_ms: 5_000,
            drift_halflife_s: 25.0,
            vol_window_ms: 2_000,
            vol_floor: 0.30,
            obi_depth_levels: 10,
            obi_threshold: 0.8,
            velocity_threshold: 45.0,
            kill_latch_ms: 5_000,
            guard_max_age_ms: 3_000,
            binance_ws_url: String::new(),
            asset: "btc".into(),
            binance_symbol: "BTCUSDT".into(),
            dashboard_bind: "127.0.0.1".into(),
            dashboard_port: 0,
            state_path: "/tmp/grinder_state_test.json".into(),
            windows_path: "/tmp/grinder_windows_test.jsonl".into(),
            mode: "paper".into(),
            halt_on_wipe: false,
            stack_cap_fraction: 0.0,
            paper_wallet0: 32.0,
            stack_skim_gain: 0.0,
            max_resets: 0,
            reset_window_s: 3_600,
            signal_listen: None,
            remote_max_age_ms: 1_500,
        }
    }
}
