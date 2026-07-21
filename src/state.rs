//! Persistance de l'état du Grinder.
//!
//! Deux fichiers, JAMAIS supprimés ni tronqués (doctrine : un reset se fait en
//! relançant le process, pas en effaçant les données) :
//! - `grinder_state.json`   : état courant (stack, série, compteurs) — réécrit
//!   de façon atomique (tmp + rename) après chaque fenêtre ;
//! - `grinder_windows.jsonl` : grand livre append-only, une ligne par fenêtre jouée.

use std::fs;
use std::io::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// État persistant du compoundage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrinderState {
    /// Cash du run courant, intégralement remis en jeu à chaque fenêtre.
    pub stack: f64,
    /// Wins consécutifs du run courant.
    pub streak: u32,
    /// Identifiant de run — incrémenté à chaque wipe/reset à la base.
    pub run_id: u64,
    /// Meilleure série de wins jamais atteinte.
    pub best_streak: u32,
    /// Plus haut stack jamais atteint.
    pub best_stack: f64,
    // Compteurs vie entière.
    pub windows_played: u64,
    pub wins: u64,
    pub losses: u64,
    pub panic_exits: u64,
    /// PnL réalisé cumulé (toutes fenêtres, tous runs).
    pub realized_pnl: f64,
    /// Coupe-circuit wipe (HALT_ON_WIPE) : plus AUCUNE entrée tant qu'un humain
    /// n'a pas remis ce flag à false dans le fichier d'état. Survit aux restarts.
    #[serde(default)]
    pub halted: bool,
    /// Cumul écrémé par le cap Kelly (STACK_CAP_FRACTION) : profits retirés de
    /// la table de jeu, restés au wallet, intouchables par un wipe.
    #[serde(default)]
    pub banked: f64,
    /// Horodatages unix des resets de run (disjoncteur MAX_RESETS_12H).
    #[serde(default)]
    pub reset_ts: Vec<i64>,
}

impl GrinderState {
    pub fn fresh(base: f64) -> Self {
        Self {
            stack: base,
            streak: 0,
            run_id: 1,
            best_streak: 0,
            best_stack: base,
            windows_played: 0,
            wins: 0,
            losses: 0,
            panic_exits: 0,
            realized_pnl: 0.0,
            halted: false,
            banked: 0.0,
            reset_ts: Vec::new(),
        }
    }
}

/// Une fenêtre jouée — ligne du grand livre JSONL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowRecord {
    pub ts: String,        // horodatage RFC3339 de la clôture de la ligne
    pub window_ts: i64,    // début unix de la fenêtre 5 min
    pub slug: String,
    pub side: String,      // "up" | "down"
    pub entry_price: f64,  // prix moyen d'entrée
    pub shares: f64,
    pub cost: f64,         // notional + frais d'entrée
    pub fees: f64,         // frais totaux (entrée + sortie éventuelle)
    pub outcome: String,   // "win" | "loss" | "panic"
    pub reason: String,    // détail sortie ("resolution", "radar_kill", "z_floor", …)
    pub proceeds: f64,     // $ récupérés (payout ou vente catastrophe) + cash résiduel
    pub pnl: f64,          // proceeds − stack engagé
    pub stack_after: f64,
    pub streak_after: u32,
    pub run_id: u64,
    pub strike: f64,
    pub spot_at_exit: f64,
    pub z_at_entry: f64,
    pub z_at_exit: f64,
    pub drift_at_exit: f64,
    // Instrumentation distance au strike (étude régimes nuit/jour) — défauts 0
    // pour relire les lignes antérieures au 15 juil.
    #[serde(default)]
    pub spot_at_entry: f64,
    /// |spot − strike| en $ au moment de l'entrée.
    #[serde(default)]
    pub dist_at_entry: f64,
    #[serde(default)]
    pub sigma_at_entry: f64,
    #[serde(default)]
    pub remaining_s_at_entry: i64,
}

/// Charge l'état, ou en crée un neuf si le fichier n'existe pas.
pub fn load(path: &str, base: f64) -> GrinderState {
    match fs::read_to_string(path) {
        Ok(txt) => match serde_json::from_str(&txt) {
            Ok(st) => st,
            Err(e) => {
                // Fichier corrompu : on NE l'écrase pas aveuglément — on le copie
                // en .corrupt-<ts> puis on repart d'un état neuf.
                let bak = format!("{path}.corrupt-{}", chrono::Utc::now().timestamp());
                let _ = fs::copy(path, &bak);
                tracing::error!(error = %e, %bak, "état illisible, sauvegardé puis réinitialisé");
                GrinderState::fresh(base)
            }
        },
        Err(_) => GrinderState::fresh(base),
    }
}

/// Écriture atomique de l'état (tmp + rename).
pub fn save(path: &str, st: &GrinderState) -> anyhow::Result<()> {
    if let Some(dir) = Path::new(path).parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = format!("{path}.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(st)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Ajoute une ligne au grand livre (append-only).
pub fn append_window(path: &str, rec: &WindowRecord) -> anyhow::Result<()> {
    if let Some(dir) = Path::new(path).parent() {
        fs::create_dir_all(dir)?;
    }
    let mut f = fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{}", serde_json::to_string(rec)?)?;
    Ok(())
}

/// Agrégats par jour UTC (dashboard) — calculés depuis le grand livre complet.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DailyStat {
    pub date: String, // AAAA-MM-JJ (UTC)
    pub windows: u32,
    pub wins: u32,
    pub panics: u32,
    pub secures: u32,
    pub volume: f64, // Σ des $ engagés
    pub pnl: f64,    // Σ pnl réel des fenêtres réglées (frais inclus)
    pub fees: f64,   // Σ frais taker (réels si loggés, sinon estimés fee_rate×p(1−p)×parts)
    /// Volume pondéré taker-rebates : Σ coût × (1−prix d'entrée) × 2.3 (poids Crypto).
    pub wv: f64,
}

/// Stats des `n_days` derniers jours (du plus récent au plus ancien).
/// `fee_rate` sert à ESTIMER les frais des lignes où fees=0 (live n'enregistre
/// pas les frais réels) : formule Polymarket fee = rate × p(1−p) × parts.
pub fn daily_stats(path: &str, n_days: usize, fee_rate: f64) -> Vec<DailyStat> {
    let Ok(txt) = std::fs::read_to_string(path) else { return vec![] };
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, DailyStat> = BTreeMap::new();
    for l in txt.lines() {
        let Ok(r) = serde_json::from_str::<WindowRecord>(l) else { continue };
        let date = r.ts.chars().take(10).collect::<String>();
        let e = map.entry(date.clone()).or_insert(DailyStat {
            date, windows: 0, wins: 0, panics: 0, secures: 0, volume: 0.0, pnl: 0.0, fees: 0.0,
            wv: 0.0,
        });
        e.windows += 1;
        e.volume += r.cost;
        e.pnl += r.pnl;
        e.wv += r.cost * (1.0 - r.entry_price) * 2.3;
        e.fees += if r.fees > 0.0 {
            r.fees
        } else {
            fee_rate * r.entry_price * (1.0 - r.entry_price) * r.shares
        };
        match r.outcome.as_str() {
            "win" => e.wins += 1,
            "panic" => e.panics += 1,
            "secure" => e.secures += 1,
            _ => {}
        }
    }
    let mut v: Vec<DailyStat> = map.into_values().collect();
    v.reverse();
    v.truncate(n_days);
    v
}

/// Relit les `n` dernières fenêtres du grand livre (pour le dashboard au boot).
pub fn tail_windows(path: &str, n: usize) -> Vec<WindowRecord> {
    let Ok(txt) = fs::read_to_string(path) else {
        return vec![];
    };
    let mut v: Vec<WindowRecord> = txt
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    if v.len() > n {
        v.drain(..v.len() - n);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_roundtrip_and_ledger_append() {
        let dir = std::env::temp_dir().join(format!("grinder-test-{}", std::process::id()));
        let sp = dir.join("state.json").to_string_lossy().into_owned();
        let wp = dir.join("windows.jsonl").to_string_lossy().into_owned();

        let mut st = GrinderState::fresh(1.0);
        st.stack = 2.34;
        st.wins = 7;
        save(&sp, &st).unwrap();
        let loaded = load(&sp, 1.0);
        assert_eq!(loaded.wins, 7);
        assert!((loaded.stack - 2.34).abs() < 1e-9);

        let rec = WindowRecord {
            ts: "2026-07-15T00:00:00Z".into(),
            window_ts: 1_784_000_100,
            slug: "btc-updown-5m-1784000100".into(),
            side: "up".into(),
            entry_price: 0.96,
            shares: 1.04,
            cost: 1.0,
            fees: 0.003,
            outcome: "win".into(),
            reason: "resolution".into(),
            proceeds: 1.04,
            pnl: 0.04,
            stack_after: 1.04,
            streak_after: 1,
            run_id: 1,
            strike: 60000.0,
            spot_at_exit: 60120.0,
            z_at_entry: 2.1,
            z_at_exit: 3.0,
            drift_at_exit: 1.2e-5,
            spot_at_entry: 60050.0,
            dist_at_entry: 50.0,
            sigma_at_entry: 0.42,
            remaining_s_at_entry: 120,
        };
        append_window(&wp, &rec).unwrap();
        append_window(&wp, &rec).unwrap();
        assert_eq!(tail_windows(&wp, 10).len(), 2);
        assert_eq!(tail_windows(&wp, 1).len(), 1);

        let _ = std::fs::remove_dir_all(dir); // nettoyage du répertoire de TEST uniquement
    }
}
