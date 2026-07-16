//! Garde distante — consomme le flux UDP du radar Tokyo (monolith).
//!
//! Trames (protocole WireTick du monolith, types.rs) :
//!   0x54 'T' + 8×f64/u64 LE (73 o) : seq, ts_ms, spot, sigma, drift, ofi,
//!        obi, velocity [, impulse]  — 65 o accepté (ancien radar, impulse=0)
//!   0x4B 'K' : KILL (flash-crash détecté AU RADAR, à ~2 ms de Binance)
//!   0x48 'H' : heartbeat — ignoré
//!
//! Le Grinder préfère ce flux quand il est frais (< REMOTE_MAX_AGE_MS) et
//! retombe SANS COUTURE sur sa garde locale sinon : jamais aveugle, jamais
//! dépendant d'une seule source. `seq` strictement croissant : les datagrammes
//! réordonnés/dupliqués sont jetés (garde-fou UDP du monolith).

use std::sync::{Arc, RwLock};

use tokio::net::UdpSocket;
use tokio::sync::watch;

use crate::tokyo::GuardState;

#[derive(Debug, Clone, Copy, Default)]
pub struct RemoteState {
    pub last_seq: u64,
    pub ts_ms_local: u64, // heure de RÉCEPTION locale (fraîcheur)
    pub spot: f64,
    pub sigma: f64,
    pub drift: f64,
    pub obi: f64,
    pub velocity: f64,
    pub impulse: f64,
    pub kill_until_ms: u64,
}

pub type RemoteShared = Arc<RwLock<RemoteState>>;

impl RemoteState {
    /// Vue `GuardState` du tick distant (mêmes unités que la garde locale).
    pub fn as_guard_state(&self, now_ms: u64) -> GuardState {
        GuardState {
            ts_ms: self.ts_ms_local,
            spot: self.spot,
            drift: self.drift,
            sigma: self.sigma,
            obi: self.obi,
            velocity: self.velocity,
            kill: now_ms < self.kill_until_ms,
        }
    }
}

fn decode_wiretick(b: &[u8]) -> Option<[f64; 8]> {
    if b.len() < 65 || b[0] != 0x54 {
        return None;
    }
    let mut w = [0u64; 8];
    for (i, slot) in w.iter_mut().enumerate().take(8) {
        *slot = u64::from_le_bytes(b[1 + i * 8..9 + i * 8].try_into().ok()?);
    }
    let impulse = if b.len() >= 73 {
        f64::from_bits(u64::from_le_bytes(b[65..73].try_into().ok()?))
    } else {
        0.0
    };
    Some([
        w[0] as f64,           // seq
        w[1] as f64,           // ts_ms émission (info)
        f64::from_bits(w[2]),  // spot
        f64::from_bits(w[3]),  // sigma
        f64::from_bits(w[4]),  // drift
        f64::from_bits(w[6]),  // obi (w[5]=ofi, non utilisé par la garde)
        f64::from_bits(w[7]),  // velocity
        impulse,
    ])
}

/// Lance l'écoute UDP. Retourne l'état partagé + un watch qui pulse à chaque
/// tick reçu (le Grinder réévalue sa position dessus, comme sur un tick local).
pub fn spawn(listen: String, kill_latch_ms: u64) -> (RemoteShared, watch::Receiver<u64>) {
    let state: RemoteShared = Default::default();
    let (tx, rx) = watch::channel(0u64);
    let st = state.clone();
    tokio::spawn(async move {
        let sock = match UdpSocket::bind(&listen).await {
            Ok(s) => {
                tracing::info!(%listen, "garde distante : écoute UDP radar Tokyo");
                s
            }
            Err(e) => {
                tracing::error!(%listen, error = %e, "bind UDP impossible — garde locale seule");
                return;
            }
        };
        let mut buf = [0u8; 128];
        loop {
            let Ok((n, _peer)) = sock.recv_from(&mut buf).await else { continue };
            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            match buf.first() {
                Some(0x54) => {
                    if let Some(v) = decode_wiretick(&buf[..n]) {
                        let Ok(mut g) = st.write() else { continue };
                        let seq = v[0] as u64;
                        if seq <= g.last_seq && g.last_seq != 0 {
                            continue; // réordonné/dupliqué
                        }
                        g.last_seq = seq;
                        g.ts_ms_local = now_ms;
                        g.spot = v[2];
                        g.sigma = v[3];
                        g.drift = v[4];
                        g.obi = v[5];
                        g.velocity = v[6];
                        g.impulse = v[7];
                        drop(g);
                        let _ = tx.send(now_ms);
                    }
                }
                Some(0x4B) => {
                    // KILL radar : latch comme la garde locale.
                    if let Ok(mut g) = st.write() {
                        g.kill_until_ms = now_ms + kill_latch_ms;
                        g.ts_ms_local = now_ms;
                    }
                    tracing::warn!("KILL reçu du radar Tokyo");
                    let _ = tx.send(now_ms);
                }
                _ => {} // heartbeat 'H' ou bruit
            }
        }
    });
    (state, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(seq: u64, spot: f64, sigma: f64, drift: f64, obi: f64, vel: f64, imp: f64) -> [u8; 73] {
        let mut b = [0u8; 73];
        b[0] = 0x54;
        for (i, v) in [
            seq,
            1_784_000_000_000u64,
            spot.to_bits(),
            sigma.to_bits(),
            drift.to_bits(),
            0.5f64.to_bits(), // ofi
            obi.to_bits(),
            vel.to_bits(),
            imp.to_bits(),
        ]
        .iter()
        .enumerate()
        {
            b[1 + i * 8..9 + i * 8].copy_from_slice(&v.to_le_bytes());
        }
        b
    }

    #[test]
    fn wiretick_decode_full_and_short() {
        let b = encode(42, 64000.5, 0.8, -1.2e-5, -0.3, 12.0, 4.2e-4);
        let v = decode_wiretick(&b).expect("73 o");
        assert_eq!(v[0] as u64, 42);
        assert!((v[2] - 64000.5).abs() < 1e-9);
        assert!((v[4] + 1.2e-5).abs() < 1e-12);
        assert!((v[5] + 0.3).abs() < 1e-9, "obi");
        assert!((v[7] - 4.2e-4).abs() < 1e-12, "impulse");
        // trame courte 65 o (ancien radar) : impulse = 0
        let v = decode_wiretick(&b[..65]).expect("65 o");
        assert_eq!(v[7], 0.0);
        // invalides
        assert!(decode_wiretick(&b[..64]).is_none());
        let mut bad = b;
        bad[0] = 0x48;
        assert!(decode_wiretick(&bad).is_none());
    }

    #[test]
    fn remote_state_maps_to_guard_state() {
        let r = RemoteState {
            ts_ms_local: 1000,
            spot: 64000.0,
            sigma: 0.6,
            drift: 2e-5,
            obi: 0.1,
            velocity: 3.0,
            kill_until_ms: 2000,
            ..Default::default()
        };
        let gs = r.as_guard_state(1500);
        assert!(gs.kill, "kill latché");
        assert!(!r.as_guard_state(2500).kill, "latch expiré");
        assert_eq!(gs.spot, 64000.0);
        assert!(gs.is_fresh(1600, 1000));
    }
}
