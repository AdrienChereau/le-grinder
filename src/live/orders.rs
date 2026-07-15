//! Ordres LIVE via SDK `polymarket_client_sdk_v2` (POLY_1271 / sig_type 3) —
//! porté du monolith, RÉDUIT au chemin taker du Grinder : FAK uniquement,
//! aucun ordre restant (donc ni cancel, ni heartbeat dead-man requis).
//!
//! Chaque POST vérifie `success` ET `error_msg` (le CLOB peut répondre 200
//! avec success=false — leçon mémoire).

use std::str::FromStr as _;

use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer};
use polymarket_client_sdk_v2::clob::types::{OrderSignature, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;

use super::auth::{LiveCredentials, CLOB_BASE};

/// Paramètres d'un ordre FAK (le token_id sélectionne Up/Down côté appelant).
#[derive(Debug, Clone, Copy)]
pub struct FakArgs {
    pub price: f64,    // prix limite ∈ (0,1) — cap d'achat ou plancher de vente
    pub size: f64,     // parts (arrondi 2 décimales — LOT_SIZE_SCALE)
    pub is_sell: bool, // false = BUY
}

#[derive(Debug, PartialEq)]
pub enum PlaceResult {
    /// Signé + loggé, rien envoyé (LIVE_ARMED=false — répétition générale).
    DryRun,
    Placed {
        order_id: String,
        filled_size: Option<f64>, // rempli immédiatement (FAK)
        avg_price: Option<f64>,
        post_ms: u64,
    },
}

// ─── Caches (init au boot par `startup`) ───
static CACHED_LOCAL_SIGNER: std::sync::OnceLock<
    LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
> = std::sync::OnceLock::new();
static CACHED_AUTH_CLIENT: std::sync::OnceLock<tokio::sync::Mutex<Client<Authenticated<Normal>>>> =
    std::sync::OnceLock::new();

/// Démarrage live : parse le signer, authentifie le client SDK (une fois),
/// sync le cache balance-allowance (obligatoire en sig_type 3).
pub async fn startup(creds: &LiveCredentials) -> anyhow::Result<()> {
    creds.log_config_check();
    if creds.sig_type != 3 {
        anyhow::bail!("seul POLY_SIG_TYPE=3 (deposit wallet POLY_1271) est supporté");
    }
    if CACHED_LOCAL_SIGNER.get().is_none() {
        let s = LocalSigner::from_str(&creds.private_key)
            .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
            .with_chain_id(Some(POLYGON));
        let _ = CACHED_LOCAL_SIGNER.set(s);
    }
    if CACHED_AUTH_CLIENT.get().is_none() {
        let signer = local_signer(creds)?;
        let client = authenticated_client(creds, &signer).await?;
        let _ = CACHED_AUTH_CLIENT.set(tokio::sync::Mutex::new(client));
        tracing::info!("client POLY_1271 authentifié et mis en cache");
    }
    super::auth::sync_balance_allowance(creds, "COLLATERAL", None)
        .await
        .map_err(|e| anyhow::anyhow!("sync balance-allowance (deposit wallet): {e}"))?;
    Ok(())
}

/// Place un ordre FAK. `live_armed=false` → signé, loggé, PAS posté.
pub async fn place_fak(
    live_armed: bool,
    creds: &LiveCredentials,
    token_id: &str,
    args: FakArgs,
) -> anyhow::Result<PlaceResult> {
    let signer = local_signer(creds)?;
    let token = U256::from_str(token_id).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;
    // btc-updown-5m : tick 0.01 → 2 décimales prix ; lot 2 décimales (SDK).
    let price = decimal_from_f64(args.price, 2, "price")?;
    let size = decimal_from_f64(args.size, 2, "size")?;
    let side = if args.is_sell { Side::Sell } else { Side::Buy };

    let lock = CACHED_AUTH_CLIENT
        .get()
        .ok_or_else(|| anyhow::anyhow!("client live non initialisé (startup non appelé)"))?;
    let client = lock.lock().await;

    let signable = client
        .limit_order()
        .token_id(token)
        .side(side)
        .price(price)
        .size(size)
        .order_type(OrderType::FAK)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("build order: {e}"))?;
    let signed = client
        .sign(&signer, signable)
        .await
        .map_err(|e| anyhow::anyhow!("sign: {e}"))?;
    if !matches!(&signed.signature, OrderSignature::Wrapped(_)) {
        anyhow::bail!("signature POLY_1271 inattendue (attendu ERC-7739 Wrapped) — vérifier le SDK");
    }
    tracing::info!(
        token = %token_id.chars().take(10).collect::<String>(),
        price = %price,
        size = %size,
        is_sell = args.is_sell,
        "ordre LIVE FAK signé"
    );
    if !live_armed {
        return Ok(PlaceResult::DryRun);
    }

    let t0 = std::time::Instant::now();
    let resp = client
        .post_order(signed)
        .await
        .map_err(|e| anyhow::anyhow!("POST /order: {e}"))?;
    let post_ms = t0.elapsed().as_millis() as u64;
    // Le CLOB peut répondre 200 avec success=false + errorMsg.
    if !resp.success || resp.error_msg.as_deref().is_some_and(|m| !m.is_empty()) {
        anyhow::bail!("ordre refusé par le CLOB: {}", resp.error_msg.unwrap_or_default());
    }
    let to_f64 = |d: &Decimal| f64::from_str(&d.to_string()).ok();
    let making = to_f64(&resp.making_amount);
    let taking = to_f64(&resp.taking_amount);
    // BUY : making = USDC dépensés, taking = shares reçus (inverse en SELL).
    let (filled_size, avg_price) = match (making, taking) {
        (Some(m), Some(t)) => {
            let (shares, usdc) = if args.is_sell { (m, t) } else { (t, m) };
            if shares > 0.0 {
                (Some(shares), Some(usdc / shares))
            } else {
                (Some(0.0), None)
            }
        }
        _ => (None, None),
    };
    tracing::info!(post_ms, order_id = %resp.order_id, ?filled_size, ?avg_price, "✅ ordre LIVE accepté");
    Ok(PlaceResult::Placed {
        order_id: resp.order_id,
        filled_size,
        avg_price,
        post_ms,
    })
}

// ─── plomberie SDK ───

async fn authenticated_client<S: Signer>(
    creds: &LiveCredentials,
    signer: &S,
) -> anyhow::Result<Client<Authenticated<Normal>>> {
    let funder: Address = creds
        .funder
        .parse()
        .map_err(|e| anyhow::anyhow!("funder: {e}"))?;
    let api_key = creds
        .api_key
        .parse()
        .map_err(|e| anyhow::anyhow!("POLY_API_KEY: {e}"))?;
    let sdk_creds = Credentials::new(api_key, creds.api_secret.clone(), creds.passphrase.clone());
    Client::new(CLOB_BASE, Config::default())?
        .authentication_builder(signer)
        .funder(funder)
        .signature_type(SignatureType::Poly1271)
        .credentials(sdk_creds)
        .authenticate()
        .await
        .map_err(|e| anyhow::anyhow!("authenticate: {e}"))
}

fn local_signer(
    creds: &LiveCredentials,
) -> anyhow::Result<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>> {
    if let Some(s) = CACHED_LOCAL_SIGNER.get() {
        return Ok(s.clone());
    }
    Ok(LocalSigner::from_str(&creds.private_key)
        .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
        .with_chain_id(Some(POLYGON)))
}

fn decimal_from_f64(v: f64, decimal_places: u32, field: &str) -> anyhow::Result<Decimal> {
    if !v.is_finite() || v <= 0.0 {
        anyhow::bail!("{field} invalide: {v}");
    }
    let d = Decimal::from_f64_retain(v).ok_or_else(|| anyhow::anyhow!("{field} invalide: {v}"))?;
    // normalize() retire les zéros traînants, sinon le SDK rejette
    // « price decimal places > tick size decimal places ».
    Ok(d.round_dp(decimal_places).normalize())
}
