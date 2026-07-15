# Le Grinder

Bot Polymarket **paper-only** sur les fenêtres BTC Up/Down 5 min : acheter le
favori quand il cote **≥ 95c**, remettre en jeu l'intégralité du stack à la
fenêtre suivante — compoundage exponentiel pur. Une fenêtre perdue = wipe,
retour à la mise de base, nouveau run.

La **seule défense** est la garde Tokyo (flux Binance) : marge z du spot vs
strike, drift, détecteur d'emballement OBI×vélocité. Si elle passe au rouge en
position, **vente en catastrophe** immédiate (taker, carnet balayé avec haircut).

## Les maths, sans fard

- Gain par win à 95c : **+5,26 %** (avant frais). Un loss à la résolution : **−100 %** du stack.
- Break-even ≈ un taux de réussite > 95 % — exactement ce que le marché price.
  L'edge éventuel vient **entièrement** du filtre Tokyo (z_entry) et de la
  sortie catastrophe. C'est ce que le paper doit prouver ou infirmer.
- La sortie catastrophe est une protection **partielle** : pendant un crash le
  carnet se vide. La simulation ampute la profondeur affichée de
  `PANIC_HAIRCUT` (défaut 50 %) et envoie le reste à zéro.

## Architecture

```
Binance WS (depth20@100ms) ──► TokyoGuard (drift EMA / vol / radar OBI×vélocité)
                                     │ 10 Hz
Polymarket Gamma+CLOB+WS ──► Grinder (machine à états)
                                     │
                    data/grinder_state.json      (état, écriture atomique)
                    data/grinder_windows.jsonl   (grand livre append-only)
                    dashboard HTTP :8095         (poll /state 1 Hz)
```

- `src/tokyo.rs` — garde locale : agrège drift/vol/radar, expose `margin_z`
  (distance log spot→strike normalisée par vol×√t restant) et le KILL latché.
- `src/grinder.rs` — machine à états : Scanning → InPosition → résolution ou
  vente catastrophe. Une entrée max par fenêtre, une position à la fois.
- `src/paper.rs` — exécution simulée : sweep taker du carnet réel, tailles
  2 décimales, frais `rate × p(1−p)`, haircut de crash sur les sorties.
- `src/connectors/`, `src/engines/` — portés du monolith (Binance WS,
  Gamma/CLOB REST, WS market Polymarket, drift, volatilité, radar).

## Cycle d'une fenêtre

1. Rollover : résolution du marché `btc-updown-5m-<ts>` (Gamma), souscription WS.
2. Strike fixé sur l'open Binance 1m de la fenêtre (proxy Chainlink).
3. **Entrée** si : un côté cote dans `[ENTRY_MIN, ENTRY_MAX]`, temps restant dans
   `[MIN_REMAINING_S, MAX_REMAINING_S]`, flux Binance frais, pas de KILL,
   `z ≥ Z_ENTRY`, drift pas à contre-sens → achat taker de tout le stack.
4. **En position** (à chaque tick Binance, 10 Hz) : KILL radar, `z < Z_EXIT` ou
   drift retourné → vente catastrophe (sweep bids × haircut, résidu à zéro).
5. **Résolution** : spot vs strike (égalité = défaite, conservateur).
   Win → stack composé ; loss → reset à `GRIND_BASE`, run suivant.

## Lancer

```bash
cp .env.example .env   # ajuster si besoin
cargo run --release
# dashboard : http://<machine>:8095  (bind 0.0.0.0 → accessible via Tailscale)
```

Reset : **ne jamais supprimer les fichiers de `data/`** (doctrine). Pour
repartir de zéro, pointer `STATE_PATH`/`WINDOWS_PATH` vers de nouveaux chemins.

## Mode LIVE

Deux verrous indépendants :
1. **Compilation** : `rustup run stable cargo build --release --features live`
   (SDK/alloy exigent rustc ≥ 1.91 — le rustc Homebrew 1.86 du PATH ne suffit
   pas, passer par la toolchain rustup). Binaire figé dans `bin/le_grinder_live`.
2. **Runtime** : `TRADING_MODE=live` dans le `.env` **et** `LIVE_ARMED=true`.
   Avec `LIVE_ARMED=false`, répétition générale : ordres signés + loggés,
   jamais postés.

```bash
# .env : TRADING_MODE=live, LIVE_ARMED=false d'abord, credentials POLY_* remplis
./bin/le_grinder_live          # répétition générale (aucun ordre posté)
# puis LIVE_ARMED=true quand la répétition est propre
```

L'état live vit dans `data/grinder_state_live.json` / `grinder_windows_live.jsonl`
(jamais partagés avec le paper). Le dashboard affiche le mode (badge rouge) et
le collatéral wallet réel après chaque clôture — c'est LE PnL qui fait foi.

### Matrice de complétude (état au 15 juil. 2026)

- [x] Signing POLY_1271 / sig_type 3 (deposit wallet), client SDK authentifié au boot.
- [x] Refresh allowance CONDITIONAL immédiatement après chaque BUY (sinon SELL
      rejeté « balance 0 ») + retry « ne jamais abandonner sur balance 0 » au SELL.
- [x] FAK uniquement (passe à toute taille), aucun ordre restant → ni cancel ni
      heartbeat dead-man nécessaires.
- [x] Vérification `success`/`error_msg` de chaque POST (200 ≠ succès).
- [x] Tailles 2 décimales, prix arrondi au tick 0.01.
- [x] Vente catastrophe : solde CONDITIONAL on-chain = vérité, FAK plancher
      0.01, 2e FAK sur résidu, résidu final loggé (ira à la résolution).
- [x] Stack plafonné au collatéral réel du wallet au boot ; collatéral relu et
      loggé après chaque clôture (vérité wallet vs ledger interne).
- [ ] **Redemption post-résolution à VÉRIFIER au 1er cycle live** : on suppose
      l'auto-settlement Polymarket des marchés crypto 5 min (USDC recrédité
      seul). Si le wallet ne bouge pas après une fenêtre gagnée → bloqueur,
      il faudra un redeem CTF explicite.
- [ ] Frais réels `crypto_fees_v2` à mesurer au 1er fill (maker=taker=1000 —
      l'impact exact sur les montants `making/taking` reste à confirmer).
- [ ] Garde Tokyo LOCALE (même machine) : latence Mac→CLOB non optimisée ;
      le radar Tokyo distant (UDP WireTick) reste un chantier ultérieur.
- [ ] Mise minimale réelle : FAK accepté à toute taille d'après nos leçons,
      mais un stack < 1 $ peut buter sur des minima CLOB non documentés.
