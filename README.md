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

## Avant tout passage en live

Bloqueurs connus, à traiter dans la matrice de complétude métier :

- [ ] Frais taker réels des fenêtres 5 min (à trancher au 1er fill live).
- [ ] Refresh de l'allowance CONDITIONAL après chaque BUY (sinon le SELL
      catastrophe est rejeté « balance 0 »).
- [ ] Vente catastrophe réelle en FAK (passe à toute taille) + vérification
      `success/error_msg` de chaque POST.
- [ ] `POLY_SIG_TYPE=3` (wallet de dépôt), signing, gestion des résidus.
- [ ] Radar Tokyo distant (UDP `WireTick`) au lieu de la garde locale.
- [ ] Mise minimale : un stack < ~5 $ ne remplit pas `orderMinSize` côté resting ;
      les FAK taker passent, mais la granularité 2 décimales mord sur un stack de 1 $.
