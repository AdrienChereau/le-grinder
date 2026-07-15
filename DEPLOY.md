# Déploiement AWS (Dublin / Tokyo)

Le Grinder est mono-process (garde Tokyo locale) : chaque nœud fait tourner le
bot complet. Répartition recommandée, en attendant le vrai split radar UDP :

| Nœud | Rôle | Pourquoi |
|---|---|---|
| **Dublin** | LIVE (quand armé) | meilleur RTT ordres vers le CLOB Polymarket (leçon monolith) |
| **Tokyo** | PAPER témoin | flux Binance quasi-local → garde plus fraîche ; sert de référence pour mesurer ce que Dublin perd en latence Binance |

## 1. Cloner (repo privé → mêmes accès GitHub que les autres bots)

```bash
git clone git@github.com:AdrienChereau/le-grinder.git ~/le-grinder
cd ~/le-grinder
```

## 2. Toolchain

Paper : rustc stable suffit. Live : rustc ≥ 1.91 obligatoire (SDK/alloy).

```bash
rustup update stable
cargo build --release                      # binaire paper
cargo build --release --features live \
  && mkdir -p bin && cp target/release/le_grinder bin/le_grinder_live \
  && cargo build --release                 # Dublin uniquement : fige le binaire live puis rebuild paper
```

## 3. Le `.env` — JAMAIS dans git

Depuis le Mac (les credentials POLY_* ne transitent que par scp/Tailscale) :

```bash
scp .env dublin:~/le-grinder/.env    # puis éditer selon le nœud
ssh dublin 'chmod 600 ~/le-grinder/.env'
```

Par nœud :
- **Tokyo (paper)** : `TRADING_MODE=paper`, pas de POLY_*, `DASH_PORT=8095`.
- **Dublin (live)** : `TRADING_MODE=live`, `LIVE_ARMED=false` d'abord
  (répétition générale), credentials POLY_* complets, `POLY_SIG_TYPE=3`,
  `DASH_PORT=8095`.
- `DASH_BIND=0.0.0.0` partout (consultation via Tailscale).

## 4. Service systemd

```bash
sudo cp deploy/le-grinder.service /etc/systemd/system/
# Dublin live : éditer ExecStart → /home/ubuntu/le-grinder/bin/le_grinder_live
sudo systemctl daemon-reload
sudo systemctl enable --now le-grinder
journalctl -u le-grinder -f          # suivi
```

Dashboard : `http://<hostname-tailscale>:8095`.

## 5. Mise à jour

```bash
cd ~/le-grinder && git pull && cargo build --release
# (+ rebuild --features live sur Dublin, recopier bin/le_grinder_live)
sudo systemctl restart le-grinder    # redémarrer HORS position (phase=scanning au dashboard)
```

## Rappels

- Ne jamais supprimer `data/*.json(l)` pour « reset » — pointer STATE_PATH
  ailleurs si besoin.
- Armer le live (`LIVE_ARMED=true`) seulement après une répétition générale
  propre, avec un petit wallet, et vérifier au premier win que l'USDC revient
  seul (case « redemption » de la matrice du README).
