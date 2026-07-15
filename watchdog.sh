#!/bin/bash
# Watchdog du Grinder : relance le binaire s'il meurt.
# Lancer avec :  nohup bash watchdog.sh > /dev/null 2>&1 &
# Sa ligne de commande ("bash watchdog.sh") ne contient PAS le chemin du
# binaire : un `pkill -f target/release/le_grinder` ne tue que le bot,
# jamais le watchdog (leçon du 15 juil.).
cd "$(dirname "$0")"
BIN=./target/release/le_grinder
while true; do
  RUST_LOG=info "$BIN" >> grinder.log 2>&1
  echo "[WATCHDOG] $(date -u +%FT%TZ) le_grinder terminé (code $?), relance dans 5 s" >> grinder.log
  sleep 5
done
