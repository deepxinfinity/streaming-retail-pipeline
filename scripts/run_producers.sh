#!/usr/bin/env bash
#Run both live producers
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
trap 'kill 0' SIGINT SIGTERM EXIT

$PY -m sim.producers.online &
$PY -m sim.producers.pos &
wait
