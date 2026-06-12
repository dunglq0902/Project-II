#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  pytest -q -m spark
else
  pytest -q "$@"
fi
