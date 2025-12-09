#!/usr/bin/env bash
set -euo pipefail

git describe --abbrev=0 --tags --exclude '*rc*' --exclude '*alpha*' --exclude '*beta*' --exclude '*docker*' --exclude '*-M*'
