#!/usr/bin/env bash

set -euo pipefail

if command -v elixir &>/dev/null
then
  elixir -e "System.version() |> IO.puts()"
fi
