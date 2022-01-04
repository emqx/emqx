#!/usr/bin/env bash

set -euo pipefail

elixir -e "System.version() |> IO.puts()"
