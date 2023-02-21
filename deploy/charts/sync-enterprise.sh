#!/usr/bin/env bash

set -euo pipefail

sed 's|emqx/emqx|emqx/emqx-enterprise|' < emqx/values.yaml > emqx-enterprise/values.yaml
cp emqx/templates/* emqx-enterprise/templates
