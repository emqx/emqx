#!/bin/sh
set -e

rebar3 compile

erl -sname abc -pa _build/default/lib/*/ebin _build/default/lib/emqx_resource/examples -s demo
