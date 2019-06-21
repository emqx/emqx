#!/usr/bin/env escript

main(_) ->
    start().

start() ->
    SpecEmqxConfig = fun(_) -> ok end,
    start(SpecEmqxConfig).

start(SpecEmqxConfig) ->
    SchemaPath = filename:join(["priv", "emqx.schema"]),
    ConfPath = filename:join(["etc", "emqx.conf"]),
    emqx_ct_helpers:start_app(emqx, SchemaPath, ConfPath, SpecEmqxConfig).
