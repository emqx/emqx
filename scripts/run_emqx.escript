#!/usr/bin/env escript

main(_) ->
    start().

start() ->
    ok = application:load(mnesia),
    MnesiaName = lists:concat(["Mnesia.", atom_to_list(node())]),
    MnesiaDir = filename:join(["_build", "data", MnesiaName]),
    ok = application:set_env(mnesia, dir, MnesiaDir),
    SpecEmqxConfig = fun(_) -> ok end,
    start(SpecEmqxConfig).

start(SpecEmqxConfig) ->
    SchemaPath = filename:join(["priv", "emqx.schema"]),
    ConfPath = filename:join(["etc", "emqx.conf"]),
    emqx_ct_helpers:start_app(emqx, SchemaPath, ConfPath, SpecEmqxConfig).
