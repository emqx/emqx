#!/usr/bin/env escript

%% This script translates the hocon_schema_json's schema dump to a new format.
%% It is used to convert older version EMQX's schema dumps to the new format
%% after all files are upgraded to the new format, this script can be removed.

-mode(compile).

main([Input]) ->
    ok = add_libs(),
    _ = atoms(),
    {ok, Data} = file:read_file(Input),
    Json = jsx:decode(Data),
    NewJson = reformat(Json),
    io:format("~s~n", [jsx:encode(NewJson)]);
main(_) ->
    io:format("Usage: schema-dump-reformat.escript <input.json>~n"),
    halt(1).

reformat(Json) ->
    emqx_conf:reformat_schema_dump(fix(Json)).

%% fix old type specs to make them compatible with new type specs
fix(#{
    <<"kind">> := <<"union">>,
    <<"members">> := [#{<<"name">> := <<"string()">>}, #{<<"name">> := <<"function()">>}]
}) ->
    %% s3_exporter.secret_access_key
    #{
        kind => primitive,
        name => <<"string()">>
    };
fix(#{<<"kind">> := <<"primitive">>, <<"name">> := <<"emqx_conf_schema:log_level()">>}) ->
    #{
        kind => enum,
        symbols => [emergency, alert, critical, error, warning, notice, info, debug, none, all]
    };
fix(#{<<"kind">> := <<"primitive">>, <<"name">> := <<"emqx_connector_http:pool_type()">>}) ->
    #{kind => enum, symbols => [random, hash]};
fix(#{<<"kind">> := <<"primitive">>, <<"name">> := <<"emqx_bridge_http_connector:pool_type()">>}) ->
    #{kind => enum, symbols => [random, hash]};
fix(Map) when is_map(Map) ->
    maps:from_list(fix(maps:to_list(Map)));
fix(List) when is_list(List) ->
    lists:map(fun fix/1, List);
fix({<<"kind">>, Kind}) ->
    {kind, binary_to_atom(Kind, utf8)};
fix({<<"name">>, Type}) ->
    {name, fix_type(Type)};
fix({K, V}) ->
    {binary_to_atom(K, utf8), fix(V)};
fix(V) when is_number(V) ->
    V;
fix(V) when is_atom(V) ->
    V;
fix(V) when is_binary(V) ->
    V.

%% ensure below ebin dirs are added to code path:
%% _build/default/lib/*/ebin
%% _build/emqx/lib/*/ebin
%% _build/emqx-enterprise/lib/*/ebin
add_libs() ->
    Profile = os:getenv("PROFILE"),
    case Profile of
        "emqx" ->
            ok;
        "emqx-enterprise" ->
            ok;
        _ ->
            io:format("PROFILE is not set~n"),
            halt(1)
    end,
    Dirs =
        filelib:wildcard("_build/default/lib/*/ebin") ++
            filelib:wildcard("_build/" ++ Profile ++ "/lib/*/ebin"),
    lists:foreach(fun add_lib/1, Dirs).

add_lib(Dir) ->
    code:add_patha(Dir),
    Beams = filelib:wildcard(Dir ++ "/*.beam"),
    _ = spawn(fun() -> lists:foreach(fun load_beam/1, Beams) end),
    ok.

load_beam(BeamFile) ->
    ModuleName = filename:basename(BeamFile, ".beam"),
    Module = list_to_atom(ModuleName),
    %% load the beams to make sure the atoms are existing
    code:ensure_loaded(Module),
    ok.

fix_type(<<"[{string(), string()}]">>) ->
    <<"map()">>;
fix_type(<<"[{binary(), binary()}]">>) ->
    <<"map()">>;
fix_type(<<"emqx_limiter_schema:rate()">>) ->
    <<"string()">>;
fix_type(<<"emqx_limiter_schema:burst_rate()">>) ->
    <<"string()">>;
fix_type(<<"emqx_limiter_schema:capacity()">>) ->
    <<"string()">>;
fix_type(<<"emqx_limiter_schema:initial()">>) ->
    <<"string()">>;
fix_type(<<"emqx_limiter_schema:failure_strategy()">>) ->
    <<"string()">>;
fix_type(<<"emqx_conf_schema:file()">>) ->
    <<"string()">>;
fix_type(<<"#{term() => binary()}">>) ->
    <<"map()">>;
fix_type(<<"[term()]">>) ->
    %% jwt claims
    <<"map()">>;
fix_type(<<"emqx_ee_bridge_influxdb:write_syntax()">>) ->
    <<"string()">>;
fix_type(<<"emqx_bridge_influxdb:write_syntax()">>) ->
    <<"string()">>;
fix_type(<<"emqx_schema:mqtt_max_packet_size()">>) ->
    <<"non_neg_integer()">>;
fix_type(<<"emqx_s3_schema:secret_access_key()">>) ->
    <<"string()">>;
fix_type(Type) ->
    Type.

%% ensure atoms are loaded
%% these atoms are from older version of emqx
atoms() ->
    [
        emqx_ee_connector_clickhouse,
        emqx_ee_bridge_gcp_pubsub,
        emqx_ee_bridge_influxdb,
        emqx_connector_http
    ].
