%%-------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_configs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_connector,
            emqx_bridge,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_get_with_json(_Config) ->
    {ok, Configs} = get_configs_with_json(),
    maps:map(
        fun(Name, Value) ->
            {ok, Config} = get_config(Name),
            ?assertEqual(Value, Config)
        end,
        maps:remove(<<"license">>, Configs)
    ),
    ok.

t_update(_Config) ->
    %% update ok
    {ok, SysMon} = get_config(<<"sysmon">>),
    #{<<"vm">> := #{<<"busy_port">> := BusyPort}} = SysMon,
    NewSysMon = #{<<"vm">> => #{<<"busy_port">> => not BusyPort}},
    {ok, #{}} = update_config(<<"sysmon">>, NewSysMon),
    {ok, SysMon1} = get_config(<<"sysmon">>),
    #{<<"vm">> := #{<<"busy_port">> := BusyPort1}} = SysMon1,
    ?assertEqual(BusyPort, not BusyPort1),
    assert_busy_port(BusyPort1),
    %% Make sure the override config is updated, and remove the default value.
    ?assertMatch(
        #{<<"vm">> := #{<<"busy_port">> := BusyPort1}},
        maps:get(<<"sysmon">>, emqx_config:read_override_conf(#{override_to => cluster}))
    ),

    %% update failed
    ErrorSysMon = emqx_utils_maps:deep_put([<<"vm">>, <<"busy_port">>], SysMon, "123"),
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_REQUEST">>}},
        update_config(<<"sysmon">>, ErrorSysMon)
    ),
    {ok, SysMon2} = get_config(<<"sysmon">>),
    ?assertEqual(SysMon1, SysMon2),

    %% reset specific config
    ok = reset_config(<<"sysmon">>, [{conf_path, <<"vm.busy_port">>}]),
    {ok, SysMon3} = get_config(<<"sysmon">>),
    ?assertMatch(#{<<"vm">> := #{<<"busy_port">> := true}}, SysMon3),
    assert_busy_port(true),

    %% reset no_default_value config
    NewSysMon1 = emqx_utils_maps:deep_put([<<"vm">>, <<"busy_port">>], SysMon, false),
    {ok, #{}} = update_config(<<"sysmon">>, NewSysMon1),
    ?assertMatch(
        {400, #{<<"code">> := <<"NO_DEFAULT_VALUE">>}},
        reset_config(<<"sysmon">>, [])
    ),
    {ok, SysMon4} = get_config(<<"sysmon">>),
    ?assertMatch(#{<<"vm">> := #{<<"busy_port">> := false}}, SysMon4),
    ok.

assert_busy_port(BusyPort) ->
    {_Pid, Monitors} = erlang:system_monitor(),
    RealBusyPort = proplists:get_value(busy_port, Monitors, false),
    ?assertEqual(BusyPort, RealBusyPort).

t_log(_Config) ->
    {ok, Log} = get_config("log"),
    File = "log/emqx-test.log",
    %% update handler
    Log1 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"enable">>], Log, true),
    Log2 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"path">>], Log1, File),
    {ok, #{}} = update_config(<<"log">>, Log2),
    {ok, Log3} = logger:get_handler_config(default),
    ?assertMatch(#{config := #{file := File}}, Log3),
    ErrLog1 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"enable">>], Log, 1),
    ?assertMatch({400, #{<<"code">> := <<"BAD_REQUEST">>}}, update_config(<<"log">>, ErrLog1)),
    ErrLog2 = emqx_utils_maps:deep_put(
        [<<"file">>, <<"default">>, <<"enabfe">>], Log, true
    ),
    ?assertMatch({400, #{<<"code">> := <<"BAD_REQUEST">>}}, update_config(<<"log">>, ErrLog2)),

    %% add new handler
    File1 = "log/emqx-test1.log",
    Handler = emqx_utils_maps:deep_get([<<"file">>, <<"default">>], Log2),
    NewLog1 = emqx_utils_maps:deep_put([<<"file">>, <<"new">>], Log2, Handler),
    NewLog2 = emqx_utils_maps:deep_put(
        [<<"file">>, <<"new">>, <<"path">>], NewLog1, File1
    ),
    {ok, #{}} = update_config(<<"log">>, NewLog2),
    {ok, Log4} = logger:get_handler_config(new),
    ?assertMatch(#{config := #{file := File1}}, Log4),

    %% disable new handler
    Disable = emqx_utils_maps:deep_put(
        [<<"file">>, <<"new">>, <<"enable">>], NewLog2, false
    ),
    {ok, #{}} = update_config(<<"log">>, Disable),
    ?assertEqual({error, {not_found, new}}, logger:get_handler_config(new)),
    ok.

t_global_zone(_Config) ->
    {ok, Zones} = get_global_zone(),
    ZonesKeys = lists:map(
        fun({K, _}) -> atom_to_binary(K) end, emqx_zone_schema:global_zone_with_default()
    ),
    ?assertEqual(lists:usort(ZonesKeys), lists:usort(maps:keys(Zones))),
    ?assertEqual(
        emqx_config:get_zone_conf(default, [mqtt, max_qos_allowed]),
        emqx_utils_maps:deep_get([<<"mqtt">>, <<"max_qos_allowed">>], Zones)
    ),
    ?assertError(
        {config_not_found, [zones, no_default, mqtt, max_qos_allowed]},
        emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed])
    ),

    NewZones1 = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 1),
    NewZones2 = emqx_utils_maps:deep_remove([<<"mqtt">>, <<"peer_cert_as_clientid">>], NewZones1),
    {ok, #{<<"mqtt">> := Res}} = update_global_zone(NewZones2),
    %% Make sure peer_cert_as_clientid is not removed(fill default)
    ?assertMatch(
        #{
            <<"max_qos_allowed">> := 1,
            <<"peer_cert_as_clientid">> := <<"disabled">>
        },
        Res
    ),
    ?assertEqual(1, emqx_config:get_zone_conf(default, [mqtt, max_qos_allowed])),
    ?assertError(
        {config_not_found, [zones, no_default, mqtt, max_qos_allowed]},
        emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed])
    ),
    %% Make sure the override config is updated, and remove the default value.
    ?assertMatch(#{<<"max_qos_allowed">> := 1}, read_conf(<<"mqtt">>)),

    BadZones = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 3),
    ?assertMatch({400, #{<<"code">> := <<"BAD_REQUEST">>}}, update_global_zone(BadZones)),

    %% Remove max_qos_allowed from raw config, but we still get default value(2).
    Mqtt0 = emqx_conf:get_raw([<<"mqtt">>]),
    ?assertEqual(1, emqx_utils_maps:deep_get([<<"max_qos_allowed">>], Mqtt0)),
    Mqtt1 = maps:remove(<<"max_qos_allowed">>, Mqtt0),
    ok = emqx_config:put_raw([<<"mqtt">>], Mqtt1),
    Mqtt2 = emqx_conf:get_raw([<<"mqtt">>]),
    ?assertNot(maps:is_key(<<"max_qos_allowed">>, Mqtt2), Mqtt2),
    {ok, #{<<"mqtt">> := Mqtt3}} = get_global_zone(),
    %% the default value is 2
    ?assertEqual(2, emqx_utils_maps:deep_get([<<"max_qos_allowed">>], Mqtt3)),
    ok = emqx_config:put_raw([<<"mqtt">>], Mqtt0),

    DefaultZones = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 2),
    {ok, #{}} = update_global_zone(DefaultZones),
    #{<<"mqtt">> := Mqtt} = emqx_config:fill_defaults(emqx_schema, #{<<"mqtt">> => #{}}, #{}),
    Default = maps:map(
        fun
            (_, V) when is_boolean(V) -> V;
            (_, V) when is_atom(V) -> atom_to_binary(V);
            (_, V) -> V
        end,
        Mqtt
    ),
    ?assertEqual(Default, read_conf(<<"mqtt">>)),
    ok.

get_global_zone() ->
    get_config("global_zone").

update_global_zone(Change) ->
    update_config("global_zone", Change).

%% v2 version binary
t_configs_key(_Config) ->
    Keys = lists:sort(emqx_conf_cli:keys()),
    {ok, Hocon} = get_configs_with_binary(undefined),
    ?assertEqual(Keys, lists:sort(maps:keys(Hocon))),
    {ok, Log} = get_configs_with_binary("log"),
    ?assertMatch(
        #{
            <<"log">> := #{
                <<"console">> := #{
                    <<"enable">> := _,
                    <<"formatter">> := <<"text">>,
                    <<"level">> := <<"warning">>,
                    <<"time_offset">> := <<"system">>
                },
                <<"file">> := _
            }
        },
        Log
    ),
    Log1 = emqx_utils_maps:deep_put([<<"log">>, <<"console">>, <<"level">>], Log, <<"error">>),
    ?assertMatch({200, _}, update_configs_with_binary(iolist_to_binary(hocon_pp:do(Log1, #{})))),
    ?assertEqual(<<"error">>, read_conf([<<"log">>, <<"console">>, <<"level">>])),
    BadLog = emqx_utils_maps:deep_put([<<"log">>, <<"console">>, <<"level">>], Log, <<"erro1r">>),
    ?assertEqual(
        {400, #{
            <<"errors">> => #{
                <<"log">> =>
                    #{
                        <<"kind">> => <<"validation_error">>,
                        <<"path">> => <<"log.console.level">>,
                        <<"reason">> => <<"unable_to_convert_to_enum_symbol">>,
                        <<"value">> => <<"erro1r">>
                    }
            }
        }},
        update_configs_with_binary(iolist_to_binary(hocon_pp:do(BadLog, #{})))
    ),
    ReadOnlyConf = #{
        <<"cluster">> =>
            #{
                <<"autoclean">> => <<"23h">>,
                <<"autoheal">> => true,
                <<"discovery_strategy">> => <<"manual">>
            }
    },
    ReadOnlyBin = iolist_to_binary(hocon_pp:do(ReadOnlyConf, #{})),
    ?assertMatch(
        {400, #{<<"errors">> := <<"Cannot update read-only key 'cluster", _/binary>>}},
        update_configs_with_binary(ReadOnlyBin)
    ),
    ?assertMatch(
        {200, _},
        update_configs_with_binary(ReadOnlyBin, _IgnoreReadonly = true)
    ).

t_get_configs_in_different_accept(_Config) ->
    [Key | _] = lists:sort(emqx_conf_cli:keys()),
    URI = emqx_mgmt_api_test_util:api_path(["configs?key=" ++ Key]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Request = fun(Accept) ->
        Headers = [{"accept", Accept}, Auth],
        case
            emqx_mgmt_api_test_util:request_api(get, URI, [], Headers, [], #{return_all => true})
        of
            {ok, {{_, Code, _}, RespHeaders, Body}} ->
                Type = proplists:get_value("content-type", RespHeaders),
                {Code, Type, Body};
            {error, {{_, Code, _}, RespHeaders, Body}} ->
                Type = proplists:get_value("content-type", RespHeaders),
                {Code, Type, Body}
        end
    end,

    %% returns text/plain if text/plain is acceptable
    ?assertMatch({200, "text/plain", _}, Request(<<"text/plain">>)),
    ?assertMatch({200, "text/plain", _}, Request(<<"*/*">>)),
    ?assertMatch(
        {200, "text/plain", _},
        Request(<<"application/json, application/xml;q=0.9, image/webp, */*;q=0.8">>)
    ),
    %% returns application/json if it only support it
    ?assertMatch({200, "application/json", _}, Request(<<"application/json">>)),
    %% returns error if it set to other type
    ?assertMatch({400, "application/json", _}, Request(<<"application/xml">>)).

t_config_update_parse_error(_Config) ->
    BadHoconList = [
        <<"not an object">>,
        <<"a = \"tlsv1\"\"\"3e-01">>
    ],
    lists:map(
        fun(BadHocon) ->
            ?assertMatch(
                {400, #{
                    <<"errors">> :=
                        #{
                            <<"line">> := 1,
                            <<"reason">> := _,
                            <<"type">> := <<"parse_error">>
                        }
                }},
                update_configs_with_binary(BadHocon)
            )
        end,
        BadHoconList
    ),
    ?assertMatch(
        {400, #{
            <<"errors">> := #{
                <<"line">> := 1,
                <<"reason">> := _,
                <<"type">> := <<"scan_error">>
            }
        }},
        update_configs_with_binary(<<"a=测试"/utf8>>)
    ).

t_config_update_unknown_root(_Config) ->
    ?assertMatch(
        {400, #{<<"errors">> := #{<<"a">> := <<"{root_key_not_found", _/binary>>}}},
        update_configs_with_binary(<<"a = \"tlsv1.3\"">>)
    ).

t_config_update_empty(_Config) ->
    ?assertMatch({200, _}, update_configs_with_binary("")).

t_config_update_unknown_field(_Config) ->
    ?assertMatch(
        {400, #{
            <<"errors">> := #{
                <<"dashboard">> :=
                    #{
                        <<"kind">> := <<"validation_error">>,
                        <<"reason">> := <<"unknown_fields">>,
                        <<"unknown">> := <<"nofield">>
                    }
            }
        }},
        update_configs_with_binary("dashboard { nofield { bind = novalue } }")
    ).

%% Helpers

get_config(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:simple_request(get, Path, []) of
        {200, Config} -> {ok, Config};
        Error -> Error
    end.

get_configs_with_json() ->
    get_configs_with_json("").

get_configs_with_json(Node) ->
    Req = #{
        method => get,
        url => emqx_mgmt_api_test_util:api_path(["configs"]),
        query_params => [{node, Node} || Node =/= ""],
        extra_headers => [{"accept", "application/json"}]
    },
    case emqx_mgmt_api_test_util:simple_request(Req) of
        {200, Config} -> {ok, Config};
        Error -> Error
    end.

get_configs_with_binary(Key) ->
    get_configs_with_binary(Key, "").

get_configs_with_binary(Key, Node) ->
    Req = #{
        method => get,
        url => emqx_mgmt_api_test_util:api_path(["configs"]),
        query_params => [{node, Node} || Node =/= ""] ++ [{key, Key} || Key =/= undefined],
        extra_headers => [{"accept", "text/plain"}]
    },
    case emqx_mgmt_api_test_util:simple_request(Req) of
        {200, Config} -> hocon:binary(Config);
        Error -> Error
    end.

update_configs_with_binary(Bin) ->
    update_configs_with_binary(Bin, _InogreReadonly = undefined).

update_configs_with_binary(Bin, IgnoreReadonly) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => put,
        url => emqx_mgmt_api_test_util:api_path(["configs"]),
        query_params => [{ignore_readonly, IgnoreReadonly} || is_boolean(IgnoreReadonly)],
        body => {raw, Bin},
        extra_headers => [{"accept", "text/plain"}],
        extra_opts => #{'content-type' => "text/plain"}
    }).

update_config(Name, Change) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:simple_request(put, Path, Change) of
        {200, Resp} -> {ok, Resp};
        Error -> Error
    end.

reset_config(Name, Query) ->
    Req = #{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["configs_reset", Name]),
        query_params => Query
    },
    case emqx_mgmt_api_test_util:simple_request(Req) of
        {200, _} -> ok;
        Error -> Error
    end.

read_conf(RootKeys) when is_list(RootKeys) ->
    case emqx_config:read_override_conf(#{override_to => cluster}) of
        undefined -> undefined;
        Conf -> emqx_utils_maps:deep_get(RootKeys, Conf, undefined)
    end;
read_conf(RootKey) ->
    read_conf([RootKey]).
