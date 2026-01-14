%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ACTION_TYPE, redis).
-define(ACTION_TYPE_BIN, <<"redis">>).
-define(CONNECTOR_TYPE, redis).
-define(CONNECTOR_TYPE_BIN, <<"redis">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(tcp, tcp).
-define(tls, tls).
-define(single, single).
-define(sentinel, sentinel).
-define(cluster, cluster).
-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

-define(KEYPREFIX, "MSGS").
-define(KEYSHARDS, 3).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_redis,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?tcp, TCConfig) ->
    [{transport, ?tcp} | TCConfig];
init_per_group(?tls, TCConfig) ->
    [{transport, ?tls} | TCConfig];
init_per_group(?single, TCConfig) ->
    ProxyConfig =
        case get_config(transport, TCConfig, ?tcp) of
            ?tcp ->
                [
                    {proxy_host, ?PROXY_HOST},
                    {proxy_port, ?PROXY_PORT},
                    {proxy_name, "redis_single_tcp"}
                ];
            ?tls ->
                []
        end,
    ProxyConfig ++ [{redis_type, ?single} | TCConfig];
init_per_group(?sentinel, TCConfig) ->
    [{redis_type, ?sentinel} | TCConfig];
init_per_group(?cluster, TCConfig) ->
    [{redis_type, ?cluster} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(?with_batch, TCConfig) ->
    [{batch_size, 5}, {batch_time, <<"100ms">>} | TCConfig];
init_per_group(?without_batch, TCConfig) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnTypeConfig = type_specific_connector_config_of(TCConfig),
    ConnectorConfig = connector_config(ConnTypeConfig),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>),
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    reset_proxy(),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

type_specific_connector_config_of(TCConfig) ->
    Type = get_config(redis_type, TCConfig, ?single),
    Transport = get_config(transport, TCConfig, ?tcp),
    case {Type, Transport} of
        {?single, ?tcp} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"single">>,
                    <<"server">> => <<"toxiproxy:6379">>
                }
            };
        {?single, ?tls} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"single">>,
                    <<"server">> => <<"redis-tls:6380">>
                },
                <<"ssl">> => enabled_ssl_opts(TCConfig)
            };
        {?sentinel, ?tcp} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"sentinel">>,
                    <<"servers">> => <<"redis-sentinel:26379">>,
                    <<"sentinel">> => <<"mytcpmaster">>
                }
            };
        {?sentinel, ?tls} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"sentinel">>,
                    <<"servers">> => <<"redis-sentinel-tls:26380">>,
                    <<"sentinel">> => <<"mytlsmaster">>
                },
                <<"ssl">> => enabled_ssl_opts(TCConfig)
            };
        {?cluster, ?tcp} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"cluster">>,
                    <<"servers">> =>
                        <<"redis-cluster-1:6379,redis-cluster-2:6379,redis-cluster-3:6379">>
                }
            };
        {?cluster, ?tls} ->
            #{
                <<"parameters">> => #{
                    <<"redis_type">> => <<"cluster">>,
                    <<"servers">> =>
                        <<"redis-cluster-tls-1:6389,redis-cluster-tls-2:6389,redis-cluster-tls-3:6389">>
                },
                <<"ssl">> => enabled_ssl_opts(TCConfig)
            }
    end.

enabled_ssl_opts(TCConfig) ->
    Type = get_config(redis_type, TCConfig, ?single),
    maps:merge(
        client_ssl_cert_opts(Type),
        #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>
        }
    ).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            %% <<"username">> => <<"test_user">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => 1
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

client_ssl_cert_opts(?single) ->
    emqx_authn_test_lib:client_ssl_cert_opts();
client_ssl_cert_opts(_Type) ->
    Dir = code:lib_dir(emqx),
    #{
        <<"keyfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"client-key.pem">>]),
        <<"certfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"client-cert.pem">>]),
        <<"cacertfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"cacert.pem">>])
    }.

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"command_template">> => [<<"RPUSH">>, <<?KEYPREFIX, "/${topic}">>, <<"${payload}">>]
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig, "redis_single_tcp"),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

format_topic(Base, I) ->
    iolist_to_binary(io_lib:format("~s/~2..0B", [Base, I rem ?KEYSHARDS])).

format_redis_key(Base, I) ->
    iolist_to_binary([?KEYPREFIX, "/", format_topic(Base, I)]).

parsed_connector_config(TCConfig) ->
    ConnectorConfig0 = get_config(connector_config, TCConfig),
    %% Converting raw config
    #{connectors := #{?CONNECTOR_TYPE := #{x := ConnectorConfig}}} =
        hocon_tconf:check_plain(
            emqx_connector_schema,
            #{
                <<"connectors">> => #{
                    ?CONNECTOR_TYPE_BIN =>
                        #{<<"x">> => ConnectorConfig0}
                }
            },
            #{atom_key => true, required => false}
        ),
    ConnectorConfig.

with_redis(TCConfig, Fn) ->
    ConnectorConfig = parsed_connector_config(TCConfig),
    Mod = emqx_bridge_redis_connector,
    Id = emqx_resource:generate_id(atom_to_binary(?FUNCTION_NAME)),
    try
        {ok, ConnState} = Mod:on_start(Id, ConnectorConfig),
        Fn(Mod, Id, ConnState)
    after
        Mod:on_stop(Id, undefined)
    end.

read_range(Topic, TCConfig) ->
    with_redis(TCConfig, fun(Mod, Id, ConnState) ->
        Key = <<?KEYPREFIX, "/", Topic/binary>>,
        Query = {cmd, [<<"LRANGE">>, Key, <<"0">>, <<"-1">>]},
        {ok, Results} = Mod:on_query(Id, Query, ConnState),
        Results
    end).

clear_range(Topic, TCConfig) ->
    with_redis(TCConfig, fun(Mod, Id, ConnState) ->
        Key = <<?KEYPREFIX, "/", Topic/binary>>,
        Query = {cmd, [<<"LTRIM">>, Key, <<"1">>, <<"0">>]},
        {ok, _} = Mod:on_query(Id, Query, ConnState),
        ok
    end).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [Transport, Type]
     || Transport <- [?tcp, ?tls],
        Type <- [?single, ?sentinel, ?cluster]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "redis_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[?tcp, ?single]];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_disconnected}).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Transport, Type, Sync, Batch]
     || Transport <- [?tcp, ?tls],
        Type <- [?single, ?sentinel, ?cluster],
        Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PrePublishFn = fun(Context) ->
        #{rule_topic := Topic} = Context,
        clear_range(Topic, TCConfig),
        Context
    end,
    PostPublishFn = fun(Context) ->
        #{rule_topic := Topic, payload := Payload} = Context,
        on_exit(fun() -> clear_range(Topic, TCConfig) end),
        ?retry(200, 10, ?assertMatch([Payload], read_range(Topic, TCConfig)))
    end,
    Opts = #{
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_check_replay() ->
    [{matrix, true}].
t_check_replay(matrix) ->
    [[?tcp, ?single, ?sync, ?with_batch]];
t_check_replay(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ct:timetrap({seconds, 15}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        ?wait_async_action(
            with_failure(down, TCConfig, fun() ->
                {_, {ok, _}} =
                    ?wait_async_action(
                        lists:foreach(
                            fun(_) ->
                                emqtt:publish(C, Topic, <<"test_payload">>)
                            end,
                            lists:seq(1, 5)
                        ),
                        #{
                            ?snk_kind := redis_bridge_connector_send_done,
                            batch := true,
                            result := {error, _}
                        }
                    )
            end),
            #{?snk_kind := redis_bridge_connector_send_done, batch := true, result := {ok, _}}
        ),
        fun(Trace) ->
            SubTrace = ?of_kind(redis_bridge_connector_send_done, Trace),
            %% No strict causality here because, depending on timing, multiple batches may
            %% be enqueued.
            ?assertMatch([#{result := {error, _}} | _], SubTrace),
            ?assertMatch([#{result := {ok, _}} | _], lists:reverse(SubTrace)),
            ok
        end
    ),
    ok.

t_permanent_error() ->
    [{matrix, true}].
t_permanent_error(matrix) ->
    [
        [?tcp, Type]
     || Type <- [?single, ?sentinel, ?cluster]
    ];
t_permanent_error(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"command_template">> => [<<"BAD">>, <<"COMMAND">>, <<"${payload}">>]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>),
                #{?snk_kind := redis_bridge_connector_send_done},
                10_000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{result := {error, _}} | _],
                ?of_kind(redis_bridge_connector_send_done, Trace)
            )
        end
    ),
    ok.

t_auth_username_password(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{
            <<"parameters">> => #{
                <<"username">> => <<"test_user">>,
                <<"password">> => <<"test_passwd">>
            }
        })
    ),
    ok.

t_auth_error_username_password(TCConfig) ->
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
        }},
        create_connector_api(TCConfig, #{
            <<"parameters">> => #{
                <<"username">> => <<"test_user">>,
                <<"password">> => <<"wrong_password">>
            }
        })
    ),
    ok.

%% Default config does not include username field
t_auth_error_password_only(TCConfig) ->
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
        }},
        create_connector_api(TCConfig, #{
            <<"parameters">> => #{<<"password">> => <<"wrong_password">>}
        })
    ),
    ok.

t_create_disconnected() ->
    [{matrix, true}].
t_create_disconnected(matrix) ->
    [[?tcp, ?single]];
t_create_disconnected(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {201, _} = create_connector_api(TCConfig, #{})
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := _} | _],
                ?of_kind(redis_bridge_connector_start_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_on_get_status_no_username_pass() ->
    [{matrix, true}].
t_on_get_status_no_username_pass(matrix) ->
    [
        [?tcp, ?single],
        [?tcp, ?sentinel]
    ];
t_on_get_status_no_username_pass(TCConfig) when is_list(TCConfig) ->
    ?check_trace(
        {201, #{<<"status">> := <<"disconnected">>}} =
            create_connector_api(TCConfig, #{
                <<"parameters">> => #{<<"username">> => <<"">>, <<"password">> => <<"">>}
            }),
        fun(Trace) ->
            case get_config(redis_type, TCConfig) of
                ?single ->
                    ?assertMatch([_ | _], ?of_kind(emqx_redis_auth_required_error, Trace));
                ?sentinel ->
                    ?assertMatch([_ | _], ?of_kind(emqx_redis_auth_required_error, Trace))
            end
        end
    ),
    ok.

t_map_to_redis_hset_args() ->
    [{matrix, true}].
t_map_to_redis_hset_args(matrix) ->
    [
        [?tcp, ?single],
        [?tcp, ?sentinel],
        [?tcp, ?cluster]
    ];
t_map_to_redis_hset_args(command_template) ->
    [<<"HMSET">>, <<"t_map_to_redis_hset_args">>, <<"${payload}">>];
t_map_to_redis_hset_args(TCConfig) when is_list(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"command_template">> =>
                [<<"HMSET">>, <<"t_map_to_redis_hset_args">>, <<"${payload}">>]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "select map_to_redis_hset_args(payload) as payload"
            " from \"${t}\" "
        >>,
        TCConfig
    ),
    C = start_client(),
    Payload = emqx_utils_json:encode(#{<<"a">> => 1, <<"b">> => <<"2">>}),
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload, [{qos, 1}]),
            #{?snk_kind := redis_bridge_connector_send_done},
            5_000
        ),
    ok.
