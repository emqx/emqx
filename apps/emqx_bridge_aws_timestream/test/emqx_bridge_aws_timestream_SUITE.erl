%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_aws_timestream_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_aws_timestream.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%% -import(emqx_common_test_helpers, [on_exit/1]).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(PROXY_NAME_TCP, "influxdb_tcp").
-define(PROXY_NAME_TLS, "influxdb_tls").

-define(HELPER_POOL, <<"influx_suite">>).

-define(api_v2, api_v2).
-define(async, async).
-define(sync, sync).
-define(tcp, tcp).
-define(tls, tls).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

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
            emqx_bridge_aws_timestream,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?api_v2, TCConfig) ->
    [{api_type, ?api_v2} | TCConfig];
init_per_group(?tcp, TCConfig) ->
    [
        {host, <<"toxiproxy">>},
        {port, 8086},
        {use_tls, false},
        {proxy_name, ?PROXY_NAME_TCP}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {host, <<"toxiproxy">>},
        {port, 8087},
        {use_tls, true},
        {proxy_name, ?PROXY_NAME_TLS}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(
        merge_maps([
            #{<<"server">> => server(TCConfig)},
            connector_config_auth_fields(TCConfig),
            #{<<"ssl">> => #{<<"enable">> => get_config(use_tls, TCConfig, false)}}
        ])
    ),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{<<"batch_size">> => get_config(batch_size, TCConfig, 1)}
    }),
    start_ehttpc_helper_pool(TCConfig),
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
    snabbkaffe:stop(),
    reset_proxy(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    stop_ehttpc_helper_pool(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

merge_maps(Maps) ->
    lists:foldl(fun maps:merge/2, #{}, Maps).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"server">> => <<"toxiproxy:8086">>,
        <<"max_inactive">> => <<"10s">>,
        <<"pool_size">> => 2,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

connector_config_auth_fields(TCConfig) ->
    case get_config(api_type, TCConfig, ?api_v2) of
        ?api_v2 ->
            #{
                <<"parameters">> => #{
                    <<"influxdb_type">> => <<"influxdb_api_v2">>,
                    <<"bucket">> => <<"mqtt">>,
                    <<"org">> => <<"emqx">>,
                    <<"token">> => <<"abcdefg">>
                }
            }
    end.

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"fallback_actions">> => [],
        <<"parameters">> => #{
            <<"precision">> => <<"ns">>,
            <<"write_syntax">> => example_write_syntax()
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

example_write_syntax() ->
    %% N.B.: this single space character is relevant
    <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "float_value=${payload.float_key},", "undef_value=${payload.undef},",
        "${undef_key}=\"hard-coded-value\",", "bool=${payload.bool}">>.

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
    ProxyName = get_config(proxy_name, TCConfig, ?PROXY_NAME_TCP),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

server(TCConfig) ->
    case get_config(use_tls, TCConfig, false) of
        false ->
            <<"toxiproxy:8086">>;
        true ->
            <<"toxiproxy:8087">>
    end.

full_matrix() ->
    [
        [APIType, Conn, Sync, Batch]
     || APIType <- [?api_v2],
        Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ].

start_ehttpc_helper_pool(TCConfig) ->
    Host = get_config(host, TCConfig, <<"toxiproxy">>),
    Port = get_config(port, TCConfig, 8086),
    {Transport, TransportOpts} =
        case get_config(use_tls, TCConfig, false) of
            true -> {tls, [{verify, verify_none}]};
            false -> {tcp, []}
        end,
    PoolOpts = [
        {host, str(Host)},
        {port, Port},
        {pool_size, 1},
        {transport, Transport},
        {transport_opts, TransportOpts}
    ],
    {ok, _} = ehttpc_sup:start_pool(?HELPER_POOL, PoolOpts),
    ok.

stop_ehttpc_helper_pool() ->
    ehttpc_sup:stop_pool(?HELPER_POOL).

str(X) -> emqx_utils_conv:str(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [APIType, Conn, ?sync, ?without_batch]
     || APIType <- [?api_v2],
        Conn <- [?tcp, ?tls]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, influxdb_client_stopped).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [APIType, Conn, ?sync, ?without_batch]
     || APIType <- [?api_v2],
        Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_influxdb_SUITE:t_rule_action(TCConfig).
