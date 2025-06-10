%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_doris_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_doris.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("mysql/include/protocol.hrl").

%% -import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(HOST_PLAIN, <<"toxiproxy">>).
-define(HOST_TLS, <<"toxiproxy">>).
-define(PORT_PLAIN, 9030).
-define(PORT_TLS, 9130).
-define(USERNAME, <<"root">>).
-define(PROXY_NAME, "doris-fe").
-define(PROXY_NAME_TLS, "doris-fe-tls").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            get_tc_prop(TestCase, matrix, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_doris,
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

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    IsTLS = is_tls(TCConfig),
    {Port, ProxyName} =
        case IsTLS of
            true -> {?PORT_TLS, ?PROXY_NAME_TLS};
            false -> {?PORT_PLAIN, ?PROXY_NAME}
        end,
    Host = host(TCConfig),
    Server = server(Host, Port),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{<<"server">> => Server}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    create_database(TCConfig),
    create_table(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {proxy_name, ProxyName}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    drop_table(TCConfig),
    ok.

is_tls(TCConfig) ->
    case [tls || tls <- group_path(TCConfig, [])] of
        [] ->
            false;
        _ ->
            true
    end.

host(TCConfig) when is_list(TCConfig) -> host(is_tls(TCConfig));
host(_IsTLS = true) -> ?HOST_TLS;
host(_IsTLS = false) -> ?HOST_PLAIN.

port(TCConfig) when is_list(TCConfig) -> port(is_tls(TCConfig));
port(_IsTLS = true) -> ?PORT_TLS;
port(_IsTLS = false) -> ?PORT_PLAIN.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

server(Host, Port) ->
    iolist_to_binary(io_lib:format("~s:~b", [Host, Port])).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"database">> => <<"mqtt">>,
        <<"server">> => server(?HOST_PLAIN, ?PORT_PLAIN),
        <<"pool_size">> => 8,
        <<"username">> => ?USERNAME,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"parameters">> => #{
            <<"sql">> => action_sql()
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_table_ddl() ->
    <<
        "create table if not exists t_mqtt_msg "
        "(msgid varchar, topic string, qos tinyint, "
        "payload string, arrived datetime) "
        "properties (replication_num = 1)"
    >>.

action_sql() ->
    <<
        "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived)"
        " values (${id}, ${topic}, ${qos}, ${payload},"
        " FROM_UNIXTIME(${timestamp}/1000))"
    >>.

eval_query(SQL, TCConfig) ->
    Opts = #{
        host => emqx_utils_conv:str(host(TCConfig)),
        port => port(TCConfig),
        user => ?USERNAME,
        basic_capabilities => #{?CLIENT_TRANSACTIONS => false}
    },
    {ok, C} = mysql:start_link(maps:to_list(Opts)),
    Res = mysql:query(C, iolist_to_binary(SQL)),
    mysql:stop(C),
    Res.

create_database(TCConfig) ->
    ok = eval_query(["create database if not exists mqtt"], TCConfig).

create_table(TCConfig) ->
    ok = eval_query(["use mqtt; ", create_table_ddl()], TCConfig).

drop_table(TCConfig) ->
    ok = eval_query(["use mqtt; drop table t_mqtt_msg"], TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[plain], [tls]];
t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, "doris_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[plain], [tls]];
t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => [connecting, disconnected]}).

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[plain], [tls]];
t_rule_action(Config) when is_list(Config) ->
    PostPublishFn = fun(Context) ->
        #{rule_topic := RuleTopic, payload := Payload} = Context,
        QoS = 2,
        ?assertMatch(
            {ok, _ColNames, [[_MsgId, RuleTopic, QoS, Payload, {{_, _, _}, {_, _, _}}]]},
            eval_query(<<"use mqtt; select * from t_mqtt_msg">>, Config)
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts).
