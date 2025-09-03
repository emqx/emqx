%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dynamo_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("erlcloud/include/erlcloud_ddb2.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, dynamo).
-define(CONNECTOR_TYPE_BIN, <<"dynamo">>).
-define(ACTION_TYPE, dynamo).
-define(ACTION_TYPE_BIN, <<"dynamo">>).

-define(PROXY_NAME, "dynamo").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(with_batch, with_batch).
-define(without_batch, without_batch).

-define(TABLE, <<"mqtt">>).

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
            emqx_bridge_dynamo,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    %% fixme: this bridge sets an application-wide config with a connector-specific config
    %% value...  🫠
    %% we set it here to avoid flakiness/inconsistency (i.e.: test process sees one state,
    %% connector sees another...)
    application:set_env(erlcloud, aws_region, "us-west-2"),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    application:unset_env(erlcloud, aws_region),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    create_table(),
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
    delete_table(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"url">> => <<"http://toxiproxy:8000">>,
        <<"aws_access_key_id">> => <<"root">>,
        <<"aws_secret_access_key">> => <<"public">>,
        <<"region">> => <<"us-west-2">>,
        <<"pool_size">> => 1,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"table">> => ?TABLE,
            <<"hash_key">> => <<"topic">>,
            <<"undefined_vars_as_null">> => false
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

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

setup_dynamo() ->
    erlcloud_ddb2:configure("root", "public", "toxiproxy", 8000, "http://").

setup_dynamo_direct() ->
    erlcloud_ddb2:configure("root", "public", "dynamo", 8000, "http://").

directly_query(Query) ->
    setup_dynamo(),
    emqx_bridge_dynamo_connector_client:execute(Query, ?TABLE, #{}).

directly_get_payload(Key) ->
    directly_get_field(Key, <<"payload">>).

directly_get_field(Key, Field) ->
    case directly_query({get_item, Key}) of
        {ok, Values} ->
            proplists:get_value(Field, Values, {error, {invalid_item, Values}});
        Error ->
            Error
    end.

ct_inspect(Line, X) ->
    ct:pal("~b: ~p", [Line, X]),
    X.
-define(ct_inspect(X), ct_inspect(?LINE, X)).

%% create a table, use the apps/emqx_bridge_dynamo/priv/dynamo/mqtt_msg.json as template
create_table() ->
    setup_dynamo(),
    delete_table(),
    ?assertMatch(
        {ok, _},
        erlcloud_ddb2:create_table(
            ?TABLE,
            [{<<"clientid">>, s}],
            <<"clientid">>,
            [{provisioned_throughput, {5, 5}}]
        )
    ),
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, #ddb2_table_description{table_status = active}},
            ?ct_inspect(erlcloud_ddb2:describe_table(?TABLE))
        )
    ),
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, []},
            ?ct_inspect(erlcloud_ddb2:get_item(?TABLE, {<<"clientid">>, <<"just_testing">>}))
        )
    ),
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, [?TABLE]},
            ?ct_inspect(erlcloud_ddb2:list_tables([]))
        )
    ).

delete_table() ->
    erlcloud_ddb2:delete_table(?TABLE),
    ?retry(
        200,
        20,
        ?assertMatch(
            {error, {<<"ResourceNotFoundException">>, _}},
            ?ct_inspect(erlcloud_ddb2:describe_table(?TABLE))
        )
    ),
    ok.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

json_encode(X) ->
    emqx_utils_json:encode(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, dynamo_connector_on_stop).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => connecting}).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?without_batch], [?with_batch]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    StartClientOpts = #{clientid => ClientId},
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            500,
            20,
            ?assertEqual(
                Payload,
                ?ct_inspect(directly_get_payload({<<"clientid">>, ClientId})),
                #{clientid => ClientId, payload => Payload}
            )
        ),
        ?assertMatch(
            %% the old behavior without undefined_vars_as_null
            <<"undefined">>,
            directly_get_field({<<"clientid">>, ClientId}, <<"foo">>)
        ),
        ok
    end,
    SQL = <<"select foo, * from \"t_rule_action\" ">>,
    RuleTopic = <<"t_rule_action">>,
    Opts = #{
        sql => SQL,
        rule_topic => RuleTopic,
        start_client_opts => StartClientOpts,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_rule_test_trace(TCConfig) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, Opts).

t_action_sync_query(TCConfig) ->
    MakeMessageFun = fun() ->
        #{
            clientid => <<"clientid">>,
            id => <<"the_message_id">>,
            payload => <<"HELLO">>,
            topic => <<"rule_topic">>
        }
    end,
    IsSuccessCheck = fun(Result) -> ?assertEqual({ok, []}, Result) end,
    TracePoint = dynamo_connector_query_return,
    emqx_bridge_v2_testlib:t_sync_query(TCConfig, MakeMessageFun, IsSuccessCheck, TracePoint).

t_undefined_vars_as_null(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"select foo, * from \"${t}\" ">>,
        TCConfig
    ),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = start_client(#{clientid => ClientId}),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>),
                #{?snk_kind := dynamo_connector_query_return},
                10_000
            ),
            ?assertMatch(
                undefined,
                directly_get_field({<<"clientid">>, ClientId}, <<"foo">>)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{result := {ok, _}}], ?of_kind(dynamo_connector_query_return, Trace)),
            ok
        end
    ),
    ok.

t_missing_data(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<"select topic from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    ?assertMatch(
        {_, {ok, #{error := {<<"ValidationException">>, <<>>}}}},
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"hey">>),
            #{?snk_kind := dynamo_connector_query_return},
            10_000
        )
    ),
    ok.

t_missing_hash_key(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<"select foo from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    ?assertMatch(
        {_, {ok, #{error := missing_filter_or_range_key}}},
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"hey">>),
            #{?snk_kind := dynamo_connector_query_return},
            10_000
        )
    ),
    ok.
