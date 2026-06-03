%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_api_2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_prometheus.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_data_integration(Mode, Format, Opts) ->
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "data_integration"]),
    get_prometheus(URL, Mode, Format, Opts).

get_prometheus(URL, Mode, Format, Opts) ->
    Headers =
        case Format of
            json -> [{"accept", "application/json"}];
            prometheus -> []
        end,
    QueryString = uri_string:compose_query([{"mode", atom_to_binary(Mode)}]),
    AuthHeader = maps:get(auth_header, Opts, {"no", "auth"}),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => Headers,
        query_params => QueryString,
        auth_header => AuthHeader
    }),
    case Format of
        json ->
            {Status, Response};
        prometheus when Status == 200 ->
            {Status, parse_prometheus(Response)};
        prometheus ->
            {Status, Response}
    end.

parse_prometheus(RawData) ->
    lists:foldl(
        fun
            (<<"#", _/binary>>, Acc) ->
                Acc;
            (Line, Acc) ->
                {Name, Labels, Value} = parse_prometheus_line(Line),
                maps:update_with(
                    Name,
                    fun(Old) -> Old#{Labels => Value} end,
                    #{Labels => Value},
                    Acc
                )
        end,
        #{},
        binary:split(iolist_to_binary(RawData), <<"\n">>, [global, trim_all])
    ).

parse_prometheus_line(Line) ->
    RE = <<"(?<name>[a-z0-9A-Z_]+)(\\{(?<labels>[^)]*)\\})? *(?<value>[0-9]+(\\.[0-9]+)?)">>,
    {match, [Name, Labels0, Value0]} = re:run(
        Line, RE, [{capture, [<<"name">>, <<"labels">>, <<"value">>], binary}]
    ),
    Labels = parse_prometheus_labels(Labels0),
    Value =
        try
            binary_to_float(Value0)
        catch
            error:badarg ->
                binary_to_integer(Value0)
        end,
    {Name, Labels, Value}.

parse_prometheus_labels(<<"">>) ->
    #{};
parse_prometheus_labels(Labels) ->
    lists:foldl(
        fun(Label, Acc) ->
            [K, V0] = binary:split(Label, <<"=">>),
            V = binary:replace(V0, <<"\"">>, <<"">>, [global]),
            Acc#{K => V}
        end,
        #{},
        binary:split(Labels, <<",">>, [global])
    ).

start_local(TestCase, TCConfig, Opts) ->
    ExtraApps = maps:get(extra_apps, Opts, []),
    AppSpecs =
        [
            emqx,
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_prometheus
        ] ++ ExtraApps,
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}),
    on_exit(fun() -> emqx_cth_suite:stop(Apps) end),
    Apps.

create_namespaced_user_auth_header(Opts) ->
    Token = emqx_bridge_v2_testlib:create_namespaced_user_and_token(Opts),
    {"Authorization", <<"Bearer ", Token/binary>>}.

global_admin_auth_header() ->
    emqx_mgmt_api_test_util:auth_header_().

create_connector_api(TCConfig, Overrides) ->
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Regression test for the case where there is an action whose connector does not exist.

This may arise if someone manually edits the configuration and starts up the node like that.
""".
t_action_without_connector(TCConfig) ->
    ActionConfig = emqx_bridge_schema_testlib:mqtt_action_config(#{
        <<"connector">> => <<"a">>
    }),
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [
            emqx_bridge_mqtt,
            {emqx_bridge, #{
                config =>
                    #{
                        <<"actions">> =>
                            #{
                                <<"mqtt">> =>
                                    #{<<"a">> => ActionConfig}
                            }
                    }
            }},
            emqx_rule_engine
        ]
    }),
    AuthHeader = global_admin_auth_header(),
    lists:foreach(
        fun(Mode) ->
            ?assertMatch(
                {200, _},
                get_data_integration(Mode, prometheus, #{auth_header => AuthHeader}),
                #{mode => Mode}
            )
        end,
        ?PROM_DATA_MODES
    ),
    ok.
