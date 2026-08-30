%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_config.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        emqx_bridge_v2_SUITE:app_specs_without_dashboard(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps).

init_per_testcase(_TestCase, Config) ->
    setup_mocks(),
    ets:new(fun_table_name(), [named_table, public]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete(fun_table_name()),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_action_info:clean_cache(),
    emqx_common_test_helpers:call_janitor(),
    meck:unload(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "`emqx ctl actions` with no sub-command prints usage instead of crashing.".
t_no_subcommand_prints_usage(_Config) ->
    {ok, Output} = capture_ctl(["actions"]),
    ?assertMatch({match, _}, re:run(Output, "actions show")),
    ?assertMatch({match, _}, re:run(Output, "actions status")).

-doc "status reports `connected` for a healthy action and something else for a down connector.".
t_status_reports_connected_and_disconnected(_Config) ->
    ok = create_connector(conn_up, connected),
    ok = create_action(action_up, conn_up),
    ok = create_connector(conn_down, disconnected),
    ok = create_action(action_down, conn_down),
    _ = force_health_check(?global_ns, action_up),
    _ = force_health_check(?global_ns, action_down),

    {ok, Output} = capture_ctl(["actions", "status"]),
    Decoded = emqx_utils_json:decode(Output),
    ?assertEqual(<<"connected">>, status_of(Decoded, action_up)),
    ?assertNotEqual(<<"connected">>, status_of(Decoded, action_down)).

-doc "`--name` selects exactly one action; omitting it lists every action.".
t_name_selects_one_action(_Config) ->
    ok = create_connector(conn1, connected),
    ok = create_action(action1, conn1),
    ok = create_action(action2, conn1),
    _ = force_health_check(?global_ns, action1),
    _ = force_health_check(?global_ns, action2),

    {ok, AllOutput} = capture_ctl(["actions", "status"]),
    ?assertEqual(2, length(emqx_utils_json:decode(AllOutput))),

    {ok, OneOutput} = capture_ctl(["actions", "status", "--name", name_arg(action1)]),
    OneDecoded = emqx_utils_json:decode(OneOutput),
    ?assertEqual(1, length(OneDecoded)),
    ?assertEqual(<<"connected">>, status_of(OneDecoded, action1)).

-doc "A `--name` target that does not exist produces documented, non-crashing output.".
t_missing_name_target(_Config) ->
    {ok, StatusOutput} = capture_ctl(["actions", "status", "--name", "no_such_type:no_such_name"]),
    ?assertEqual([], emqx_utils_json:decode(StatusOutput)),

    {ok, ShowOutput} = capture_ctl(["actions", "show", "--name", "no_such_type:no_such_name"]),
    ?assertEqual(null, emqx_utils_json:decode(ShowOutput)).

-doc "`--ns` selects a namespace; omitting it uses the global namespace.".
t_ns_selects_namespace(_Config) ->
    Namespace = <<"cli_test_ns">>,
    ok = create_connector(Namespace, conn_ns, connected),
    ok = create_action(Namespace, action_ns, conn_ns, #{}),
    _ = force_health_check(Namespace, action_ns),

    {ok, GlobalOutput} = capture_ctl(["actions", "status"]),
    ?assertEqual([], emqx_utils_json:decode(GlobalOutput)),

    {ok, NsOutput} = capture_ctl(["actions", "status", "--ns", binary_to_list(Namespace)]),
    NsDecoded = emqx_utils_json:decode(NsOutput),
    ?assertEqual(<<"connected">>, status_of(NsDecoded, action_ns)).

-doc "`show` output is valid JSON and never leaks a secret configured on the action.".
t_show_output_is_json_and_redacts_secret(_Config) ->
    Secret = <<"Bearer super-secret-token">>,
    ok = create_connector(conn_secret, connected),
    ok = create_action(action_secret, conn_secret, #{<<"Authorization">> => Secret}),
    _ = force_health_check(?global_ns, action_secret),

    {ok, Output} = capture_ctl([
        "actions", "show", "--name", name_arg(action_secret)
    ]),
    ?assertEqual(nomatch, re:run(Output, "super-secret-token", [{capture, none}])),

    Decoded = emqx_utils_json:decode(Output),
    ?assertMatch(#{<<"status">> := <<"connected">>}, Decoded),
    ?assertMatch(
        #{<<"parameters">> := #{<<"headers">> := #{<<"Authorization">> := <<"******">>}}},
        Decoded
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

capture_ctl(Args) ->
    {Result, OutputChunks} =
        emqx_common_test_helpers:capture_io_format(fun() -> emqx_ctl:run_command(Args) end),
    {Result, iolist_to_binary(OutputChunks)}.

status_of(Decoded, ActionName) when is_list(Decoded) ->
    Key = <<(bin(action_type()))/binary, ":", (bin(ActionName))/binary>>,
    lists:foldl(
        fun
            (#{Key := Status}, _Acc) -> Status;
            (_, Acc) -> Acc
        end,
        undefined,
        Decoded
    ).

name_arg(ActionName) ->
    binary_to_list(<<(bin(action_type()))/binary, ":", (bin(ActionName))/binary>>).

create_connector(Name, Status) ->
    create_connector(?global_ns, Name, Status).

create_connector(Namespace, Name, Status) ->
    {ok, _} = emqx_connector:create(Namespace, con_type(), Name, con_config(Status)),
    ok.

create_action(Name, ConnectorName) ->
    create_action(Name, ConnectorName, #{}).

create_action(Name, ConnectorName, Headers) ->
    create_action(?global_ns, Name, ConnectorName, Headers).

create_action(Namespace, Name, ConnectorName, Headers) ->
    {ok, _} = emqx_bridge_v2:create(
        Namespace, actions, action_type(), Name, action_config(ConnectorName, Headers)
    ),
    ok.

force_health_check(Namespace, Name) ->
    emqx_bridge_v2:health_check(Namespace, actions, action_type(), Name).

con_config(Status) ->
    #{
        <<"enable">> => true,
        <<"status">> => atom_to_binary(Status),
        <<"resource_opts">> => #{<<"health_check_interval">> => 100}
    }.

action_config(ConnectorName, Headers) ->
    #{
        <<"connector">> => bin(ConnectorName),
        <<"enable">> => true,
        <<"parameters">> => #{<<"headers">> => Headers},
        <<"resource_opts">> => #{<<"health_check_interval">> => 100, <<"resume_interval">> => 100}
    }.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

fun_table_name() ->
    emqx_bridge_v2_cli_SUITE_fun_table.

con_type() -> cli_test_connector.

con_mod() -> emqx_bridge_v2_cli_test_connector.

action_type() -> cli_test_action.

con_schema() ->
    [
        {
            con_type(),
            hoconsc:mk(
                hoconsc:map(name, hoconsc:ref(?MODULE, connector_config)),
                #{desc => <<"CLI test connector config">>, required => false}
            )
        }
    ].

action_schema() ->
    [
        {
            action_type(),
            hoconsc:mk(
                hoconsc:map(name, hoconsc:ref(?MODULE, action)),
                #{desc => <<"CLI test action config">>, required => false}
            )
        }
    ].

fields(connector_config) ->
    [
        {enable, hoconsc:mk(typerefl:boolean(), #{default => true})},
        {status, hoconsc:mk(hoconsc:enum([connected, disconnected]), #{default => connected})},
        {resource_opts, hoconsc:mk(typerefl:map(), #{default => #{}})}
    ];
fields(action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(hoconsc:ref(?MODULE, action_parameters), #{})
    );
fields(action_parameters) ->
    [
        {headers, hoconsc:mk(map(), #{default => #{}, required => false})}
    ].

setup_mocks() ->
    MeckOpts = [passthrough, no_link, no_history],

    catch meck:new(emqx_connector_schema, MeckOpts),
    meck:expect(emqx_connector_schema, fields, 1, con_schema()),
    meck:expect(emqx_connector_schema, connector_type_to_bridge_types, 1, [con_type()]),

    catch meck:new(emqx_connector_resource, MeckOpts),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, con_mod()),

    catch meck:new(emqx_bridge_v2_schema, MeckOpts),
    meck:expect(emqx_bridge_v2_schema, fields, fun(Struct) ->
        case Struct of
            actions -> action_schema();
            _ -> meck:passthrough([Struct])
        end
    end),
    meck:expect(emqx_bridge_v2_schema, registered_action_types, 0, [action_type()]),

    catch meck:new(emqx_bridge_v2, MeckOpts),
    ActionType = action_type(),
    ActionTypeBin = atom_to_binary(ActionType),
    meck:expect(
        emqx_bridge_v2,
        bridge_v2_type_to_connector_type,
        fun
            (Type) when Type =:= ActionType; Type =:= ActionTypeBin -> con_type();
            (Type) -> meck:passthrough([Type])
        end
    ),
    ok.
