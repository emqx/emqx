%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_http_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(RECORDED_EVENTS_TAB, recorded_actions).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    Apps = emqx_cth_suite:start(
        lists:flatten(
            [
                emqx,
                emqx_conf,
                emqx_rule_engine,
                emqx_schema_validation,
                emqx_management,
                emqx_mgmt_api_test_util:emqx_dashboard(),
                emqx_schema_registry
            ]
        ),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_all_validations(),
    snabbkaffe:stop(),
    reset_all_global_metrics(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(assertIndexOrder(EXPECTED, TOPIC), assert_index_order(EXPECTED, TOPIC, #{line => ?LINE})).

clear_all_validations() ->
    lists:foreach(
        fun(#{name := Name}) ->
            {ok, _} = emqx_schema_validation:delete(Name)
        end,
        emqx_schema_validation:list()
    ).

reset_all_global_metrics() ->
    lists:foreach(
        fun({Name, _}) ->
            emqx_metrics:set(Name, 0)
        end,
        emqx_metrics:all()
    ).

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

validation(Name, Checks) ->
    validation(Name, Checks, _Overrides = #{}).

validation(Name, Checks, Overrides) ->
    Default = #{
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"description">> => <<"my validation">>,
        <<"enable">> => true,
        <<"name">> => Name,
        <<"topics">> => <<"t/+">>,
        <<"strategy">> => <<"all_pass">>,
        <<"failure_action">> => <<"drop">>,
        <<"log_failure">> => #{<<"level">> => <<"warning">>},
        <<"checks">> => Checks
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

sql_check() ->
    sql_check(<<"select * where true">>).

sql_check(SQL) ->
    #{
        <<"type">> => <<"sql">>,
        <<"sql">> => SQL
    }.

schema_check(Type, SerdeName) ->
    schema_check(Type, SerdeName, _Overrides = #{}).

schema_check(Type, SerdeName, Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => emqx_utils_conv:bin(Type),
            <<"schema">> => SerdeName
        },
        Overrides
    ).

api_root() -> "schema_validations".

simplify_result(Res) ->
    case Res of
        {error, {{_, Status, _}, _, Body}} ->
            {Status, Body};
        {ok, {{_, Status, _}, _, Body}} ->
            {Status, Body}
    end.

list() ->
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    Res = request(get, Path, _Params = []),
    ct:pal("list result:\n  ~p", [Res]),
    simplify_result(Res).

lookup(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name]),
    Res = request(get, Path, _Params = []),
    ct:pal("lookup ~s result:\n  ~p", [Name, Res]),
    simplify_result(Res).

insert(Params) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    Res = request(post, Path, Params),
    ct:pal("insert result:\n  ~p", [Res]),
    simplify_result(Res).

update(Params) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    Res = request(put, Path, Params),
    ct:pal("update result:\n  ~p", [Res]),
    simplify_result(Res).

delete(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name]),
    Res = request(delete, Path, _Params = []),
    ct:pal("delete result:\n  ~p", [Res]),
    simplify_result(Res).

reorder(Order) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "reorder"]),
    Params = #{<<"order">> => Order},
    Res = request(post, Path, Params),
    ct:pal("reorder result:\n  ~p", [Res]),
    simplify_result(Res).

enable(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name, "enable", "true"]),
    Res = request(post, Path, _Params = []),
    ct:pal("enable result:\n  ~p", [Res]),
    simplify_result(Res).

disable(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name, "enable", "false"]),
    Res = request(post, Path, _Params = []),
    ct:pal("disable result:\n  ~p", [Res]),
    simplify_result(Res).

get_metrics(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name, "metrics"]),
    Res = request(get, Path, _Params = []),
    ct:pal("get metrics result:\n  ~p", [Res]),
    simplify_result(Res).

reset_metrics(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "validation", Name, "metrics", "reset"]),
    Res = request(post, Path, _Params = []),
    ct:pal("reset metrics result:\n  ~p", [Res]),
    simplify_result(Res).

all_metrics() ->
    Path = emqx_mgmt_api_test_util:api_path(["metrics"]),
    Res = request(get, Path, _Params = []),
    ct:pal("all metrics result:\n  ~p", [Res]),
    simplify_result(Res).

monitor_metrics() ->
    Path = emqx_mgmt_api_test_util:api_path(["monitor"]),
    Res = request(get, Path, _Params = []),
    ct:pal("monitor metrics result:\n  ~p", [Res]),
    simplify_result(Res).

upload_backup(BackupFilePath) ->
    Path = emqx_mgmt_api_test_util:api_path(["data", "files"]),
    Res = emqx_mgmt_api_test_util:upload_request(
        Path,
        BackupFilePath,
        "filename",
        <<"application/octet-stream">>,
        [],
        emqx_mgmt_api_test_util:auth_header_()
    ),
    simplify_result(Res).

export_backup() ->
    Path = emqx_mgmt_api_test_util:api_path(["data", "export"]),
    Res = request(post, Path, {raw, <<>>}),
    simplify_result(Res).

import_backup(BackupName) ->
    Path = emqx_mgmt_api_test_util:api_path(["data", "import"]),
    Body = #{<<"filename">> => unicode:characters_to_binary(BackupName)},
    Res = request(post, Path, Body),
    simplify_result(Res).

connect(ClientId) ->
    connect(ClientId, _IsPersistent = false).

connect(ClientId, IsPersistent) ->
    Properties = emqx_utils_maps:put_if(#{}, 'Session-Expiry-Interval', 30, IsPersistent),
    {ok, Client} = emqtt:start_link([
        {clean_start, true},
        {clientid, ClientId},
        {properties, Properties},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Client),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

publish(Client, Topic, Payload) ->
    publish(Client, Topic, Payload, _QoS = 0).

publish(Client, Topic, {raw, Payload}, QoS) ->
    case emqtt:publish(Client, Topic, Payload, QoS) of
        ok -> ok;
        {ok, _} -> ok;
        Err -> Err
    end;
publish(Client, Topic, Payload, QoS) ->
    case emqtt:publish(Client, Topic, emqx_utils_json:encode(Payload), QoS) of
        ok -> ok;
        {ok, _} -> ok;
        Err -> Err
    end.

json_valid_payloads() ->
    [
        #{i => 10, s => <<"s">>},
        #{i => 10}
    ].

json_invalid_payloads() ->
    [
        #{i => <<"wrong type">>},
        #{x => <<"unknown property">>}
    ].

json_create_serde(SerdeName) ->
    Source = #{
        type => object,
        properties => #{
            i => #{type => integer},
            s => #{type => string}
        },
        required => [<<"i">>],
        additionalProperties => false
    },
    Schema = #{type => json, source => emqx_utils_json:encode(Source)},
    ok = emqx_schema_registry:add_schema(SerdeName, Schema),
    on_exit(fun() -> ok = emqx_schema_registry:delete_schema(SerdeName) end),
    ok.

avro_valid_payloads(SerdeName) ->
    lists:map(
        fun(Payload) -> emqx_schema_registry_serde:encode(SerdeName, Payload) end,
        [
            #{i => 10, s => <<"s">>},
            #{i => 10}
        ]
    ).

avro_invalid_payloads() ->
    [
        emqx_utils_json:encode(#{i => 10, s => <<"s">>}),
        <<"">>
    ].

avro_create_serde(SerdeName) ->
    Source = #{
        type => record,
        name => <<"test">>,
        namespace => <<"emqx.com">>,
        fields => [
            #{name => <<"i">>, type => <<"int">>},
            #{name => <<"s">>, type => [<<"null">>, <<"string">>], default => <<"null">>}
        ]
    },
    Schema = #{type => avro, source => emqx_utils_json:encode(Source)},
    ok = emqx_schema_registry:add_schema(SerdeName, Schema),
    on_exit(fun() -> ok = emqx_schema_registry:delete_schema(SerdeName) end),
    ok.

protobuf_valid_payloads(SerdeName, MessageType) ->
    lists:map(
        fun(Payload) -> emqx_schema_registry_serde:encode(SerdeName, Payload, [MessageType]) end,
        [
            #{<<"name">> => <<"some name">>, <<"id">> => 10, <<"email">> => <<"emqx@emqx.io">>},
            #{<<"name">> => <<"some name">>, <<"id">> => 10}
        ]
    ).

protobuf_invalid_payloads() ->
    [
        emqx_utils_json:encode(#{name => <<"a">>, id => 10, email => <<"email">>}),
        <<"not protobuf">>
    ].

protobuf_create_serde(SerdeName) ->
    protobuf_upsert_serde(SerdeName, <<"Person">>).

protobuf_upsert_serde(SerdeName, MessageType) ->
    Source = protobuf_source(MessageType),
    Schema = #{type => protobuf, source => Source},
    ok = emqx_schema_registry:add_schema(SerdeName, Schema),
    on_exit(fun() -> ok = emqx_schema_registry:delete_schema(SerdeName) end),
    ok.

protobuf_source(MessageType) ->
    iolist_to_binary(
        [
            <<"message ">>,
            MessageType,
            <<" {\n">>,
            <<
                "     required string name = 1;\n"
                "     required int32 id = 2;\n"
                "     optional string email = 3;\n"
                "  }\n"
                "message UnionValue {\n"
                "    oneof u {\n"
                "        int32  a = 1;\n"
                "        string b = 2;\n"
                "    }\n"
                "}"
            >>
        ]
    ).

%% Checks that the internal order in the registry/index matches expectation.
assert_index_order(ExpectedOrder, Topic, Comment) ->
    ?assertEqual(
        ExpectedOrder,
        [
            N
         || #{name := N} <- emqx_schema_validation_registry:matching_validations(Topic)
        ],
        Comment
    ).

create_failure_tracing_rule() ->
    Params = #{
        enable => true,
        sql => <<"select * from \"$events/schema_validation_failed\" ">>,
        actions => [make_trace_fn_action()]
    },
    create_rule(Params).

create_rule(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Res = request(post, Path, Params),
    ct:pal("create rule result:\n  ~p", [Res]),
    case Res of
        {ok, {{_, 201, _}, _, #{<<"id">> := RuleId}}} ->
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            simplify_result(Res);
        _ ->
            simplify_result(Res)
    end.

make_trace_fn_action() ->
    persistent_term:put({?MODULE, test_pid}, self()),
    Fn = <<(atom_to_binary(?MODULE))/binary, ":trace_rule">>,
    emqx_utils_ets:new(?RECORDED_EVENTS_TAB, [named_table, public, ordered_set]),
    #{function => Fn, args => #{}}.

trace_rule(Data, Envs, _Args) ->
    Now = erlang:monotonic_time(),
    ets:insert(?RECORDED_EVENTS_TAB, {Now, #{data => Data, envs => Envs}}),
    TestPid = persistent_term:get({?MODULE, test_pid}),
    TestPid ! {action, #{data => Data, envs => Envs}},
    ok.

get_traced_failures_from_rule_engine() ->
    ets:tab2list(?RECORDED_EVENTS_TAB).

assert_all_metrics(Line, Expected) ->
    Keys = maps:keys(Expected),
    ?retry(
        100,
        10,
        begin
            Res = all_metrics(),
            ?assertMatch({200, _}, Res),
            {200, [Metrics]} = Res,
            ?assertEqual(Expected, maps:with(Keys, Metrics), #{line => Line})
        end
    ),
    ok.

-define(assertAllMetrics(Expected), assert_all_metrics(?LINE, Expected)).

%% check that dashboard monitor contains the success and failure metric keys
assert_monitor_metrics() ->
    ok = snabbkaffe:start_trace(),
    %% hack: force monitor to flush data now
    {_, {ok, _}} =
        ?wait_async_action(
            emqx_dashboard_monitor ! {sample, erlang:system_time(millisecond)},
            #{?snk_kind := dashboard_monitor_flushed}
        ),
    Res = monitor_metrics(),
    ?assertMatch({200, _}, Res),
    {200, Metrics} = Res,
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    <<"validation_failed">> := _,
                    <<"validation_succeeded">> := _
                },
                M
            )
        end,
        Metrics
    ),
    ok.

normalize_validations(RawValidations) ->
    [
        V#{<<"topics">> := [T]}
     || #{<<"topics">> := T} = V <- RawValidations
    ].

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% Smoke test where we have a single check and `all_pass' strategy.
t_smoke_test(_Config) ->
    Name1 = <<"foo">>,
    Check1 = sql_check(<<"select payload.value as x where x > 15">>),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    lists:foreach(
        fun({QoS, IsPersistent}) ->
            ct:pal("qos = ~b, is persistent = ~p", [QoS, IsPersistent]),
            C = connect(<<"c1">>, IsPersistent),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            {200, _} = update(Validation1),

            ok = publish(C, <<"t/1">>, #{value => 20}, QoS),
            ?assertReceive({publish, _}),
            ok = publish(C, <<"t/1">>, #{value => 10}, QoS),
            ?assertNotReceive({publish, _}),
            ok = publish(C, <<"t/1/a">>, #{value => 10}, QoS),
            ?assertReceive({publish, _}),

            %% test `disconnect' failure action
            Validation2 = validation(Name1, [Check1], #{<<"failure_action">> => <<"disconnect">>}),
            {200, _} = update(Validation2),

            unlink(C),
            PubRes = publish(C, <<"t/1">>, #{value => 0}, QoS),
            case QoS =:= 0 of
                true ->
                    ?assertMatch(ok, PubRes);
                false ->
                    ?assertMatch(
                        {error, {disconnected, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, _}},
                        PubRes
                    )
            end,
            ?assertNotReceive({publish, _}),
            ?assertReceive({disconnected, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, _}),

            ok
        end,
        [
            {QoS, IsPersistent}
         || IsPersistent <- [false, true],
            QoS <- [0, 1, 2]
        ]
    ),

    ok.

t_crud(_Config) ->
    ?assertMatch({200, []}, list()),

    Name1 = <<"foo">>,
    Validation1 = validation(Name1, [sql_check()]),

    ?assertMatch({201, #{<<"name">> := Name1}}, insert(Validation1)),
    ?assertMatch({200, #{<<"name">> := Name1}}, lookup(Name1)),
    ?assertMatch({200, [#{<<"name">> := Name1}]}, list()),
    %% Duplicated name
    ?assertMatch({400, #{<<"code">> := <<"ALREADY_EXISTS">>}}, insert(Validation1)),

    Name2 = <<"bar">>,
    Validation2 = validation(Name2, [sql_check()]),
    %% Not found
    ?assertMatch({404, _}, update(Validation2)),
    ?assertMatch({201, _}, insert(Validation2)),
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}]},
        list()
    ),
    ?assertMatch({200, #{<<"name">> := Name2}}, lookup(Name2)),
    Validation1b = validation(Name1, [
        sql_check(<<"select * where true">>),
        sql_check(<<"select * where false">>)
    ]),
    ?assertMatch({200, _}, update(Validation1b)),
    ?assertMatch({200, #{<<"checks">> := [_, _]}}, lookup(Name1)),
    %% order is unchanged
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}]},
        list()
    ),

    ?assertMatch({204, _}, delete(Name1)),
    ?assertMatch({404, _}, lookup(Name1)),
    ?assertMatch({200, [#{<<"name">> := Name2}]}, list()),
    ?assertMatch({404, _}, update(Validation1)),

    ok.

%% test the "reorder" API
t_reorder(_Config) ->
    %% no validations to reorder
    ?assertMatch({204, _}, reorder([])),

    %% unknown validation
    ?assertMatch(
        {400, #{<<"not_found">> := [<<"nonexistent">>]}},
        reorder([<<"nonexistent">>])
    ),

    Topic = <<"t">>,

    Name1 = <<"foo">>,
    Validation1 = validation(Name1, [sql_check()], #{<<"topics">> => Topic}),
    {201, _} = insert(Validation1),

    %% unknown validation
    ?assertMatch(
        {400, #{
            %% Note: minirest currently encodes empty lists as a "[]" string...
            <<"duplicated">> := "[]",
            <<"not_found">> := [<<"nonexistent">>],
            <<"not_reordered">> := [Name1]
        }},
        reorder([<<"nonexistent">>])
    ),

    %% repeated validations
    ?assertMatch(
        {400, #{
            <<"not_found">> := "[]",
            <<"duplicated">> := [Name1],
            <<"not_reordered">> := "[]"
        }},
        reorder([Name1, Name1])
    ),

    %% mixed known, unknown and repeated validations
    ?assertMatch(
        {400, #{
            <<"not_found">> := [<<"nonexistent">>],
            <<"duplicated">> := [Name1],
            %% Note: minirest currently encodes empty lists as a "[]" string...
            <<"not_reordered">> := "[]"
        }},
        reorder([Name1, <<"nonexistent">>, <<"nonexistent">>, Name1])
    ),

    ?assertMatch({204, _}, reorder([Name1])),
    ?assertMatch({200, [#{<<"name">> := Name1}]}, list()),
    ?assertIndexOrder([Name1], Topic),

    Name2 = <<"bar">>,
    Validation2 = validation(Name2, [sql_check()], #{<<"topics">> => Topic}),
    {201, _} = insert(Validation2),
    Name3 = <<"baz">>,
    Validation3 = validation(Name3, [sql_check()], #{<<"topics">> => Topic}),
    {201, _} = insert(Validation3),

    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}, #{<<"name">> := Name3}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name2, Name3], Topic),

    %% Doesn't mention all validations
    ?assertMatch(
        {400, #{
            %% Note: minirest currently encodes empty lists as a "[]" string...
            <<"not_found">> := "[]",
            <<"not_reordered">> := [_, _]
        }},
        reorder([Name1])
    ),
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}, #{<<"name">> := Name3}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name2, Name3], Topic),

    ?assertMatch({204, _}, reorder([Name3, Name2, Name1])),
    ?assertMatch(
        {200, [#{<<"name">> := Name3}, #{<<"name">> := Name2}, #{<<"name">> := Name1}]},
        list()
    ),
    ?assertIndexOrder([Name3, Name2, Name1], Topic),

    ?assertMatch({204, _}, reorder([Name1, Name3, Name2])),
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name3}, #{<<"name">> := Name2}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name3, Name2], Topic),

    ok.

t_enable_disable_via_update(_Config) ->
    Topic = <<"t">>,

    Name1 = <<"foo">>,
    AlwaysFailCheck = sql_check(<<"select * where false">>),
    Validation1 = validation(Name1, [AlwaysFailCheck], #{<<"topics">> => Topic}),

    {201, _} = insert(Validation1#{<<"enable">> => false}),
    ?assertIndexOrder([], Topic),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, Topic),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    {200, _} = update(Validation1#{<<"enable">> => true}),
    ?assertIndexOrder([Name1], Topic),

    ok = publish(C, Topic, #{}),
    ?assertNotReceive({publish, _}),

    {200, _} = update(Validation1#{<<"enable">> => false}),
    ?assertIndexOrder([], Topic),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    %% Test index after delete; ensure it's in the index before
    {200, _} = update(Validation1#{<<"enable">> => true}),
    ?assertIndexOrder([Name1], Topic),
    {204, _} = delete(Name1),
    ?assertIndexOrder([], Topic),

    ok.

t_log_failure_none(_Config) ->
    ?check_trace(
        begin
            Name1 = <<"foo">>,
            AlwaysFailCheck = sql_check(<<"select * where false">>),
            Validation1 = validation(
                Name1,
                [AlwaysFailCheck],
                #{<<"log_failure">> => #{<<"level">> => <<"none">>}}
            ),

            {201, _} = insert(Validation1),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            ok = publish(C, <<"t/1">>, #{}),
            ?assertNotReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{log_level := none}], ?of_kind(schema_validation_failed, Trace)),
            ok
        end
    ),
    ok.

t_action_ignore(_Config) ->
    Name1 = <<"foo">>,
    ?check_trace(
        begin
            AlwaysFailCheck = sql_check(<<"select * where false">>),
            Validation1 = validation(
                Name1,
                [AlwaysFailCheck],
                #{<<"failure_action">> => <<"ignore">>}
            ),

            {201, _} = insert(Validation1),

            {201, _} = create_failure_tracing_rule(),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            ok = publish(C, <<"t/1">>, #{}),
            ?assertReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{action := ignore}], ?of_kind(schema_validation_failed, Trace)),
            ok
        end
    ),
    ?assertMatch(
        [{_, #{data := #{validation := Name1, event := 'schema.validation_failed'}}}],
        get_traced_failures_from_rule_engine()
    ),
    ok.

t_enable_disable_via_api_endpoint(_Config) ->
    Topic = <<"t">>,

    Name1 = <<"foo">>,
    AlwaysFailCheck = sql_check(<<"select * where false">>),
    Validation1 = validation(Name1, [AlwaysFailCheck], #{<<"topics">> => Topic}),

    {201, _} = insert(Validation1),
    ?assertIndexOrder([Name1], Topic),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, Topic),

    ok = publish(C, Topic, #{}),
    ?assertNotReceive({publish, _}),

    %% already enabled
    {204, _} = enable(Name1),
    ?assertIndexOrder([Name1], Topic),
    ?assertMatch({200, #{<<"enable">> := true}}, lookup(Name1)),

    ok = publish(C, Topic, #{}),
    ?assertNotReceive({publish, _}),

    {204, _} = disable(Name1),
    ?assertIndexOrder([], Topic),
    ?assertMatch({200, #{<<"enable">> := false}}, lookup(Name1)),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    %% already disabled
    {204, _} = disable(Name1),
    ?assertIndexOrder([], Topic),
    ?assertMatch({200, #{<<"enable">> := false}}, lookup(Name1)),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    %% Re-enable
    {204, _} = enable(Name1),
    ?assertIndexOrder([Name1], Topic),
    ?assertMatch({200, #{<<"enable">> := true}}, lookup(Name1)),

    ok = publish(C, Topic, #{}),
    ?assertNotReceive({publish, _}),

    ok.

t_metrics(_Config) ->
    %% extra validation that always passes at the head to check global metrics
    Name0 = <<"bar">>,
    Check0 = sql_check(<<"select 1 where true">>),
    Validation0 = validation(Name0, [Check0]),
    {201, _} = insert(Validation0),

    Name1 = <<"foo">>,
    Check1 = sql_check(<<"select payload.x as x where x > 5">>),
    Validation1 = validation(Name1, [Check1]),

    %% Non existent
    ?assertMatch({404, _}, get_metrics(Name1)),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.validation_failed">> => 0,
        <<"messages.validation_succeeded">> => 0
    }),

    {201, _} = insert(Validation1),

    ?assertMatch(
        {200, #{
            <<"metrics">> :=
                #{
                    <<"matched">> := 0,
                    <<"succeeded">> := 0,
                    <<"failed">> := 0,
                    <<"rate">> := _,
                    <<"rate_last5m">> := _,
                    <<"rate_max">> := _
                },
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> := #{
                            <<"matched">> := 0,
                            <<"succeeded">> := 0,
                            <<"failed">> := 0,
                            <<"rate">> := _,
                            <<"rate_last5m">> := _,
                            <<"rate_max">> := _
                        }
                    }
                ]
        }},
        get_metrics(Name1)
    ),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.validation_failed">> => 0,
        <<"messages.validation_succeeded">> => 0
    }),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{x => 10}),
    ?assertReceive({publish, _}),

    ?retry(
        100,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> :=
                    #{
                        <<"matched">> := 1,
                        <<"succeeded">> := 1,
                        <<"failed">> := 0
                    },
                <<"node_metrics">> :=
                    [
                        #{
                            <<"node">> := _,
                            <<"metrics">> := #{
                                <<"matched">> := 1,
                                <<"succeeded">> := 1,
                                <<"failed">> := 0
                            }
                        }
                    ]
            }},
            get_metrics(Name1)
        )
    ),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.validation_failed">> => 0,
        <<"messages.validation_succeeded">> => 1
    }),

    ok = publish(C, <<"t/1">>, #{x => 5}),
    ?assertNotReceive({publish, _}),

    ?retry(
        100,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> :=
                    #{
                        <<"matched">> := 2,
                        <<"succeeded">> := 1,
                        <<"failed">> := 1
                    },
                <<"node_metrics">> :=
                    [
                        #{
                            <<"node">> := _,
                            <<"metrics">> := #{
                                <<"matched">> := 2,
                                <<"succeeded">> := 1,
                                <<"failed">> := 1
                            }
                        }
                    ]
            }},
            get_metrics(Name1)
        )
    ),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.validation_failed">> => 1,
        <<"messages.validation_succeeded">> => 1
    }),

    ?assertMatch({204, _}, reset_metrics(Name1)),
    ?retry(
        100,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> :=
                    #{
                        <<"matched">> := 0,
                        <<"succeeded">> := 0,
                        <<"failed">> := 0,
                        <<"rate">> := _,
                        <<"rate_last5m">> := _,
                        <<"rate_max">> := _
                    },
                <<"node_metrics">> :=
                    [
                        #{
                            <<"node">> := _,
                            <<"metrics">> := #{
                                <<"matched">> := 0,
                                <<"succeeded">> := 0,
                                <<"failed">> := 0,
                                <<"rate">> := _,
                                <<"rate_last5m">> := _,
                                <<"rate_max">> := _
                            }
                        }
                    ]
            }},
            get_metrics(Name1)
        )
    ),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.validation_failed">> => 1,
        <<"messages.validation_succeeded">> => 1
    }),

    %% updating a validation resets its metrics
    ok = publish(C, <<"t/1">>, #{x => 5}),
    ?assertNotReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{x => 10}),
    ?assertReceive({publish, _}),
    ?retry(
        100,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> :=
                    #{
                        <<"matched">> := 2,
                        <<"succeeded">> := 1,
                        <<"failed">> := 1
                    },
                <<"node_metrics">> :=
                    [
                        #{
                            <<"node">> := _,
                            <<"metrics">> := #{
                                <<"matched">> := 2,
                                <<"succeeded">> := 1,
                                <<"failed">> := 1
                            }
                        }
                    ]
            }},
            get_metrics(Name1)
        )
    ),
    {200, _} = update(Validation1),
    ?retry(
        100,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> :=
                    #{
                        <<"matched">> := 0,
                        <<"succeeded">> := 0,
                        <<"failed">> := 0
                    },
                <<"node_metrics">> :=
                    [
                        #{
                            <<"node">> := _,
                            <<"metrics">> := #{
                                <<"matched">> := 0,
                                <<"succeeded">> := 0,
                                <<"failed">> := 0
                            }
                        }
                    ]
            }},
            get_metrics(Name1)
        )
    ),

    assert_monitor_metrics(),

    ok.

t_duplicated_schema_checks(_Config) ->
    Name1 = <<"foo">>,
    SerdeName = <<"myserde">>,
    Check = schema_check(json, SerdeName),
    json_create_serde(SerdeName),

    Validation1 = validation(Name1, [Check, sql_check(), Check]),
    ?assertMatch({400, _}, insert(Validation1)),

    Validation2 = validation(Name1, [Check, sql_check()]),
    ?assertMatch({201, _}, insert(Validation2)),

    ?assertMatch({400, _}, update(Validation1)),

    ok.

%% Check the `all_pass' strategy
t_all_pass(_Config) ->
    Name1 = <<"foo">>,
    Check1 = sql_check(<<"select payload.x as x where x > 5">>),
    Check2 = sql_check(<<"select payload.x as x where x > 10">>),
    Validation1 = validation(Name1, [Check1, Check2], #{<<"strategy">> => <<"all_pass">>}),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
    ok = publish(C, <<"t/1">>, #{x => 0}),
    ?assertNotReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{x => 7}),
    ?assertNotReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{x => 11}),
    ?assertReceive({publish, _}),

    ok.

%% Check the `any_pass' strategy
t_any_pass(_Config) ->
    Name1 = <<"foo">>,
    Check1 = sql_check(<<"select payload.x as x where x > 5">>),
    Check2 = sql_check(<<"select payload.x as x where x > 10">>),
    Validation1 = validation(Name1, [Check1, Check2], #{<<"strategy">> => <<"any_pass">>}),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{x => 11}),
    ?assertReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{x => 7}),
    ?assertReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{x => 0}),
    ?assertNotReceive({publish, _}),

    ok.

%% Checks that multiple validations are run in order.
t_multiple_validations(_Config) ->
    {201, _} = create_failure_tracing_rule(),

    Name1 = <<"foo">>,
    Check1 = sql_check(<<"select payload.x as x, payload.y as y where x > 10 or y > 0">>),
    Validation1 = validation(Name1, [Check1], #{<<"failure_action">> => <<"drop">>}),
    {201, _} = insert(Validation1),

    Name2 = <<"bar">>,
    Check2 = sql_check(<<"select payload.x as x where x > 5">>),
    Validation2 = validation(Name2, [Check2], #{<<"failure_action">> => <<"disconnect">>}),
    {201, _} = insert(Validation2),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{x => 11, y => 1}),
    ?assertReceive({publish, _}),
    %% Barred by `Name1'
    ok = publish(C, <<"t/1">>, #{x => 7, y => 0}),
    ?assertNotReceive({publish, _}),
    ?assertNotReceive({disconnected, _, _}),
    %% Barred by `Name2'
    unlink(C),
    ok = publish(C, <<"t/1">>, #{x => 0, y => 1}),
    ?assertNotReceive({publish, _}),
    ?assertReceive({disconnected, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, _}),

    ?assertMatch(
        [
            {_, #{data := #{validation := Name1, event := 'schema.validation_failed'}}},
            {_, #{data := #{validation := Name2, event := 'schema.validation_failed'}}}
        ],
        get_traced_failures_from_rule_engine()
    ),

    ok.

%% Test that we validate schema registry serde existency when using the HTTP API.
t_schema_check_non_existent_serde(_Config) ->
    SerdeName = <<"idontexist">>,
    Name1 = <<"foo">>,

    Check1 = schema_check(json, SerdeName),
    Validation1 = validation(Name1, [Check1]),
    ?assertMatch({400, _}, insert(Validation1)),

    Check2 = schema_check(avro, SerdeName),
    Validation2 = validation(Name1, [Check2]),
    ?assertMatch({400, _}, insert(Validation2)),

    MessageType = <<"idontexisteither">>,
    Check3 = schema_check(protobuf, SerdeName, #{<<"message_type">> => MessageType}),
    Validation3 = validation(Name1, [Check3]),
    ?assertMatch({400, _}, insert(Validation3)),

    protobuf_create_serde(SerdeName),
    %% Still fails because reference message type doesn't exist.
    ?assertMatch({400, _}, insert(Validation3)),

    ok.

%% Test that we validate schema registry serde existency when loading configs.
t_schema_check_non_existent_serde_load_config(_Config) ->
    Name1 = <<"1">>,
    SerdeName1 = <<"serde1">>,
    MessageType1 = <<"mt">>,
    Check1A = schema_check(protobuf, SerdeName1, #{<<"message_type">> => MessageType1}),
    Validation1A = validation(Name1, [Check1A]),
    protobuf_upsert_serde(SerdeName1, MessageType1),
    {201, _} = insert(Validation1A),
    Name2 = <<"2">>,
    SerdeName2 = <<"serde2">>,
    Check2A = schema_check(json, SerdeName2),
    Validation2A = validation(Name2, [Check2A]),
    json_create_serde(SerdeName2),
    {201, _} = insert(Validation2A),

    %% Config to load
    %% Will replace existing config
    MissingMessageType = <<"missing_mt">>,
    Check1B = schema_check(protobuf, SerdeName1, #{<<"message_type">> => MissingMessageType}),
    Validation1B = validation(Name1, [Check1B]),

    %% Will replace existing config
    MissingSerdeName1 = <<"missing1">>,
    Check2B = schema_check(json, MissingSerdeName1),
    Validation2B = validation(Name2, [Check2B]),

    %% New validation; should be appended
    Name3 = <<"3">>,
    MissingSerdeName2 = <<"missing2">>,
    Check3 = schema_check(avro, MissingSerdeName2),
    Validation3 = validation(Name3, [Check3]),

    ConfRootBin = <<"schema_validation">>,
    ConfigToLoad1 = #{
        ConfRootBin => #{
            <<"validations">> => [Validation1B, Validation2B, Validation3]
        }
    },
    ConfigToLoadBin1 = iolist_to_binary(hocon_pp:do(ConfigToLoad1, #{})),
    %% Merge
    ResMerge = emqx_conf_cli:load_config(ConfigToLoadBin1, #{mode => merge}),
    ?assertMatch({error, _}, ResMerge),
    {error, ErrorMessage1} = ResMerge,
    ?assertEqual(match, re:run(ErrorMessage1, <<"missing_schemas">>, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage1, MissingSerdeName1, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage1, MissingSerdeName2, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage1, MissingMessageType, [{capture, none}])),

    %% Replace
    ResReplace = emqx_conf_cli:load_config(ConfigToLoadBin1, #{mode => replace}),
    ?assertMatch({error, _}, ResReplace),
    {error, ErrorMessage2} = ResReplace,
    ?assertEqual(match, re:run(ErrorMessage2, <<"missing_schemas">>, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage2, MissingSerdeName1, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage2, MissingSerdeName2, [{capture, none}])),
    ?assertEqual(match, re:run(ErrorMessage2, MissingMessageType, [{capture, none}])),

    ok.

t_schema_check_json(_Config) ->
    SerdeName = <<"myserde">>,
    json_create_serde(SerdeName),

    Name1 = <<"foo">>,
    Check1 = schema_check(json, SerdeName),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, Payload),
            ?assertReceive({publish, _})
        end,
        json_valid_payloads()
    ),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, Payload),
            ?assertNotReceive({publish, _})
        end,
        json_invalid_payloads()
    ),

    ok.

t_schema_check_avro(_Config) ->
    SerdeName = <<"myserde">>,
    avro_create_serde(SerdeName),

    Name1 = <<"foo">>,
    Check1 = schema_check(avro, SerdeName),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertReceive({publish, _})
        end,
        avro_valid_payloads(SerdeName)
    ),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        avro_invalid_payloads()
    ),

    ok.

t_schema_check_protobuf(_Config) ->
    SerdeName = <<"myserde">>,
    MessageType = <<"Person">>,
    protobuf_create_serde(SerdeName),

    Name1 = <<"foo">>,
    Check1 = schema_check(protobuf, SerdeName, #{<<"message_type">> => MessageType}),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, MessageType)
    ),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_invalid_payloads()
    ),

    %% Bad config: unknown message name
    %% Schema updated to use another message type after validation was created
    OtherMessageType = <<"NewPersonType">>,
    protobuf_upsert_serde(SerdeName, OtherMessageType),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, OtherMessageType)
    ),

    ok.

%% Tests that restoring a backup config works.
%%   * Existing validations (identified by `name') are left untouched.
%%   * No validations are removed.
%%   * New validations are appended to the existing list.
%%   * Existing validations are not reordered.
t_import_config_backup(_Config) ->
    %% Setup backup file.

    %% Will clash with existing validation; different order.
    Name2 = <<"2">>,
    Check2B = sql_check(<<"select 2 where false">>),
    Validation2B = validation(Name2, [Check2B]),
    {201, _} = insert(Validation2B),

    %% Will clash with existing validation.
    Name1 = <<"1">>,
    Check1B = sql_check(<<"select 1 where false">>),
    Validation1B = validation(Name1, [Check1B]),
    {201, _} = insert(Validation1B),

    %% New validation; should be appended
    Name4 = <<"4">>,
    Check4 = sql_check(<<"select 4 where true">>),
    Validation4 = validation(Name4, [Check4]),
    {201, _} = insert(Validation4),

    {200, #{<<"filename">> := BackupName}} = export_backup(),

    %% Clear this setup and pretend we have other data to begin with.
    clear_all_validations(),
    {200, []} = list(),

    Check1A = sql_check(<<"select 1 where true">>),
    Validation1A = validation(Name1, [Check1A]),
    {201, _} = insert(Validation1A),

    Check2A = sql_check(<<"select 2 where true">>),
    Validation2A = validation(Name2, [Check2A]),
    {201, _} = insert(Validation2A),

    Name3 = <<"3">>,
    Check3 = sql_check(<<"select 3 where true">>),
    Validation3 = validation(Name3, [Check3]),
    {201, _} = insert(Validation3),

    {204, _} = import_backup(BackupName),

    ExpectedValidations = normalize_validations([
        Validation1A,
        Validation2A,
        Validation3,
        Validation4
    ]),
    ?assertMatch({200, ExpectedValidations}, list()),
    ?assertIndexOrder([Name1, Name2, Name3, Name4], <<"t/a">>),

    ok.

%% Tests that importing configurations from the CLI interface work.
t_load_config(_Config) ->
    Name1 = <<"1">>,
    Check1A = sql_check(<<"select 1 where true">>),
    Validation1A = validation(Name1, [Check1A]),
    {201, _} = insert(Validation1A),

    Name2 = <<"2">>,
    Check2A = sql_check(<<"select 2 where true">>),
    Validation2A = validation(Name2, [Check2A]),
    {201, _} = insert(Validation2A),

    Name3 = <<"3">>,
    Check3 = sql_check(<<"select 3 where true">>),
    Validation3 = validation(Name3, [Check3]),
    {201, _} = insert(Validation3),

    %% Config to load
    %% Will replace existing config
    Check2B = sql_check(<<"select 2 where false">>),
    Validation2B = validation(Name2, [Check2B]),

    %% Will replace existing config
    Check1B = sql_check(<<"select 1 where false">>),
    Validation1B = validation(Name1, [Check1B]),

    %% New validation; should be appended
    Name4 = <<"4">>,
    Check4 = sql_check(<<"select 4 where true">>),
    Validation4 = validation(Name4, [Check4]),

    ConfRootBin = <<"schema_validation">>,
    ConfigToLoad1 = #{
        ConfRootBin => #{
            <<"validations">> => [Validation2B, Validation1B, Validation4]
        }
    },
    ConfigToLoadBin1 = iolist_to_binary(hocon_pp:do(ConfigToLoad1, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoadBin1, #{mode => merge})),
    ExpectedValidations1 = normalize_validations([
        Validation1A,
        Validation2A,
        Validation3,
        Validation4
    ]),
    ?assertMatch(
        #{
            ConfRootBin := #{
                <<"validations">> := ExpectedValidations1
            }
        },
        emqx_conf_cli:get_config(<<"schema_validation">>)
    ),
    ?assertIndexOrder([Name1, Name2, Name3, Name4], <<"t/a">>),

    %% Replace
    Check4B = sql_check(<<"select 4, true where true">>),
    Validation4B = validation(Name4, [Check4B]),

    Name5 = <<"5">>,
    Check5 = sql_check(<<"select 5 where true">>),
    Validation5 = validation(Name5, [Check5]),

    ConfigToLoad2 = #{
        ConfRootBin => #{<<"validations">> => [Validation4B, Validation3, Validation5]}
    },
    ConfigToLoadBin2 = iolist_to_binary(hocon_pp:do(ConfigToLoad2, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoadBin2, #{mode => replace})),
    ExpectedValidations2 = normalize_validations([Validation4B, Validation3, Validation5]),
    ?assertMatch(
        #{
            ConfRootBin := #{
                <<"validations">> := ExpectedValidations2
            }
        },
        emqx_conf_cli:get_config(<<"schema_validation">>)
    ),
    ?assertIndexOrder([Name4, Name3, Name5], <<"t/a">>),

    ok.

%% Checks that the republish action failure metric increases when schema validation fails
%% for an outgoing message.  Though this is arguably more appropriate as an
%% `emqx_rule_runtime' test case, it's simpler to setup the conditions here.
t_republish_action_failure(_Config) ->
    ?check_trace(
        begin
            Name1 = <<"1">>,
            %% Always fails
            Check1A = sql_check(<<"select 1 where false">>),
            Validation1A = validation(Name1, [Check1A]),
            {201, _} = insert(Validation1A),

            RuleTopic = <<"some/topic">>,
            Params = #{
                enable => true,
                sql => iolist_to_binary([
                    <<"select * from \"">>,
                    RuleTopic,
                    <<"\"">>
                ]),
                actions => [
                    #{
                        function => <<"republish">>,
                        args =>
                            #{
                                <<"mqtt_properties">> => #{},
                                <<"payload">> => <<"aaa">>,
                                <<"qos">> => 0,
                                <<"retain">> => false,
                                <<"topic">> => <<"t/republished">>,
                                <<"user_properties">> => <<>>,
                                <<"direct_dispatch">> => false
                            }
                    }
                ]
            },
            {201, #{<<"id">> := RuleId}} = create_rule(Params),
            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
            ok = publish(C, RuleTopic, #{}),
            ?assertNotReceive({publish, _}),

            ?assertMatch(
                #{
                    matched := 1,
                    failed := 0,
                    passed := 1,
                    'actions.total' := 1,
                    'actions.success' := 0,
                    'actions.failed' := 1,
                    'actions.failed.unknown' := 1,
                    'actions.failed.out_of_service' := 0
                },
                emqx_metrics_worker:get_counters(rule_metrics, RuleId)
            ),

            %% `publish' return type is different when failure action is `disconnect'
            Validation1B = validation(Name1, [Check1A], #{<<"failure_action">> => <<"disconnect">>}),
            {200, _} = update(Validation1B),

            ok = publish(C, RuleTopic, #{}),
            ?assertNotReceive({publish, _}),

            ?assertMatch(
                #{
                    matched := 2,
                    failed := 0,
                    passed := 2,
                    'actions.total' := 2,
                    'actions.success' := 0,
                    'actions.failed' := 2,
                    'actions.failed.unknown' := 2,
                    'actions.failed.out_of_service' := 0
                },
                emqx_metrics_worker:get_counters(rule_metrics, RuleId)
            ),

            ok
        end,
        []
    ),
    ok.
