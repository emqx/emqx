%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_http_api_SUITE).

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
    Apps = emqx_cth_suite:start(
        lists:flatten(
            [
                emqx,
                emqx_conf,
                emqx_message_transformation,
                emqx_management,
                emqx_mgmt_api_test_util:emqx_dashboard(),
                emqx_schema_registry,
                emqx_rule_engine
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
    clear_all_transformations(),
    snabbkaffe:stop(),
    reset_all_global_metrics(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(assertIndexOrder(EXPECTED, TOPIC), assert_index_order(EXPECTED, TOPIC, #{line => ?LINE})).

bin(X) -> emqx_utils_conv:bin(X).

clear_all_transformations() ->
    lists:foreach(
        fun(#{name := Name}) ->
            {ok, _} = emqx_message_transformation:delete(Name)
        end,
        emqx_message_transformation:list()
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

transformation(Name, Operations) ->
    transformation(Name, Operations, _Overrides = #{}).

transformation(Name, Operations0, Overrides) ->
    Operations = lists:map(fun normalize_operation/1, Operations0),
    Default = #{
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"description">> => <<"my transformation">>,
        <<"enable">> => true,
        <<"name">> => Name,
        <<"topics">> => [<<"t/+">>],
        <<"failure_action">> => <<"drop">>,
        <<"log_failure">> => #{<<"level">> => <<"warning">>},
        <<"payload_decoder">> => #{<<"type">> => <<"json">>},
        <<"payload_encoder">> => #{<<"type">> => <<"json">>},
        <<"operations">> => Operations
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

normalize_operation({K, V}) ->
    #{<<"key">> => bin(K), <<"value">> => bin(V)}.

dummy_operation() ->
    topic_operation(<<"concat([topic, '/', payload.t])">>).

topic_operation(VariformExpr) ->
    operation(topic, VariformExpr).

operation(Key, VariformExpr) ->
    {Key, VariformExpr}.

json_serde() ->
    #{<<"type">> => <<"json">>}.

avro_serde(SerdeName) ->
    #{<<"type">> => <<"avro">>, <<"schema">> => SerdeName}.

dryrun_input_message() ->
    dryrun_input_message(_Overrides = #{}).

dryrun_input_message(Overrides) ->
    dryrun_input_message(Overrides, _Opts = #{}).

dryrun_input_message(Overrides, Opts) ->
    Encoder = maps:get(encoder, Opts, fun emqx_utils_json:encode/1),
    Defaults = #{
        client_attrs => #{},
        payload => #{},
        qos => 2,
        retain => true,
        topic => <<"t/u/v">>,
        user_property => #{}
    },
    InputMessage0 = emqx_utils_maps:deep_merge(Defaults, Overrides),
    maps:update_with(payload, Encoder, InputMessage0).

api_root() -> "message_transformations".

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
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name]),
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
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name]),
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
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name, "enable", "true"]),
    Res = request(post, Path, _Params = []),
    ct:pal("enable result:\n  ~p", [Res]),
    simplify_result(Res).

disable(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name, "enable", "false"]),
    Res = request(post, Path, _Params = []),
    ct:pal("disable result:\n  ~p", [Res]),
    simplify_result(Res).

get_metrics(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name, "metrics"]),
    Res = request(get, Path, _Params = []),
    ct:pal("get metrics result:\n  ~p", [Res]),
    simplify_result(Res).

reset_metrics(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "transformation", Name, "metrics", "reset"]),
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

dryrun_transformation(Transformation, Message) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "dryrun"]),
    Params = #{transformation => Transformation, message => Message},
    Res = request(post, Path, Params),
    ct:pal("dryrun transformation result:\n  ~p", [Res]),
    simplify_result(Res).

connect(ClientId) ->
    connect(ClientId, _IsPersistent = false).

connect(ClientId, IsPersistent) ->
    connect(ClientId, IsPersistent, _Opts = #{}).

connect(ClientId, IsPersistent, Opts) ->
    StartProps = maps:get(start_props, Opts, #{}),
    Properties0 = maps:get(properties, Opts, #{}),
    Properties = emqx_utils_maps:put_if(Properties0, 'Session-Expiry-Interval', 30, IsPersistent),
    Defaults = #{
        clean_start => true,
        clientid => ClientId,
        properties => Properties,
        proto_ver => v5
    },
    Props = emqx_utils_maps:deep_merge(Defaults, StartProps),
    {ok, Client} = emqtt:start_link(Props),
    {ok, _} = emqtt:connect(Client),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

publish(Client, Topic, Payload) ->
    publish(Client, Topic, Payload, _QoS = 0).

publish(Client, Topic, Payload, QoS) ->
    publish(Client, Topic, Payload, QoS, _Opts = #{}).

publish(Client, Topic, {raw, Payload}, QoS, Opts) ->
    Props = maps:get(props, Opts, #{}),
    case emqtt:publish(Client, Topic, Props, Payload, [{qos, QoS}]) of
        ok -> ok;
        {ok, _} -> ok;
        Err -> Err
    end;
publish(Client, Topic, Payload, QoS, Opts) ->
    Props = maps:get(props, Opts, #{}),
    case emqtt:publish(Client, Topic, Props, emqx_utils_json:encode(Payload), [{qos, QoS}]) of
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
    Source =
        <<
            "message Person {\n"
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
        >>,
    Schema = #{type => protobuf, source => Source},
    ok = emqx_schema_registry:add_schema(SerdeName, Schema),
    on_exit(fun() -> ok = emqx_schema_registry:delete_schema(SerdeName) end),
    ok.

%% Checks that the internal order in the registry/index matches expectation.
assert_index_order(ExpectedOrder, Topic, Comment) ->
    ?assertEqual(
        ExpectedOrder,
        [
            N
         || #{name := N} <- emqx_message_transformation_registry:matching_transformations(Topic)
        ],
        Comment
    ).

create_failure_tracing_rule() ->
    Params = #{
        enable => true,
        sql => <<"select * from \"$events/message_transformation_failed\" ">>,
        actions => [make_trace_fn_action()]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Res = request(post, Path, Params),
    ct:pal("create failure tracing rule result:\n  ~p", [Res]),
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
                    <<"transformation_failed">> := _,
                    <<"transformation_succeeded">> := _
                },
                M
            )
        end,
        Metrics
    ),
    ok.

-define(assertReceiveReturn(PATTERN, TIMEOUT),
    (fun() ->
        receive
            PATTERN = ____Msg0 -> ____Msg0
        after TIMEOUT ->
            error({message_not_received, {line, ?LINE}})
        end
    end)()
).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% Smoke test where we have an example transfomration.
t_smoke_test(_Config) ->
    Name1 = <<"foo">>,
    Operations = [
        operation(qos, <<"payload.q">>),
        operation(topic, <<"concat([topic, '/', payload.t])">>),
        operation(retain, <<"payload.r">>),
        operation(<<"user_property.a">>, <<"payload.u.a">>),
        operation(<<"user_property.copy">>, <<"user_property.original">>),
        operation(<<"payload">>, <<"payload.p.hello">>)
    ],
    Transformation1 = transformation(Name1, Operations),
    {201, _} = insert(Transformation1),

    lists:foreach(
        fun({QoS, IsPersistent}) ->
            ct:pal("qos = ~b, is persistent = ~p", [QoS, IsPersistent]),
            C = connect(<<"c1">>, IsPersistent),
            %% rap => retain as published
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>, [{qos, 2}, {rap, true}]),

            {200, _} = update(Transformation1),

            ok = publish(
                C,
                <<"t/1">>,
                #{
                    p => #{<<"hello">> => <<"world">>},
                    q => QoS,
                    r => true,
                    t => <<"t">>,
                    u => #{a => <<"b">>}
                },
                _QosPub = 0,
                #{props => #{'User-Property' => [{<<"original">>, <<"user_prop">>}]}}
            ),
            ?assertReceive(
                {publish, #{
                    payload := <<"\"world\"">>,
                    qos := QoS,
                    retain := true,
                    topic := <<"t/1/t">>,
                    properties := #{
                        'User-Property' := [
                            {<<"a">>, <<"b">>},
                            {<<"copy">>, <<"user_prop">>},
                            {<<"original">>, <<"user_prop">>}
                        ]
                    }
                }}
            ),
            %% remember to clear retained message
            on_exit(fun() -> emqx:publish(emqx_message:make(<<"t/1/t">>, <<"">>)) end),

            %% test `disconnect' failure action
            Transformation2 = transformation(
                Name1,
                Operations,
                #{<<"failure_action">> => <<"disconnect">>}
            ),
            {200, _} = update(Transformation2),

            unlink(C),
            %% Missing `t' in the payload, so transformation fails
            PubRes = publish(C, <<"t/1">>, #{z => <<"does not matter">>}, QoS),
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

%% A smoke test for a subset of read-only context fields.
%%   * clientid
%%   * id
%%   * node
%%   * peername
%%   * publish_received_at
%%   * username
%%   * timestamp
%%   * pub_props (and specific fields within containing hyphens)
t_smoke_test_2(_Config) ->
    Name1 = <<"foo">>,
    Operations = [
        operation(<<"payload.clientid">>, <<"clientid">>),
        operation(<<"payload.id">>, <<"id">>),
        operation(<<"payload.node">>, <<"node">>),
        operation(<<"payload.peername">>, <<"peername">>),
        operation(<<"payload.publish_received_at">>, <<"publish_received_at">>),
        operation(<<"payload.username">>, <<"username">>),
        operation(<<"payload.flags">>, <<"flags">>),
        operation(<<"payload.timestamp">>, <<"timestamp">>),
        operation(<<"payload.pub_props">>, <<"pub_props">>),
        operation(<<"payload.content_type">>, <<"pub_props.Content-Type">>)
    ],
    Transformation1 = transformation(Name1, Operations),
    {201, _} = insert(Transformation1),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId),
    {ok, _, [_]} = emqtt:subscribe(C1, <<"t/#">>, [{qos, 2}]),
    ok = publish(C1, <<"t/1">>, #{}, _QoS = 0, #{
        props => #{
            'Content-Type' => <<"application/json">>,
            'User-Property' => [{<<"a">>, <<"b">>}]
        }
    }),
    {publish, #{payload := Payload0}} = ?assertReceiveReturn({publish, _}, 1_000),
    NodeBin = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"id">> := <<_/binary>>,
            <<"node">> := NodeBin,
            <<"peername">> := <<"127.0.0.1:", _/binary>>,
            <<"publish_received_at">> := PRAt,
            <<"username">> := <<"">>,
            <<"flags">> := #{<<"dup">> := false, <<"retain">> := false},
            <<"timestamp">> := _,
            <<"pub_props">> := #{
                <<"Content-Type">> := <<"application/json">>,
                <<"User-Property">> := #{<<"a">> := <<"b">>}
            },
            <<"content_type">> := <<"application/json">>
        } when is_integer(PRAt),
        emqx_utils_json:decode(Payload0, [return_maps])
    ),
    %% Reconnect with an username.
    emqtt:stop(C1),
    Username = <<"myusername">>,
    C2 = connect(ClientId, _IsPersistent = false, #{start_props => #{username => Username}}),
    {ok, _, [_]} = emqtt:subscribe(C2, <<"t/#">>, [{qos, 2}]),
    ok = publish(C2, <<"t/1">>, #{}, _QoS = 0, #{
        props => #{
            'Content-Type' => <<"application/json">>,
            'User-Property' => [{<<"a">>, <<"b">>}]
        }
    }),
    {publish, #{payload := Payload1}} = ?assertReceiveReturn({publish, _}, 1_000),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"id">> := <<_/binary>>,
            <<"node">> := NodeBin,
            <<"peername">> := <<"127.0.0.1:", _/binary>>,
            <<"publish_received_at">> := PRAt,
            <<"username">> := Username,
            <<"flags">> := #{<<"dup">> := false, <<"retain">> := false},
            <<"timestamp">> := _,
            <<"pub_props">> := #{
                <<"Content-Type">> := <<"application/json">>,
                <<"User-Property">> := #{<<"a">> := <<"b">>}
            },
            <<"content_type">> := <<"application/json">>
        } when is_integer(PRAt),
        emqx_utils_json:decode(Payload1, [return_maps])
    ),
    ok.

t_crud(_Config) ->
    ?assertMatch({200, []}, list()),

    Topic = <<"t/1">>,
    Name1 = <<"foo">>,
    Transformation1 = transformation(Name1, [dummy_operation()]),

    ?assertMatch({201, #{<<"name">> := Name1}}, insert(Transformation1)),
    ?assertMatch({200, #{<<"name">> := Name1}}, lookup(Name1)),
    ?assertMatch({200, [#{<<"name">> := Name1}]}, list()),
    ?assertIndexOrder([Name1], Topic),
    %% Duplicated name
    ?assertMatch({400, #{<<"code">> := <<"ALREADY_EXISTS">>}}, insert(Transformation1)),

    Name2 = <<"bar">>,
    Transformation2 = transformation(Name2, [dummy_operation()]),
    %% Not found
    ?assertMatch({404, _}, update(Transformation2)),
    ?assertMatch({201, _}, insert(Transformation2)),
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name2], Topic),
    ?assertMatch({200, #{<<"name">> := Name2}}, lookup(Name2)),
    Transformation1b = transformation(Name1, [dummy_operation(), dummy_operation()]),
    ?assertMatch({200, _}, update(Transformation1b)),
    ?assertMatch({200, #{<<"operations">> := [_, _]}}, lookup(Name1)),
    %% order is unchanged
    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name2], Topic),

    ?assertMatch({204, _}, delete(Name1)),
    ?assertMatch({404, _}, lookup(Name1)),
    ?assertMatch({200, [#{<<"name">> := Name2}]}, list()),
    ?assertIndexOrder([Name2], Topic),
    ?assertMatch({404, _}, update(Transformation1)),

    ok.

%% test the "reorder" API
t_reorder(_Config) ->
    %% no transformations to reorder
    ?assertMatch({204, _}, reorder([])),

    %% unknown transformation
    ?assertMatch(
        {400, #{<<"not_found">> := [<<"nonexistent">>]}},
        reorder([<<"nonexistent">>])
    ),

    Topic = <<"t">>,

    Name1 = <<"foo">>,
    Transformation1 = transformation(Name1, [dummy_operation()], #{<<"topics">> => Topic}),
    {201, _} = insert(Transformation1),

    %% unknown transformation
    ?assertMatch(
        {400, #{
            %% Note: minirest currently encodes empty lists as a "[]" string...
            <<"duplicated">> := "[]",
            <<"not_found">> := [<<"nonexistent">>],
            <<"not_reordered">> := [Name1]
        }},
        reorder([<<"nonexistent">>])
    ),

    %% repeated transformations
    ?assertMatch(
        {400, #{
            <<"not_found">> := "[]",
            <<"duplicated">> := [Name1],
            <<"not_reordered">> := "[]"
        }},
        reorder([Name1, Name1])
    ),

    %% mixed known, unknown and repeated transformations
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
    Transformation2 = transformation(Name2, [dummy_operation()], #{<<"topics">> => Topic}),
    {201, _} = insert(Transformation2),
    Name3 = <<"baz">>,
    Transformation3 = transformation(Name3, [dummy_operation()], #{<<"topics">> => Topic}),
    {201, _} = insert(Transformation3),

    ?assertMatch(
        {200, [#{<<"name">> := Name1}, #{<<"name">> := Name2}, #{<<"name">> := Name3}]},
        list()
    ),
    ?assertIndexOrder([Name1, Name2, Name3], Topic),

    %% Doesn't mention all transformations
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
    AlwaysFailOp = topic_operation(<<"missing.var">>),
    Transformation1 = transformation(Name1, [AlwaysFailOp], #{<<"topics">> => Topic}),

    {201, _} = insert(Transformation1#{<<"enable">> => false}),
    ?assertIndexOrder([], Topic),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, Topic),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    {200, _} = update(Transformation1#{<<"enable">> => true}),
    ?assertIndexOrder([Name1], Topic),

    ok = publish(C, Topic, #{}),
    ?assertNotReceive({publish, _}),

    {200, _} = update(Transformation1#{<<"enable">> => false}),
    ?assertIndexOrder([], Topic),

    ok = publish(C, Topic, #{}),
    ?assertReceive({publish, _}),

    %% Test index after delete; ensure it's in the index before
    {200, _} = update(Transformation1#{<<"enable">> => true}),
    ?assertIndexOrder([Name1], Topic),
    {204, _} = delete(Name1),
    ?assertIndexOrder([], Topic),

    ok.

t_log_failure_none(_Config) ->
    ?check_trace(
        begin
            Name1 = <<"foo">>,
            AlwaysFailOp = topic_operation(<<"missing.var">>),
            Transformation1 = transformation(
                Name1,
                [AlwaysFailOp],
                #{<<"log_failure">> => #{<<"level">> => <<"none">>}}
            ),

            {201, _} = insert(Transformation1),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            ok = publish(C, <<"t/1">>, #{}),
            ?assertNotReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{log_level := none} | _], ?of_kind(message_transformation_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_action_ignore(_Config) ->
    Name1 = <<"foo">>,
    ?check_trace(
        begin
            AlwaysFailOp = topic_operation(<<"missing.var">>),
            Transformation1 = transformation(
                Name1,
                [AlwaysFailOp],
                #{<<"failure_action">> => <<"ignore">>}
            ),

            {201, _} = insert(Transformation1),

            {201, _} = create_failure_tracing_rule(),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            ok = publish(C, <<"t/1">>, #{}),
            ?assertReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{action := ignore}], ?of_kind(message_transformation_failed, Trace)),
            ok
        end
    ),
    ?assertMatch(
        [{_, #{data := #{transformation := Name1, event := 'message.transformation_failed'}}}],
        get_traced_failures_from_rule_engine()
    ),
    ok.

t_enable_disable_via_api_endpoint(_Config) ->
    Topic = <<"t">>,

    Name1 = <<"foo">>,
    AlwaysFailOp = topic_operation(<<"missing.var">>),
    Transformation1 = transformation(Name1, [AlwaysFailOp], #{<<"topics">> => Topic}),

    {201, _} = insert(Transformation1),
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
    %% extra transformation that always passes at the head to check global metrics
    Name0 = <<"bar">>,
    Operation0 = topic_operation(<<"concat([topic, '/', 't'])">>),
    Transformation0 = transformation(Name0, [Operation0]),
    {201, _} = insert(Transformation0),

    Name1 = <<"foo">>,
    Operation1 = topic_operation(<<"concat([topic, '/', payload.t])">>),
    Transformation1 = transformation(Name1, [Operation1]),

    %% Non existent
    ?assertMatch({404, _}, get_metrics(Name1)),
    ?assertAllMetrics(#{
        <<"messages.dropped">> => 0,
        <<"messages.transformation_failed">> => 0,
        <<"messages.transformation_succeeded">> => 0
    }),

    {201, _} = insert(Transformation1),

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
        <<"messages.transformation_failed">> => 0,
        <<"messages.transformation_succeeded">> => 0
    }),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{t => <<"s">>}),
    ?assertReceive({publish, #{topic := <<"t/1/t/s">>}}),

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
        <<"messages.transformation_failed">> => 0,
        <<"messages.transformation_succeeded">> => 1
    }),

    ok = publish(C, <<"t/1">>, #{}),
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
        <<"messages.transformation_failed">> => 1,
        <<"messages.transformation_succeeded">> => 1
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
        <<"messages.transformation_failed">> => 1,
        <<"messages.transformation_succeeded">> => 1
    }),

    %% updating a transformation resets its metrics
    ok = publish(C, <<"t/1">>, #{}),
    ?assertNotReceive({publish, _}),
    ok = publish(C, <<"t/1">>, #{t => <<"u">>}),
    ?assertReceive({publish, #{topic := <<"t/1/t/u">>}}),
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
    {200, _} = update(Transformation1),
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

%% Checks that multiple transformations are run in order.
t_multiple_transformations(_Config) ->
    {201, _} = create_failure_tracing_rule(),

    Name1 = <<"foo">>,
    Operation1 = topic_operation(<<"concat([topic, '/', payload.x])">>),
    Transformation1 = transformation(Name1, [Operation1], #{<<"failure_action">> => <<"drop">>}),
    {201, _} = insert(Transformation1),

    Name2 = <<"bar">>,
    Operation2 = topic_operation(<<"concat([topic, '/', payload.y])">>),
    Transformation2 = transformation(Name2, [Operation2], #{
        <<"failure_action">> => <<"disconnect">>
    }),
    {201, _} = insert(Transformation2),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/0">>, #{x => 1, y => 2}),
    ?assertReceive({publish, #{topic := <<"t/0/1/2">>}}),
    %% Barred by `Name1' (missing var)
    ok = publish(C, <<"t/0">>, #{y => 2}),
    ?assertNotReceive({publish, _}),
    ?assertNotReceive({disconnected, _, _}),
    %% Barred by `Name2' (missing var)
    unlink(C),
    ok = publish(C, <<"t/1">>, #{x => 1}),
    ?assertNotReceive({publish, _}),
    ?assertReceive({disconnected, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, _}),

    ?assertMatch(
        [
            {_, #{
                data := #{
                    transformation := Name1,
                    event := 'message.transformation_failed',
                    peername := <<_/binary>>
                }
            }},
            {_, #{
                data := #{
                    transformation := Name2,
                    event := 'message.transformation_failed',
                    peername := <<_/binary>>
                }
            }}
        ],
        get_traced_failures_from_rule_engine()
    ),

    ok.

t_non_existent_serde(_Config) ->
    SerdeName = <<"idontexist">>,
    Name1 = <<"foo">>,
    Operation1 = dummy_operation(),
    PayloadSerde = #{<<"type">> => <<"avro">>, <<"schema">> => SerdeName},
    Transformation1 = transformation(Name1, [Operation1], #{
        <<"payload_decoder">> => PayloadSerde,
        <<"payload_encoder">> => PayloadSerde
    }),
    {201, _} = insert(Transformation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{i => 10, s => <<"s">>}),
    ?assertNotReceive({publish, _}),

    ok.

t_avro(_Config) ->
    SerdeName = <<"myserde">>,
    avro_create_serde(SerdeName),

    Name1 = <<"foo">>,
    Operation1 = operation(<<"payload.s">>, <<"concat(['hello'])">>),
    PayloadSerde = #{<<"type">> => <<"avro">>, <<"schema">> => SerdeName},
    Transformation1 = transformation(Name1, [Operation1], #{
        <<"payload_decoder">> => PayloadSerde,
        <<"payload_encoder">> => PayloadSerde
    }),
    {201, _} = insert(Transformation1),

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
    %% Transformation that produces invalid output according to schema
    Operation2 = operation(<<"payload.i">>, <<"concat(['invalid'])">>),
    Transformation2 = transformation(Name1, [Operation2], #{
        <<"payload_decoder">> => PayloadSerde,
        <<"payload_encoder">> => PayloadSerde
    }),
    {200, _} = update(Transformation2),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        avro_valid_payloads(SerdeName)
    ),

    ok.

t_protobuf(_Config) ->
    SerdeName = <<"myserde">>,
    MessageType = <<"Person">>,
    protobuf_create_serde(SerdeName),

    Name1 = <<"foo">>,
    PayloadSerde = #{
        <<"type">> => <<"protobuf">>,
        <<"schema">> => SerdeName,
        <<"message_type">> => MessageType
    },
    Operation1 = operation(<<"payload.name">>, <<"concat(['hello'])">>),
    Transformation1 = transformation(Name1, [Operation1], #{
        <<"payload_decoder">> => PayloadSerde,
        <<"payload_encoder">> => PayloadSerde
    }),
    {201, _} = insert(Transformation1),

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
    BadPayloadSerde = PayloadSerde#{<<"message_type">> := <<"idontexist">>},
    Transformation2 = transformation(Name1, [Operation1], #{
        <<"payload_decoder">> => BadPayloadSerde,
        <<"payload_encoder">> => BadPayloadSerde
    }),
    {200, _} = update(Transformation2),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, MessageType)
    ),

    %% Transformation that produces invalid output according to schema
    Operation2 = operation(<<"payload.id">>, <<"concat(['invalid'])">>),
    Transformation3 = transformation(Name1, [Operation2], #{
        <<"payload_decoder">> => PayloadSerde,
        <<"payload_encoder">> => PayloadSerde
    }),
    {200, _} = update(Transformation3),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, MessageType)
    ),

    ok.

%% Checks what happens if a wrong transformation chain is used.  In this case, the second
%% transformation attempts to protobuf-decode a message that was already decoded but not
%% re-encoded by the first transformation.
t_protobuf_bad_chain(_Config) ->
    ?check_trace(
        begin
            SerdeName = <<"myserde">>,
            MessageType = <<"Person">>,
            protobuf_create_serde(SerdeName),

            Name1 = <<"foo">>,
            PayloadSerde = #{
                <<"type">> => <<"protobuf">>,
                <<"schema">> => SerdeName,
                <<"message_type">> => MessageType
            },
            NoneSerde = #{<<"type">> => <<"none">>},
            JSONSerde = #{<<"type">> => <<"json">>},

            Transformation1 = transformation(Name1, _Ops1 = [], #{
                <<"payload_decoder">> => PayloadSerde,
                <<"payload_encoder">> => NoneSerde
            }),
            {201, _} = insert(Transformation1),

            %% WRONG: after the first transformation, payload is already decoded, so we
            %% shouldn't use protobuf again.
            Name2 = <<"bar">>,
            Transformation2A = transformation(Name2, [], #{
                <<"payload_decoder">> => PayloadSerde,
                <<"payload_encoder">> => JSONSerde
            }),
            {201, _} = insert(Transformation2A),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

            [Payload | _] = protobuf_valid_payloads(SerdeName, MessageType),
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            ?assertMatch(
                [],
                [
                    E
                 || #{
                        ?snk_kind := message_transformation_failed,
                        message := "payload_decode_schema_failure",
                        reason := function_clause
                    } = E <- Trace
                ]
            ),
            %% No unexpected crashes
            ?assertMatch(
                [],
                [
                    E
                 || #{
                        ?snk_kind := message_transformation_failed,
                        stacktrace := _
                    } = E <- Trace
                ]
            ),
            ?assertMatch(
                [
                    #{
                        explain :=
                            <<"Attempted to schema decode an already decoded message.", _/binary>>
                    }
                    | _
                ],
                [
                    E
                 || #{
                        ?snk_kind := message_transformation_failed,
                        message := "payload_decode_error"
                    } = E <- Trace
                ]
            ),
            ok
        end
    ),
    ok.

%% Tests that restoring a backup config works.
%%   * Existing transformations (identified by `name') are left untouched.
%%   * No transformations are removed.
%%   * New transformations are appended to the existing list.
%%   * Existing transformations are not reordered.
t_import_config_backup(_Config) ->
    %% Setup backup file.

    %% Will clash with existing transformation; different order.
    Name2 = <<"2">>,
    Operation2B = topic_operation(<<"concat([topic, '/', 2, 'b'])">>),
    Transformation2B = transformation(Name2, [Operation2B]),
    {201, _} = insert(Transformation2B),

    %% Will clash with existing transformation.
    Name1 = <<"1">>,
    Operation1B = topic_operation(<<"concat([topic, '/', 1, 'b'])">>),
    Transformation1B = transformation(Name1, [Operation1B]),
    {201, _} = insert(Transformation1B),

    %% New transformation; should be appended
    Name4 = <<"4">>,
    Operation4 = topic_operation(<<"concat([topic, '/', 4])">>),
    Transformation4 = transformation(Name4, [Operation4]),
    {201, _} = insert(Transformation4),

    {200, #{<<"filename">> := BackupName}} = export_backup(),

    %% Clear this setup and pretend we have other data to begin with.
    clear_all_transformations(),
    {200, []} = list(),

    Operation1A = topic_operation(<<"concat([topic, '/', 1, 'a'])">>),
    Transformation1A = transformation(Name1, [Operation1A]),
    {201, _} = insert(Transformation1A),

    Operation2A = topic_operation(<<"concat([topic, '/', 2, 'a'])">>),
    Transformation2A = transformation(Name2, [Operation2A]),
    {201, _} = insert(Transformation2A),

    Name3 = <<"3">>,
    Operation3 = topic_operation(<<"concat([topic, '/', 3])">>),
    Transformation3 = transformation(Name3, [Operation3]),
    {201, _} = insert(Transformation3),

    {204, _} = import_backup(BackupName),

    ExpectedTransformations = [
        Transformation1A,
        Transformation2A,
        Transformation3,
        Transformation4
    ],
    ?assertMatch({200, ExpectedTransformations}, list(), #{expected => ExpectedTransformations}),
    ?assertIndexOrder([Name1, Name2, Name3, Name4], <<"t/a">>),

    ok.

%% Tests that importing configurations from the CLI interface work.
t_load_config(_Config) ->
    Name1 = <<"1">>,
    Operation1A = topic_operation(<<"concat([topic, '/', 1, 'a'])">>),
    Transformation1A = transformation(Name1, [Operation1A]),
    {201, _} = insert(Transformation1A),

    Name2 = <<"2">>,
    Operation2A = topic_operation(<<"concat([topic, '/', 2, 'a'])">>),
    Transformation2A = transformation(Name2, [Operation2A]),
    {201, _} = insert(Transformation2A),

    Name3 = <<"3">>,
    Operation3 = topic_operation(<<"concat([topic, '/', 3])">>),
    Transformation3 = transformation(Name3, [Operation3]),
    {201, _} = insert(Transformation3),

    %% Config to load
    %% Will replace existing config
    Operation2B = topic_operation(<<"concat([topic, '/', 2, 'b'])">>),
    Transformation2B = transformation(Name2, [Operation2B]),

    %% Will replace existing config
    Operation1B = topic_operation(<<"concat([topic, '/', 1, 'b'])">>),
    Transformation1B = transformation(Name1, [Operation1B]),

    %% New transformation; should be appended
    Name4 = <<"4">>,
    Operation4 = topic_operation(<<"concat([topic, '/', 4])">>),
    Transformation4 = transformation(Name4, [Operation4]),

    ConfRootBin = <<"message_transformation">>,
    ConfigToLoad1 = #{
        ConfRootBin => #{
            <<"transformations">> => [Transformation2B, Transformation1B, Transformation4]
        }
    },
    ConfigToLoadBin1 = iolist_to_binary(hocon_pp:do(ConfigToLoad1, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoadBin1, #{mode => merge})),
    ExpectedTransformations1 = [
        Transformation1A,
        Transformation2A,
        Transformation3,
        Transformation4
    ],
    ?assertMatch(
        #{
            ConfRootBin := #{
                <<"transformations">> := ExpectedTransformations1
            }
        },
        emqx_conf_cli:get_config(<<"message_transformation">>)
    ),
    ?assertIndexOrder([Name1, Name2, Name3, Name4], <<"t/a">>),

    %% Replace
    Operation4B = topic_operation(<<"concat([topic, '/', 4, 'b'])">>),
    Transformation4B = transformation(Name4, [Operation4B]),

    Name5 = <<"5">>,
    Operation5 = topic_operation(<<"concat([topic, '/', 5])">>),
    Transformation5 = transformation(Name5, [Operation5]),

    ConfigToLoad2 = #{
        ConfRootBin => #{
            <<"transformations">> => [
                Transformation4B,
                Transformation3,
                Transformation5
            ]
        }
    },
    ConfigToLoadBin2 = iolist_to_binary(hocon_pp:do(ConfigToLoad2, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoadBin2, #{mode => replace})),
    ExpectedTransformations2 = [
        Transformation4B,
        Transformation3,
        Transformation5
    ],
    ?assertMatch(
        #{
            ConfRootBin := #{
                <<"transformations">> := ExpectedTransformations2
            }
        },
        emqx_conf_cli:get_config(<<"message_transformation">>)
    ),
    ?assertIndexOrder([Name4, Name3, Name5], <<"t/a">>),

    ok.

%% We need to verify that the final `payload' output by the transformations is a binary.
t_final_payload_must_be_binary(_Config) ->
    ?check_trace(
        begin
            Name1 = <<"foo">>,
            Operations = [operation(<<"payload.hello">>, <<"concat(['world'])">>)],
            Transformation1 = transformation(Name1, Operations, #{
                <<"payload_decoder">> => #{<<"type">> => <<"json">>},
                <<"payload_encoder">> => #{<<"type">> => <<"none">>}
            }),
            {201, _} = insert(Transformation1),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
            ok = publish(C, <<"t/1">>, #{x => 1, y => true}),
            ?assertNotReceive({publish, _}),

            ?retry(
                100,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> :=
                            #{
                                <<"matched">> := 1,
                                <<"succeeded">> := 0,
                                <<"failed">> := 1
                            },
                        <<"node_metrics">> :=
                            [
                                #{
                                    <<"node">> := _,
                                    <<"metrics">> := #{
                                        <<"matched">> := 1,
                                        <<"succeeded">> := 0,
                                        <<"failed">> := 1
                                    }
                                }
                            ]
                    }},
                    get_metrics(Name1)
                )
            ),

            %% When there are multiple transformations for a topic, the last one is
            %% responsible for properly encoding the payload to a binary.
            Name2 = <<"bar">>,
            Transformation2 = transformation(Name2, _Operations = [], #{
                <<"payload_decoder">> => #{<<"type">> => <<"none">>},
                <<"payload_encoder">> => #{<<"type">> => <<"none">>}
            }),
            {201, _} = insert(Transformation2),

            ok = publish(C, <<"t/1">>, #{x => 1, y => true}),
            ?assertNotReceive({publish, _}),

            %% The old, first transformation succeeds.
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
            ),

            %% The last transformation gets the failure metric bump.
            ?assertMatch(
                {200, #{
                    <<"metrics">> :=
                        #{
                            <<"matched">> := 1,
                            <<"succeeded">> := 0,
                            <<"failed">> := 1
                        },
                    <<"node_metrics">> :=
                        [
                            #{
                                <<"node">> := _,
                                <<"metrics">> := #{
                                    <<"matched">> := 1,
                                    <<"succeeded">> := 0,
                                    <<"failed">> := 1
                                }
                            }
                        ]
                }},
                get_metrics(Name2)
            ),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{message := "transformation_bad_encoding"},
                    #{message := "transformation_bad_encoding"}
                ],
                ?of_kind(message_transformation_failed, Trace)
            ),
            ok
        end
    ),
    ok.

%% Checks that an input value that does not respect the declared encoding bumps the
%% failure metric as expected.  Also, such a crash does not lead to the message continuing
%% the publication process.
t_bad_decoded_value_failure_metric(_Config) ->
    ?check_trace(
        begin
            Name = <<"bar">>,
            Operations = [operation(<<"payload.msg">>, <<"payload">>)],
            Transformation = transformation(Name, Operations, #{
                <<"payload_decoder">> => #{<<"type">> => <<"none">>},
                <<"payload_encoder">> => #{<<"type">> => <<"json">>}
            }),
            {201, _} = insert(Transformation),
            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
            ok = publish(C, <<"t/1">>, {raw, <<"aaa">>}),
            ?assertNotReceive({publish, _}),
            ?retry(
                100,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> :=
                            #{
                                <<"matched">> := 1,
                                <<"succeeded">> := 0,
                                <<"failed">> := 1
                            },
                        <<"node_metrics">> :=
                            [
                                #{
                                    <<"node">> := _,
                                    <<"metrics">> := #{
                                        <<"matched">> := 1,
                                        <<"succeeded">> := 0,
                                        <<"failed">> := 1
                                    }
                                }
                            ]
                    }},
                    get_metrics(Name)
                )
            ),
            ok
        end,
        []
    ),
    ok.

%% Smoke test for the `json_encode' and `json_decode' BIFs.
t_json_encode_decode_smoke_test(_Config) ->
    ?check_trace(
        begin
            Name = <<"foo">>,
            Operations = [
                operation(
                    <<"payload">>,
                    <<"json_decode('{\"hello\":\"world\"}')">>
                ),
                operation(
                    <<"payload">>,
                    <<"json_encode(maps.put('hello', 'planet', payload))">>
                )
            ],
            Transformation = transformation(Name, Operations, #{
                <<"payload_decoder">> => #{<<"type">> => <<"none">>},
                <<"payload_encoder">> => #{<<"type">> => <<"none">>}
            }),
            {201, _} = insert(Transformation),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
            ok = publish(C, <<"t/1">>, #{}),
            ?assertReceive({publish, #{payload := <<"{\"hello\":\"planet\"}">>}}),
            ok
        end,
        []
    ),
    ok.

%% Simple smoke test for client attributes support.
t_client_attrs(_Config) ->
    {ok, Compiled} = emqx_variform:compile(<<"user_property.tenant">>),
    ok = emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"tenant">>
        }
    ]),
    on_exit(fun() -> ok = emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []) end),
    ?check_trace(
        begin
            Name1 = <<"foo">>,
            Operation1 = operation(topic, <<"concat([client_attrs.tenant, '/', topic])">>),
            Transformation1 = transformation(Name1, [Operation1]),
            {201, _} = insert(Transformation1),

            Tenant = <<"mytenant">>,
            C = connect(
                <<"c1">>,
                _IsPersistent = false,
                #{properties => #{'User-Property' => [{<<"tenant">>, Tenant}]}}
            ),
            {ok, _, [_]} = emqtt:subscribe(C, emqx_topic:join([Tenant, <<"#">>])),

            ok = publish(C, <<"t/1">>, #{x => 1, y => 2}),
            ?assertReceive({publish, #{topic := <<"mytenant/t/1">>}}),

            ok
        end,
        []
    ),
    ok.

%% Smoke tests for the dryrun endpoint.
t_dryrun_transformation(_Config) ->
    ?check_trace(
        begin
            Name1 = <<"foo">>,
            Operations = [
                operation(qos, <<"payload.q">>),
                operation(topic, <<"concat([topic, '/', payload.t])">>),
                operation(retain, <<"payload.r">>),
                operation(<<"user_property.a">>, <<"payload.u.a">>),
                operation(<<"user_property.copy">>, <<"user_property.original">>),
                operation(<<"payload.user">>, <<"username">>),
                operation(<<"payload.flags">>, <<"flags">>),
                operation(<<"payload.pprops">>, <<"pub_props">>),
                operation(<<"payload.expiry">>, <<"pub_props.Message-Expiry-Interval">>),
                operation(<<"payload.peername">>, <<"peername">>),
                operation(<<"payload.node">>, <<"node">>),
                operation(<<"payload.id">>, <<"id">>),
                operation(<<"payload.clientid">>, <<"clientid">>),
                operation(<<"payload.now">>, <<"timestamp">>),
                operation(<<"payload.recv_at">>, <<"publish_received_at">>),
                operation(<<"payload.hi">>, <<"payload.p.hello">>)
            ],
            Transformation1 = transformation(Name1, Operations),

            %% Good input
            ClientId = <<"myclientid">>,
            Username = <<"myusername">>,
            Peername = <<"10.0.50.1:63221">>,
            Message1 = dryrun_input_message(#{
                clientid => ClientId,
                payload => #{
                    p => #{<<"hello">> => <<"world">>},
                    q => 1,
                    r => true,
                    t => <<"t">>,
                    u => #{a => <<"b">>}
                },
                peername => Peername,
                pub_props => #{<<"Message-Expiry-Interval">> => 30},
                user_property => #{<<"original">> => <<"user_prop">>},
                username => Username
            }),
            Res1 = dryrun_transformation(Transformation1, Message1),
            ?assertMatch(
                {200, #{
                    <<"payload">> := _,
                    <<"qos">> := 1,
                    <<"retain">> := true,
                    <<"topic">> := <<"t/u/v/t">>,
                    <<"user_property">> := #{
                        <<"a">> := <<"b">>,
                        <<"original">> := <<"user_prop">>,
                        <<"copy">> := <<"user_prop">>
                    }
                }},
                Res1
            ),
            {200, #{<<"payload">> := EncPayloadRes1}} = Res1,
            NodeBin = atom_to_binary(node()),
            ?assertMatch(
                #{
                    <<"hi">> := <<"world">>,
                    <<"now">> := _,
                    <<"recv_at">> := _,
                    <<"clientid">> := ClientId,
                    <<"pprops">> := #{
                        <<"Message-Expiry-Interval">> := 30,
                        <<"User-Property">> := #{
                            <<"original">> := <<"user_prop">>
                        }
                    },
                    <<"expiry">> := 30,
                    <<"peername">> := Peername,
                    <<"node">> := NodeBin,
                    <<"id">> := <<_/binary>>,
                    <<"flags">> := #{<<"dup">> := false, <<"retain">> := true},
                    <<"user">> := Username
                },
                emqx_utils_json:decode(EncPayloadRes1, [return_maps])
            ),

            %% Bad input: fails to decode
            Message2 = dryrun_input_message(#{payload => "{"}, #{encoder => fun(X) -> X end}),
            ?assertMatch(
                {400, #{
                    <<"decoder">> := <<"json">>,
                    <<"reason">> := <<_/binary>>
                }},
                dryrun_transformation(Transformation1, Message2)
            ),

            %% Bad output: fails to encode
            MissingSerde = <<"missing_serde">>,
            Transformation2 = transformation(Name1, [dummy_operation()], #{
                <<"payload_decoder">> => json_serde(),
                <<"payload_encoder">> => avro_serde(MissingSerde)
            }),
            ?assertMatch(
                {400, #{
                    <<"msg">> := <<"payload_encode_schema_not_found">>,
                    <<"encoder">> := <<"avro">>,
                    <<"schema_name">> := MissingSerde
                }},
                dryrun_transformation(Transformation2, Message1)
            ),

            %% Bad input: unbound var during one of the operations
            Message3 = dryrun_input_message(#{
                payload => #{
                    p => #{<<"hello">> => <<"world">>},
                    q => 1,
                    %% Missing:
                    %% r => true,
                    t => <<"t">>,
                    u => #{a => <<"b">>}
                }
            }),
            ?assertMatch(
                {400, #{
                    <<"msg">> :=
                        <<"transformation_eval_operation_failure">>,
                    <<"reason">> :=
                        #{
                            <<"reason">> := <<"var_unbound">>,
                            <<"var_name">> := <<"payload.r">>
                        }
                }},
                dryrun_transformation(Transformation1, Message3)
            ),

            ok
        end,
        []
    ),
    ok.

%% Verifies that if a transformation's decoder is fed a non-binary input (e.g.:
%% `undefined'), it returns a friendly message.
t_non_binary_input_for_decoder(_Config) ->
    ?check_trace(
        begin
            %% The transformations set up here lead to an invalid input payload for the
            %% second transformation: `payload = undefined' (an atom) after the first
            %% transformation.
            Name1 = <<"foo">>,
            Operations1 = [operation(payload, <<"flags.dup">>)],
            NoSerde = #{<<"type">> => <<"none">>},
            Transformation1 = transformation(Name1, Operations1, #{
                <<"payload_decoder">> => NoSerde,
                <<"payload_encoder">> => NoSerde
            }),
            Name2 = <<"bar">>,
            Operations2 = [],
            JSONSerde = #{<<"type">> => <<"json">>},
            Transformation2 = transformation(Name2, Operations2, #{
                <<"payload_decoder">> => JSONSerde,
                <<"payload_encoder">> => JSONSerde
            }),
            {201, _} = insert(Transformation1),
            {201, _} = insert(Transformation2),

            C = connect(<<"c1">>),
            {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),
            ok = publish(C, <<"t/1">>, #{x => 1, y => true}),
            ?assertNotReceive({publish, _}),

            ok
        end,
        fun(Trace) ->
            SubTrace = ?of_kind(message_transformation_failed, Trace),
            ?assertMatch([], [E || E = #{reason := function_clause} <- SubTrace]),
            ?assertMatch([#{reason := <<"payload must be a binary">>} | _], SubTrace),
            ok
        end
    ),
    ok.
