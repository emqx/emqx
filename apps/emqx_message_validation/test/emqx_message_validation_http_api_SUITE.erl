%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_validation_http_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

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
                emqx_message_validation,
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
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(assertIndexOrder(EXPECTED, TOPIC), assert_index_order(EXPECTED, TOPIC, #{line => ?LINE})).

clear_all_validations() ->
    lists:foreach(
        fun(#{name := Name}) ->
            {ok, _} = emqx_message_validation:delete(Name)
        end,
        emqx_message_validation:list()
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
        <<"log_failure_at">> => <<"warning">>,
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

api_root() -> "message_validations".

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

protobuf_valid_payloads(SerdeName, MessageName) ->
    lists:map(
        fun(Payload) -> emqx_schema_registry_serde:encode(SerdeName, Payload, [MessageName]) end,
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
         || #{name := N} <- emqx_message_validation_registry:matching_validations(Topic)
        ],
        Comment
    ).

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
    ok = publish(C, <<"t/1">>, #{x => 7, y => 0}),
    ?assertNotReceive({publish, _}),
    ?assertNotReceive({disconnected, _, _}),
    unlink(C),
    ok = publish(C, <<"t/1">>, #{x => 0, y => 1}),
    ?assertNotReceive({publish, _}),
    ?assertReceive({disconnected, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, _}),

    ok.

t_schema_check_non_existent_serde(_Config) ->
    SerdeName = <<"idontexist">>,
    Name1 = <<"foo">>,
    Check1 = schema_check(json, SerdeName),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    ok = publish(C, <<"t/1">>, #{i => 10, s => <<"s">>}),
    ?assertNotReceive({publish, _}),

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
    MessageName = <<"Person">>,
    protobuf_create_serde(SerdeName),

    Name1 = <<"foo">>,
    Check1 = schema_check(protobuf, SerdeName, #{<<"message_name">> => MessageName}),
    Validation1 = validation(Name1, [Check1]),
    {201, _} = insert(Validation1),

    C = connect(<<"c1">>),
    {ok, _, [_]} = emqtt:subscribe(C, <<"t/#">>),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, MessageName)
    ),
    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_invalid_payloads()
    ),

    %% Bad config: unknown message name
    Check2 = schema_check(protobuf, SerdeName, #{<<"message_name">> => <<"idontexist">>}),
    Validation2 = validation(Name1, [Check2]),
    {200, _} = update(Validation2),

    lists:foreach(
        fun(Payload) ->
            ok = publish(C, <<"t/1">>, {raw, Payload}),
            ?assertNotReceive({publish, _})
        end,
        protobuf_valid_payloads(SerdeName, MessageName)
    ),

    ok.
