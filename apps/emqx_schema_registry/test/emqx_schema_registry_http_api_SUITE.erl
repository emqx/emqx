%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_http_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_schema_registry.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(REDACTED, <<"******">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    OnlyOnceTCs = only_once_testcases(),
    [
        {group, avro},
        {group, protobuf},
        {group, json}
        | OnlyOnceTCs
    ].

groups() ->
    OnlyOnceTCs = only_once_testcases(),
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    PerTypeTCs = AllTCs -- OnlyOnceTCs,
    [
        {avro, PerTypeTCs},
        {protobuf, PerTypeTCs},
        {json, PerTypeTCs}
    ].

only_once_testcases() ->
    [
        t_empty_sparkplug,
        t_external_registry_crud_confluent,
        t_smoke_test_external_registry_confluent,
        t_external_http_serde
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_schema_registry,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(avro, Config) ->
    Source = #{
        type => record,
        name => <<"apitest">>,
        fields => [
            #{name => <<"i">>, type => <<"int">>},
            #{name => <<"s">>, type => <<"string">>}
        ]
    },
    SourceBin = emqx_utils_json:encode(Source),
    InvalidSourceBin = <<"{}">>,
    [
        {serde_type, avro},
        {schema_source, SourceBin},
        {invalid_schema_source, InvalidSourceBin}
        | Config
    ];
init_per_group(protobuf, Config) ->
    SourceBin =
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
            "}\n"
        >>,
    InvalidSourceBin = <<"xxxx">>,
    [
        {serde_type, protobuf},
        {schema_source, SourceBin},
        {invalid_schema_source, InvalidSourceBin}
        | Config
    ];
init_per_group(json, Config) ->
    Source =
        #{
            properties => #{
                foo => #{},
                bar => #{}
            },
            required => [<<"foo">>]
        },
    SourceBin = emqx_utils_json:encode(Source),
    InvalidSourceBin = <<"\"not an object\"">>,
    [
        {serde_type, json},
        {schema_source, SourceBin},
        {invalid_schema_source, InvalidSourceBin}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    clear_schemas(),
    clear_external_registries(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_schemas(),
    clear_external_registries(),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

request(get) ->
    do_request(get, uri(["schema_registry"]), _Body = []);
request({get, Name}) ->
    do_request(get, uri(["schema_registry", Name]), _Body = []);
request({delete, Name}) ->
    do_request(delete, uri(["schema_registry", Name]), _Body = []);
request({put, Name, Params}) ->
    do_request(put, uri(["schema_registry", Name]), Params);
request({post, Params}) ->
    do_request(post, uri(["schema_registry"]), Params).

do_request(Method, Path, Body) ->
    Header = emqx_common_test_http:default_auth_header(),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    Res0 = emqx_mgmt_api_test_util:request_api(Method, Path, [], Header, Body, Opts),
    case Res0 of
        {ok, Code, <<>>} ->
            {ok, Code, <<>>};
        {ok, Code, Res1} ->
            Res2 = emqx_utils_json:decode(Res1),
            Res3 = try_decode_error_message(Res2),
            {ok, Code, Res3};
        Error ->
            Error
    end.

try_decode_error_message(#{<<"message">> := Msg0} = Res0) ->
    case emqx_utils_json:safe_decode(Msg0) of
        {ok, Msg} ->
            Res0#{<<"message">> := Msg};
        {error, _} ->
            Res0
    end;
try_decode_error_message(Res) ->
    Res.

clear_schemas() ->
    maps:foreach(
        fun(Name, _Schema) ->
            ok = emqx_schema_registry:delete_schema(Name)
        end,
        emqx_schema_registry:list_schemas()
    ).

clear_external_registries() ->
    maps:foreach(
        fun(Name, _Schema) ->
            ok = emqx_schema_registry_config:delete_external_registry(Name)
        end,
        emqx_schema_registry_config:list_external_registries()
    ).

dryrun_rule(SQL, Context) ->
    Params = #{
        context => Context,
        sql => SQL
    },
    Path = emqx_mgmt_api_test_util:api_path(["rule_test"]),
    Res = do_request(post, Path, Params),
    ct:pal("dryrun rule result:\n  ~p", [Res]),
    case Res of
        {ok, {{_, 201, _}, _, #{<<"id">> := RuleId}}} ->
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            simplify_result(Res);
        _ ->
            simplify_result(Res)
    end.

simplify_result(Res) ->
    case Res of
        {ok, Status, Body} ->
            {Status, Body}
    end.

simple_request(Method, Path, Params) ->
    emqx_mgmt_api_test_util:simple_request(Method, Path, Params).

confluent_schema_registry_no_auth() ->
    confluent_schema_registry_no_auth(_Overrides = #{}).

confluent_schema_registry_no_auth(#{} = Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => <<"confluent">>,
            <<"url">> => confluent_url_bin(without_auth),
            <<"auth">> => <<"none">>
        },
        Overrides
    ).

confluent_schema_registry_with_basic_auth() ->
    confluent_schema_registry_with_basic_auth(_Overrides = #{}).

confluent_schema_registry_with_basic_auth(#{} = Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => <<"confluent">>,
            <<"url">> => confluent_url_bin(with_auth),
            <<"auth">> => confluent_schema_registry_basic_auth()
        },
        Overrides
    ).

confluent_schema_registry_basic_auth() ->
    #{
        <<"mechanism">> => <<"basic">>,
        <<"username">> => <<"cpsruser">>,
        <<"password">> => <<"mypass">>
    }.

get_schema(Name) ->
    case request({get, Name}) of
        {ok, Code, Res} ->
            {Code, Res};
        Error ->
            Error
    end.

create_schema(Params) ->
    case request({post, Params}) of
        {ok, Code, Res} ->
            {Code, Res};
        Error ->
            Error
    end.

update_schema(Name, Params) ->
    case request({put, Name, Params}) of
        {ok, Code, Res} ->
            {Code, Res};
        Error ->
            Error
    end.

create_schema_protobuf_bundle(Params) ->
    do_create_or_update_schema_protobuf_bundle(Params#{method => post}).

update_schema_protobuf_bundle(Params) ->
    do_create_or_update_schema_protobuf_bundle(Params#{method => put}).

do_create_or_update_schema_protobuf_bundle(Params) ->
    #{
        name := Name,
        file := Filename,
        root_proto_path := RootPath,
        method := Method
    } = Params,
    Description = maps:get(description, Params, undefined),
    URL = uri(["schema_registry_protobuf", "bundle"]),
    Res = emqx_mgmt_api_test_util:upload_request(#{
        url => URL,
        filepath_to_upload => Filename,
        file_parameter_name => <<"bundle">>,
        mime_type => <<"application/octet-stream">>,
        other_params => lists:flatten([
            {<<"name">>, Name},
            {<<"root_proto_file">>, RootPath},
            [{<<"description">>, Description} || Description /= undefined]
        ]),
        auth_token => emqx_mgmt_api_test_util:auth_header_(),
        method => Method
    }),
    emqx_mgmt_api_test_util:simplify_decode_result(Res).

delete_schema(Name) ->
    case request({delete, Name}) of
        {ok, Code, Res} ->
            {Code, Res};
        Error ->
            Error
    end.

list_external_registries() ->
    Path = uri(["schema_registry_external"]),
    simple_request(get, Path, _Params = []).

get_external_registry(Name) ->
    Path = uri(["schema_registry_external", "registry", Name]),
    simple_request(get, Path, _Params = []).

create_external_registry(Params) ->
    Path = uri(["schema_registry_external"]),
    simple_request(post, Path, Params).

update_external_registry(Name, Params) ->
    Path = uri(["schema_registry_external", "registry", Name]),
    simple_request(put, Path, Params).

delete_external_registry(Name) ->
    Path = uri(["schema_registry_external", "registry", Name]),
    simple_request(delete, Path, _Params = []).

find_external_registry_worker(Name) ->
    [
        Pid
     || {N, Pid, _, _} <- supervisor:which_children(emqx_schema_registry_external_sup),
        emqx_utils_conv:bin(N) =:= Name
    ].

confluent_url_bin(WithOrWithoutAuth) ->
    list_to_binary(confluent_url_string(WithOrWithoutAuth)).

confluent_url_string(without_auth) ->
    "http://confluent_schema_registry:8081";
confluent_url_string(with_auth) ->
    "http://confluent_schema_registry_basicauth:8081".

start_confluent_client(WithOrWithoutAuth) ->
    Cfg0 = #{url => confluent_url_string(WithOrWithoutAuth)},
    Cfg = emqx_utils_maps:put_if(
        Cfg0,
        auth,
        to_avlizer_auth(confluent_schema_registry_basic_auth()),
        WithOrWithoutAuth =:= with_auth
    ),
    {ok, Server} = avlizer_confluent:start_link(_ServerRef = undefined, Cfg),
    Table = avlizer_confluent:get_table(Server),
    #{server => Server, table => Table}.

to_avlizer_auth(#{<<"mechanism">> := <<"basic">>} = Auth0) ->
    #{<<"username">> := Username, <<"password">> := Password} = Auth0,
    {basic, emqx_utils_conv:str(Username), emqx_utils_conv:str(Password)}.

register_schema_confluent(#{server := Server}, Subject, Schema) ->
    {ok, Id} = avlizer_confluent:register_schema(Server, Subject, Schema),
    Id.

confluent_encode(Data, SchemaId, RegistryClient) ->
    #{server := Server, table := Table} = RegistryClient,
    Encoder = avlizer_confluent:make_encoder2(Server, Table, SchemaId),
    avlizer_confluent:encode(Encoder, Data).

confluent_encode_and_tag(Data, SchemaId, RegistryClient) ->
    avlizer_confluent:tag_data(SchemaId, confluent_encode(Data, SchemaId, RegistryClient)).

confluent_decode_untagged(Data, SchemaId, RegistryClient) ->
    #{server := Server, table := Table} = RegistryClient,
    Decoder = avlizer_confluent:make_decoder2(Server, Table, SchemaId),
    avlizer_confluent:decode(Decoder, Data).

avro_schema2() ->
    avro_record:type(
        <<"myrecord">>,
        [avro_record:define_field(f1, avro_map:type(avro_primitive:int_type()))],
        [{namespace, 'com.example'}]
    ).

sql(Template, Context) ->
    Parsed = emqx_template:parse(Template),
    iolist_to_binary(emqx_template:render_strict(Parsed, Context)).

publish_context({json, Data}) ->
    publish_context1(emqx_utils_json:encode(Data));
publish_context({hex, Data}) ->
    publish_context1(bin2hex(Data)).

publish_context1(Payload) ->
    #{
        <<"clientid">> => <<"c_emqx">>,
        <<"event_type">> => <<"message_publish">>,
        <<"payload">> => Payload,
        <<"qos">> => 1,
        <<"topic">> => <<"t">>,
        <<"username">> => <<"u_emqx">>
    }.

bin2hex(Bin) ->
    emqx_rule_funcs:bin2hexstr(Bin).

mk_external_http_create_params(Opts) ->
    #{name := SchemaName, port := Port} = Opts,
    URL = maps:get(
        url,
        Opts,
        <<"http://127.0.0.1:", (integer_to_binary(Port))/binary, "/sr?qp=123">>
    ),
    #{
        <<"type">> => <<"external_http">>,
        <<"parameters">> => #{
            <<"url">> => URL,
            <<"external_params">> => <<"xor">>,
            <<"headers">> => #{<<"extra">> => <<"headers">>},
            <<"request_timeout">> => <<"1s">>,
            <<"max_retries">> => 1,
            <<"connect_timeout">> => <<"2s">>,
            <<"pool_type">> => <<"random">>,
            <<"pool_size">> => 2,
            <<"enable_pipelining">> => 100,
            <<"max_inactive">> => <<"1s">>,
            <<"ssl">> => #{<<"enable">> => false}
        },
        <<"name">> => SchemaName,
        <<"description">> => <<"My external schema">>
    }.

start_external_http_serde_server() ->
    on_exit(fun emqx_utils_http_test_server:stop/0),
    {ok, {Port, _ServerPid}} = emqx_utils_http_test_server:start_link(random, "/sr"),
    emqx_utils_http_test_server:set_handler(fun ?MODULE:external_http_handler/2),
    {ok, Port}.

external_http_handler(Req0, State) ->
    {ok, Body, Req} = cowboy_req:read_body(Req0),
    maybe
        {ok, #{
            <<"schema_name">> := _Name,
            <<"type">> := EncodeOrDecode,
            <<"opts">> := ExtraOpts,
            <<"payload">> := PayloadB64
        }} ?= emqx_utils_json:safe_decode(Body),
        {ok, Payload} ?= decode_base64(PayloadB64),
        {ok, RespBody} ?= exec_external_http_serde(EncodeOrDecode, ExtraOpts, Payload),
        RespBodyB64 = base64:encode(RespBody),
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            RespBodyB64,
            Req
        ),
        {ok, Rep, State}
    else
        {error, Err} ->
            RepErr = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"application/json">>},
                Err,
                Req
            ),
            {ok, RepErr, State}
    end.

decode_base64(Bin) ->
    try
        {ok, base64:decode(Bin)}
    catch
        error:_ ->
            {error, bad_base64}
    end.

exec_external_http_serde(_EncodeOrDecode, <<"xor">>, Payload) ->
    %% Implementing example similar from 4.x docs
    Key = $z,
    Xored = xor_bin(Key, Payload),
    {ok, Xored};
exec_external_http_serde(EncodeOrDecode, Opts, Payload) ->
    {error, #{reason => bad_input, type => EncodeOrDecode, opts => Opts, payload => Payload}}.

xor_bin(Key, Payload) ->
    <<<<(X bxor Key)>> || <<X>> <= Payload>>.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_crud(Config) ->
    SerdeType = ?config(serde_type, Config),
    SourceBin = ?config(schema_source, Config),
    InvalidSourceBin = ?config(invalid_schema_source, Config),
    SerdeTypeBin = atom_to_binary(SerdeType),
    SchemaName = <<"my_schema">>,
    Params = #{
        <<"type">> => SerdeTypeBin,
        <<"source">> => SourceBin,
        <<"name">> => SchemaName,
        <<"description">> => <<"My schema">>
    },
    UpdateParams = maps:without([<<"name">>], Params),

    %% no schemas at first
    ?assertMatch({ok, 200, []}, request(get)),
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Schema not found">>
        }},
        request({get, <<"some_name_that_is_not_an_atom_yet">>})
    ),
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Schema not found">>
        }},
        request({get, SchemaName})
    ),
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Schema not found">>
        }},
        request({put, SchemaName, UpdateParams})
    ),
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Schema not found">>
        }},
        request({delete, SchemaName})
    ),
    %% create a schema
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := SerdeTypeBin,
            <<"source">> := SourceBin,
            <<"name">> := SchemaName,
            <<"description">> := <<"My schema">>
        }},
        request({post, Params})
    ),
    %% Test that we can't create a schema with the special Sparkplug B name
    %% (the special Sparkplug B name contains a random sequence of chars so
    %% should be very unlikely that users try to do this)
    ParmsWithForbiddenName = maps:put(
        <<"name">>, ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Params
    ),
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>
        }},
        request({post, ParmsWithForbiddenName})
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := SerdeTypeBin,
            <<"source">> := SourceBin,
            <<"name">> := SchemaName,
            <<"description">> := <<"My schema">>
        }},
        request({get, SchemaName})
    ),
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := SerdeTypeBin,
                <<"source">> := SourceBin,
                <<"name">> := SchemaName,
                <<"description">> := <<"My schema">>
            }
        ]},
        request(get)
    ),
    UpdateParams1 = UpdateParams#{<<"description">> := <<"My new schema">>},
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := SerdeTypeBin,
            <<"source">> := SourceBin,
            <<"name">> := SchemaName,
            <<"description">> := <<"My new schema">>
        }},
        request({put, SchemaName, UpdateParams1})
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"ALREADY_EXISTS">>,
            <<"message">> := <<"Schema already exists">>
        }},
        request({post, Params})
    ),
    %% typechecks, but is invalid
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"{post_config_update,emqx_schema_registry_config,", _/binary>>
        }},
        request({put, SchemaName, UpdateParams#{<<"source">> := InvalidSourceBin}})
    ),

    ?assertMatch(
        {ok, 204, <<>>},
        request({delete, SchemaName})
    ),

    %% doesn't typecheck
    lists:foreach(
        fun(Field) ->
            ?assertMatch(
                {ok, 400, #{
                    <<"code">> := <<"BAD_REQUEST">>,
                    <<"message">> := #{<<"reason">> := <<"required_field">>}
                }},
                request({post, maps:without([Field], Params)}),
                #{field => Field}
            )
        end,
        [<<"name">>, <<"source">>]
    ),
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                #{
                    <<"expected">> := <<"avro | protobuf | json | ", _/binary>>,
                    <<"field_name">> := <<"type">>
                }
        }},
        request({post, maps:without([<<"type">>], Params)}),
        #{field => <<"type">>}
    ),
    %% typechecks, but is invalid
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"{post_config_update,emqx_schema_registry_config,", _/binary>>
        }},
        request({post, Params#{<<"source">> := InvalidSourceBin}})
    ),

    %% unknown serde type
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                #{
                    <<"expected">> := <<"avro | protobuf | json | ", _/binary>>,
                    <<"field_name">> := <<"type">>
                }
        }},
        request({post, Params#{<<"type">> := <<"foo">>}})
    ),

    ok.

t_empty_sparkplug(_Config) ->
    ?check_trace(
        begin
            SQL = <<
                "select sparkplug_decode(sparkplug_encode(json_decode(payload))) as decoded"
                " from \"t\" "
            >>,
            Context = #{
                <<"clientid">> => <<"c_emqx">>,
                <<"event_type">> => <<"message_publish">>,
                <<"payload">> => <<"{}">>,
                <<"qos">> => 1,
                <<"topic">> => <<"t">>,
                <<"username">> => <<"u_emqx">>
            },
            %% N.B.: It's not an error for a `metrics' field to "appear out of nowhere"
            %% after a roundtrip.  Since Sparkplug B schema uses proto 2, and the `metrics'
            %% field is `repeated', this field has no presence tracking, hence it's
            %% impossible to distinguish between an absent field and a field with its
            %% default value (an empty array, in this case).
            %% https://protobuf.dev/programming-guides/field_presence/#presence-in-proto2-apis
            ?assertMatch(
                {200, #{<<"decoded">> := #{<<"metrics">> := []}}}, dryrun_rule(SQL, Context)
            ),
            ok
        end,
        []
    ),
    ok.

%% Tests that we can't create or lookup names that are too long and get a decent error
%% message.
t_name_too_long(Config) ->
    SerdeType = ?config(serde_type, Config),
    SourceBin = ?config(schema_source, Config),
    SerdeTypeBin = atom_to_binary(SerdeType),
    %% Too long!
    SchemaName = binary:copy(<<"a">>, 256),
    Params = #{
        <<"type">> => SerdeTypeBin,
        <<"source">> => SourceBin,
        <<"name">> => SchemaName,
        <<"description">> => <<"My schema">>
    },
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"kind">> := <<"validation_error">>,
            <<"message">> := <<"Name length must be less than 255">>
        }},
        request({post, Params})
    ),
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Schema not found">>
        }},
        request({get, SchemaName})
    ),
    ok.

%% Checks basic CRUD operations for dealing with external confluent registry.
t_external_registry_crud_confluent(_Config) ->
    Name1 = <<"my_reg1">>,
    Params1 = confluent_schema_registry_no_auth(),
    NamedParams1 = Params1#{<<"name">> => Name1},

    ?assertEqual({200, #{}}, list_external_registries()),
    ?assertMatch({404, _}, get_external_registry(Name1)),
    ?assertMatch({404, _}, update_external_registry(Name1, Params1)),
    ?assertMatch({204, _}, delete_external_registry(Name1)),

    ?assertMatch({201, _}, create_external_registry(NamedParams1)),
    ?assertEqual({200, Params1}, get_external_registry(Name1)),
    ?assertMatch({200, #{Name1 := Params1}}, list_external_registries()),
    ?assertMatch([_], find_external_registry_worker(Name1)),

    Params2 = confluent_schema_registry_with_basic_auth(),
    Expected2 = emqx_utils_maps:deep_put([<<"auth">>, <<"password">>], Params2, ?REDACTED),
    ?assertMatch({200, Expected2}, update_external_registry(Name1, Params2)),
    ?assertMatch({200, Expected2}, get_external_registry(Name1)),
    ?assertMatch({200, #{Name1 := Expected2}}, list_external_registries()),
    ?assertMatch([_], find_external_registry_worker(Name1)),

    Name2 = <<"my_reg2">>,
    Params3 = confluent_schema_registry_no_auth(),
    NamedParams3 = Params3#{<<"name">> => Name2},
    ?assertMatch({404, _}, get_external_registry(Name2)),
    ?assertMatch({404, _}, update_external_registry(Name2, Params3)),
    ?assertMatch({204, _}, delete_external_registry(Name2)),
    ?assertMatch([], find_external_registry_worker(Name2)),

    ?assertMatch({201, _}, create_external_registry(NamedParams3)),
    ?assertMatch({200, Params3}, get_external_registry(Name2)),
    ?assertMatch({200, #{Name1 := Expected2, Name2 := Params3}}, list_external_registries()),
    ?assertMatch([_], find_external_registry_worker(Name1)),
    ?assertMatch([_], find_external_registry_worker(Name2)),

    ?assertMatch({204, _}, delete_external_registry(Name1)),
    ?assertMatch({404, _}, get_external_registry(Name1)),
    ?assertMatch({404, _}, update_external_registry(Name1, Params1)),
    ?assertMatch({204, _}, delete_external_registry(Name1)),
    ?assertMatch({200, #{Name2 := Params3}}, list_external_registries()),
    ?assertMatch([], find_external_registry_worker(Name1)),
    ?assertMatch([_], find_external_registry_worker(Name2)),

    %% Bad params
    BadParams = #{},
    ?assertMatch({400, _}, create_external_registry(BadParams)),
    ?assertMatch({400, _}, update_external_registry(Name2, BadParams)),

    ok.

%% Happy path tests when using external registry (confluent).
t_smoke_test_external_registry_confluent(_Config) ->
    Name1 = <<"my_reg1">>,
    Params1 = confluent_schema_registry_with_basic_auth(),
    NamedParams1 = Params1#{<<"name">> => Name1},
    {201, _} = create_external_registry(NamedParams1),

    RegistryClient = start_confluent_client(with_auth),
    Subject = atom_to_list(?FUNCTION_NAME),
    SchemaId = register_schema_confluent(RegistryClient, Subject, avro_schema2()),

    %% encode: fetch schema on the fly using id; good data
    SQL1 = sql(
        <<
            "select bin2hexstr(avro_encode('${.name}', json_decode(payload), ${.schema_id})) as encoded"
            " from \"t\" "
        >>,
        #{name => Name1, schema_id => SchemaId}
    ),
    Data1 = #{<<"f1">> => #{<<"bah">> => 123}},
    Context1 = publish_context({json, Data1}),
    Expected1 = bin2hex(confluent_encode(Data1, SchemaId, RegistryClient)),
    ?assertMatch(
        {200, #{<<"encoded">> := Expected1}},
        dryrun_rule(SQL1, Context1),
        #{expected => Expected1}
    ),

    %% decode: fetch schema on the fly using id; good data
    SQL2 = sql(
        <<
            "select avro_decode('${.name}', hexstr2bin(payload), ${.schema_id}) as decoded"
            " from \"t\" "
        >>,
        #{name => Name1, schema_id => SchemaId}
    ),
    Encoded2 = confluent_encode(Data1, SchemaId, RegistryClient),
    Context2 = publish_context({hex, Encoded2}),
    Expected2 = Data1,
    ?assertMatch(
        {200, #{<<"decoded">> := Expected2}},
        dryrun_rule(SQL2, Context2),
        #{expected => Expected2}
    ),

    %% encode and tag using a schema registered in emqx
    SchemaName3 = <<"my_schema">>,
    Params3 = #{
        <<"type">> => <<"avro">>,
        <<"source">> => emqx_utils_json:encode(#{
            <<"type">> => <<"record">>,
            <<"name">> => <<"apitest">>,
            <<"fields">> => [
                #{<<"name">> => <<"i">>, <<"type">> => <<"int">>},
                #{<<"name">> => <<"s">>, <<"type">> => <<"string">>}
            ]
        }),
        <<"name">> => SchemaName3,
        <<"description">> => <<"My schema">>
    },
    {ok, 201, _} = request({post, Params3}),

    Subject3 = <<"subj3">>,
    SQL3 = sql(
        <<
            "select bin2hexstr(schema_encode_and_tag("
            "    '${.schema}', '${.registry}', json_decode(payload), '${.subject}')) as encoded"
            " from \"t\" "
        >>,
        #{schema => SchemaName3, registry => Name1, subject => Subject3}
    ),
    Data3 = #{<<"i">> => 10, <<"s">> => <<"abc">>},
    Context3 = publish_context({json, Data3}),
    Expected3 = bin2hex(iolist_to_binary(emqx_schema_registry_serde:encode(SchemaName3, Data3))),
    %% 40 bits => 5 * 2 hex digits
    MagicTagHexSize = 10,
    ?assertMatch(
        {200, #{<<"encoded">> := <<_Tag3:MagicTagHexSize/binary, Expected3/binary>>}},
        dryrun_rule(SQL3, Context3),
        #{expected => Expected3}
    ),

    %% decode tagged data on the fly
    SQL4 = sql(
        <<
            "select schema_decode_tagged("
            "    '${.registry}', hexstr2bin(payload)) as decoded"
            " from \"t\" "
        >>,
        #{registry => Name1}
    ),
    Encoded4 = confluent_encode_and_tag(Data1, SchemaId, RegistryClient),
    Context4 = publish_context({hex, Encoded4}),
    Expected4 = Data1,
    ?assertMatch(
        {200, #{<<"decoded">> := Expected4}},
        dryrun_rule(SQL4, Context4),
        #{expected => Expected4}
    ),

    ok.

%% Smoke test for registering and using an external HTTP serde.
t_external_http_serde(_Config) ->
    {ok, Port} = start_external_http_serde_server(),
    SchemaName = <<"my_external_http_serde">>,
    Params = mk_external_http_create_params(#{name => SchemaName, port => Port}),
    ?assertMatch(
        {201, #{
            <<"parameters">> := #{
                <<"connect_timeout">> := <<"2s">>,
                <<"request_timeout">> := <<"1s">>,
                <<"max_inactive">> := <<"1s">>
            }
        }},
        create_schema(Params)
    ),

    NodeBin = atom_to_binary(node()),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"connected">>,
            <<"node_status">> := #{NodeBin := <<"connected">>},
            <<"parameters">> := #{
                <<"connect_timeout">> := <<"2s">>,
                <<"request_timeout">> := <<"1s">>,
                <<"max_inactive">> := <<"1s">>
            }
        }},
        get_schema(SchemaName)
    ),

    %% Roundtrip smoke test
    SQL1 = sql(
        <<
            "select"
            "   schema_encode('${.name}', payload) as encoded,"
            "   schema_decode('${.name}', encoded) as decoded"
            " from \"t\" "
        >>,
        #{name => SchemaName}
    ),
    Data1 = #{<<"f1">> => #{<<"bah">> => 123}},
    Context1 = #{<<"payload">> := EncodedData1} = publish_context({json, Data1}),
    ExpectedEncoded1 = xor_bin($z, EncodedData1),
    ExpectedDecoded1 = EncodedData1,
    ?assertMatch(
        {200, #{
            <<"encoded">> := ExpectedEncoded1,
            <<"decoded">> := ExpectedDecoded1
        }},
        dryrun_rule(SQL1, Context1),
        #{
            expected_encoded => ExpectedEncoded1,
            expected_decoded => ExpectedDecoded1
        }
    ),

    %% Request returns non-200 status code
    emqx_utils_http_test_server:set_handler(fun(Req, State) ->
        Rep = cowboy_req:reply(400, #{}, <<"boom">>, Req),
        {ok, Rep, State}
    end),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"select_and_transform_error">> := #{
                    <<"msg">> := <<"external_http_request_failed">>,
                    <<"reason">> := #{
                        <<"status_code">> := 400,
                        <<"body">> := <<"boom">>
                    }
                }
            }
        }},
        dryrun_rule(SQL1, Context1)
    ),

    %% Response is not a valid base64-encoded binary
    emqx_utils_http_test_server:set_handler(fun(Req, State) ->
        Rep = cowboy_req:reply(200, #{}, <<"çççççç">>, Req),
        {ok, Rep, State}
    end),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"select_and_transform_error">> := #{
                    <<"msg">> := <<"bad_external_http_response_format">>,
                    <<"hint">> := <<"server response is not a valid base64-encoded string">>,
                    <<"reason">> := <<"badarg">>,
                    <<"response">> := <<_/binary>>
                }
            }
        }},
        dryrun_rule(SQL1, Context1)
    ),

    %% Request times out
    emqx_utils_http_test_server:set_handler(fun(Req, State) ->
        ct:sleep({seconds, 2}),
        Rep = cowboy_req:reply(200, #{}, <<"">>, Req),
        {ok, Rep, State}
    end),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"select_and_transform_error">> := #{
                    <<"msg">> := <<"external_http_request_failed">>,
                    <<"reason">> := <<"timeout">>
                }
            }
        }},
        dryrun_rule(SQL1, Context1)
    ),

    %% Update resource
    NewURL = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary, "/sr?qp=456">>,
    NewParams0 = mk_external_http_create_params(#{name => SchemaName, port => Port, url => NewURL}),
    NewParams = maps:remove(<<"name">>, NewParams0),
    ?assertMatch([_], emqx_resource:list_group_instances(?SCHEMA_REGISTRY_RESOURCE_GROUP)),
    ?assertMatch(
        {200, #{
            <<"parameters">> := #{
                <<"connect_timeout">> := <<"2s">>,
                <<"request_timeout">> := <<"1s">>,
                <<"max_inactive">> := <<"1s">>
            }
        }},
        update_schema(SchemaName, NewParams)
    ),
    ?assertMatch([_], emqx_resource:list_group_instances(?SCHEMA_REGISTRY_RESOURCE_GROUP)),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"connected">>,
            <<"node_status">> := #{NodeBin := <<"connected">>}
        }},
        get_schema(SchemaName)
    ),
    emqx_utils_http_test_server:set_handler(fun(Req, State) ->
        ?assertMatch([{<<"qp">>, <<"456">>}], cowboy_req:parse_qs(Req)),
        external_http_handler(Req, State)
    end),
    ?assertMatch({200, _}, dryrun_rule(SQL1, Context1)),

    %% Delete resource
    ?assertMatch([_], emqx_resource:list_group_instances(?SCHEMA_REGISTRY_RESOURCE_GROUP)),
    ?assertMatch(
        {{204, _}, {ok, _}},
        ?wait_async_action(
            delete_schema(SchemaName),
            #{?snk_kind := serde_destroyed}
        )
    ),
    ?assertMatch([], emqx_resource:list_group_instances(?SCHEMA_REGISTRY_RESOURCE_GROUP)),
    ?assertMatch(
        {400, #{<<"message">> := #{<<"select_and_transform_error">> := <<"serde_not_found">>}}},
        dryrun_rule(SQL1, Context1)
    ),

    ok.

%% Check that we convert input TLS certificates.
t_external_http_serde_ssl(_Config) ->
    {ok, Port} = start_external_http_serde_server(),
    SchemaName = <<"my_external_http_serde">>,
    Params0 = mk_external_http_create_params(#{name => SchemaName, port => Port}),
    GetPEM = fun(File) ->
        Path = emqx_common_test_helpers:app_path(
            emqx,
            filename:join(["etc", "certs", File])
        ),
        {ok, Contents} = file:read_file(Path),
        Contents
    end,
    Params = emqx_utils_maps:deep_merge(
        Params0,
        #{
            <<"parameters">> => #{
                <<"ssl">> => #{
                    <<"enable">> => true,
                    <<"cacertfile">> => GetPEM("cacert.pem"),
                    <<"certfile">> => GetPEM("cert.pem"),
                    <<"keyfile">> => GetPEM("key.pem")
                }
            }
        }
    ),
    %% Contents were saved to files in data dir.
    DataDir = list_to_binary(emqx:data_dir()),
    DataDirSize = byte_size(DataDir),
    ?assertMatch(
        {201, #{
            <<"parameters">> := #{
                <<"ssl">> := #{
                    <<"enable">> := true,
                    <<"cacertfile">> := <<DataDir:DataDirSize/binary, _/binary>>,
                    <<"certfile">> := <<DataDir:DataDirSize/binary, _/binary>>,
                    <<"keyfile">> := <<DataDir:DataDirSize/binary, _/binary>>
                }
            }
        }},
        create_schema(Params),
        #{data_dir => DataDir}
    ),
    ok.

t_protobuf_bundle(Config) ->
    PrivDir = ?config(priv_dir, Config),
    BundleFilename1 = filename:join([PrivDir, "bundle.tar.gz"]),
    FileList1 = [
        {"a.proto", emqx_schema_registry_SUITE:proto_file(<<"a.proto">>)},
        {"nested/b.proto", emqx_schema_registry_SUITE:proto_file(<<"b.proto">>)},
        {"c.proto", emqx_schema_registry_SUITE:proto_file(<<"c.proto">>)}
    ],
    on_exit(fun() -> file:delete(BundleFilename1) end),
    ok = erl_tar:create(BundleFilename1, FileList1, [compressed]),
    Name = <<"myserde">>,
    Description = <<"my bundle">>,
    %% Create
    ?assertMatch(
        {201, #{
            <<"name">> := Name,
            <<"type">> := <<"protobuf">>,
            <<"description">> := Description,
            <<"source">> := #{
                <<"type">> := <<"bundle">>,
                <<"root_proto_path">> := <<_/binary>>
            }
        }},
        create_schema_protobuf_bundle(#{
            name => Name,
            file => BundleFilename1,
            description => Description,
            root_proto_path => <<"a.proto">>
        })
    ),
    %% Update
    BundleFilename2 = filename:join([PrivDir, "bundle2.tar.gz"]),
    FileList2 = [
        {"some.proto", emqx_schema_registry_SUITE:proto_file(<<"c.proto">>)}
    ],
    on_exit(fun() -> file:delete(BundleFilename2) end),
    ok = erl_tar:create(BundleFilename2, FileList2, [compressed]),
    Description2 = <<"new description">>,
    ?assertMatch(
        {200, #{
            <<"name">> := Name,
            <<"type">> := <<"protobuf">>,
            <<"description">> := Description2,
            <<"source">> := #{
                <<"type">> := <<"bundle">>,
                <<"root_proto_path">> := <<_/binary>>
            }
        }},
        update_schema_protobuf_bundle(#{
            name => Name,
            file => BundleFilename2,
            description => Description2,
            root_proto_path => <<"some.proto">>
        })
    ),
    ok.
