%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_http_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_schema_registry.hrl").

-define(APPS, [emqx_conf, emqx_schema_registry]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, avro},
        {group, protobuf},
        {group, json}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {avro, AllTCs},
        {protobuf, AllTCs},
        {json, AllTCs}
    ].

init_per_suite(Config) ->
    emqx_config:save_schema_mod_and_names(emqx_schema_registry_schema),
    emqx_mgmt_api_test_util:init_suite(?APPS),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(lists:reverse(?APPS)),
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
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_schemas(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

request(get) ->
    do_request(get, _Parts = [], _Body = []);
request({get, Name}) ->
    do_request(get, _Parts = [Name], _Body = []);
request({delete, Name}) ->
    do_request(delete, _Parts = [Name], _Body = []);
request({put, Name, Params}) ->
    do_request(put, _Parts = [Name], Params);
request({post, Params}) ->
    do_request(post, _Parts = [], Params).

do_request(Method, PathParts, Body) ->
    Header = emqx_common_test_http:default_auth_header(),
    URI = uri(["schema_registry" | PathParts]),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    Res0 = emqx_mgmt_api_test_util:request_api(Method, URI, [], Header, Body, Opts),
    case Res0 of
        {ok, Code, <<>>} ->
            {ok, Code, <<>>};
        {ok, Code, Res1} ->
            Res2 = emqx_utils_json:decode(Res1, [return_maps]),
            Res3 = try_decode_error_message(Res2),
            {ok, Code, Res3};
        Error ->
            Error
    end.

try_decode_error_message(#{<<"message">> := Msg0} = Res0) ->
    case emqx_utils_json:safe_decode(Msg0, [return_maps]) of
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
                <<"{post_config_update,emqx_schema_registry,", _/binary>>
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
                    <<"expected">> := <<"avro | protobuf | json">>,
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
                <<"{post_config_update,emqx_schema_registry,", _/binary>>
        }},
        request({post, Params#{<<"source">> := InvalidSourceBin}})
    ),

    %% unknown serde type
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                #{
                    <<"expected">> := <<"avro | protobuf | json">>,
                    <<"field_name">> := <<"type">>
                }
        }},
        request({post, Params#{<<"type">> := <<"foo">>}})
    ),

    ok.
