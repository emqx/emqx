%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_swagger_response_SUITE).

-behaviour(minirest_api).
-behaviour(hocon_schema).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

-compile(nowarn_export_all).
-compile(export_all).

-type url() :: emqx_http_lib:uri_map().
-reflect_type([url/0]).
-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
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

t_simple_binary(_config) ->
    Path = "/simple/bin",
    ExpectSpec = #{description => <<"binary ok">>},
    ExpectRefs = [],
    validate(Path, ExpectSpec, ExpectRefs),
    ok.

t_object(_config) ->
    Path = "/object",
    Object =
        #{
            <<"content">> => #{
                <<"application/json">> =>
                    #{
                        <<"schema">> => #{
                            required => [<<"per_page">>, <<"timeout">>],
                            <<"properties">> => [
                                {<<"per_page">>, #{
                                    description => <<"good per page desc">>,
                                    maximum => 100,
                                    minimum => 1,
                                    type => integer
                                }},
                                {<<"timeout">>, #{
                                    default => 5,
                                    <<"oneOf">> =>
                                        [
                                            #{example => <<"1h">>, type => string},
                                            #{enum => [infinity], type => string}
                                        ]
                                }},
                                {<<"inner_ref">>, #{
                                    <<"$ref">> =>
                                        <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                                }}
                            ],
                            <<"type">> => object
                        }
                    }
            }
        },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_error(_Config) ->
    Path = "/error",
    Error400 = #{
        <<"content">> =>
            #{
                <<"application/json">> => #{
                    <<"schema">> => #{
                        <<"type">> => object,
                        <<"properties">> =>
                            [
                                {<<"code">>, #{enum => ['Bad1', 'Bad2'], type => string}},
                                {<<"message">>, #{
                                    description => <<"Bad request desc">>, type => string
                                }}
                            ]
                    }
                }
            }
    },
    Error404 = #{
        <<"content">> =>
            #{
                <<"application/json">> => #{
                    <<"schema">> => #{
                        <<"type">> => object,
                        <<"properties">> =>
                            [
                                {<<"code">>, #{enum => ['Not-Found'], type => string}},
                                {<<"message">>, #{
                                    description => <<"Error code to troubleshoot problems.">>,
                                    type => string
                                }}
                            ]
                    }
                }
            }
    },
    {OperationId, Spec, Refs, #{}} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    Response = maps:get(responses, maps:get(get, Spec)),
    ?assertEqual(Error400, maps:get(<<"400">>, Response)),
    ?assertEqual(Error404, maps:get(<<"404">>, Response)),
    ?assertEqual(#{}, maps:without([<<"400">>, <<"404">>], Response)),
    ?assertEqual([], Refs),
    ok.

t_nest_object(_Config) ->
    Path = "/nest/object",
    Object =
        #{
            <<"content">> => #{
                <<"application/json">> => #{
                    <<"schema">> =>
                        #{
                            required => [<<"timeout">>],
                            <<"type">> => object,
                            <<"properties">> => [
                                {<<"per_page">>, #{
                                    description => <<"good per page desc">>,
                                    maximum => 100,
                                    minimum => 1,
                                    type => integer
                                }},
                                {<<"timeout">>, #{
                                    default => 5,
                                    <<"oneOf">> =>
                                        [
                                            #{example => <<"1h">>, type => string},
                                            #{enum => [infinity], type => string}
                                        ]
                                }},
                                {<<"nest_object">>, #{
                                    <<"type">> => object,
                                    <<"properties">> => [
                                        {<<"good_nest_1">>, #{type => integer}},
                                        {<<"good_nest_2">>, #{
                                            <<"$ref">> =>
                                                <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                                        }}
                                    ]
                                }},
                                {<<"inner_ref">>, #{
                                    <<"$ref">> =>
                                        <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                                }}
                            ]
                        }
                }
            }
        },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_empty(_Config) ->
    ?assertThrow(
        {error, #{
            msg := <<"Object only supports non-empty fields list">>,
            args := [],
            module := ?MODULE
        }},
        validate("/empty", error, [])
    ),
    ok.

t_raw_local_ref(_Config) ->
    Path = "/raw/ref/local",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_raw_remote_ref(_Config) ->
    Path = "/raw/ref/remote",
    Object = #{
        <<"content">> =>
            #{
                <<"application/json">> => #{
                    <<"schema">> => #{
                        <<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>
                    }
                }
            }
    },
    ExpectRefs = [{emqx_swagger_remote_schema, "ref1"}],
    validate(Path, Object, ExpectRefs),
    ok.

t_local_ref(_Config) ->
    Path = "/ref/local",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_remote_ref(_Config) ->
    Path = "/ref/remote",
    Object = #{
        <<"content">> =>
            #{
                <<"application/json">> => #{
                    <<"schema">> => #{
                        <<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>
                    }
                }
            }
    },
    ExpectRefs = [{emqx_swagger_remote_schema, "ref1"}],
    validate(Path, Object, ExpectRefs),
    ok.

t_bad_ref(_Config) ->
    Path = "/ref/bad",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> =>
                    #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.bad_ref">>}
            }
        }
    },
    ExpectRefs = [{?MODULE, bad_ref}],
    ?assertThrow(
        {error, #{
            module := ?MODULE,
            msg := <<"Object only supports non-empty fields list">>
        }},
        validate(Path, Object, ExpectRefs)
    ),
    ok.

t_none_ref(_Config) ->
    Path = "/ref/none",
    ?assertError(
        {failed_to_generate_swagger_spec, ?MODULE, Path, error, _FunctionClause, _Stacktrace},
        validate(Path, #{}, [])
    ),
    ok.

t_nest_ref(_Config) ->
    Path = "/ref/nest/ref",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.nest_ref">>
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, nest_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_sub_fields(_Config) ->
    Path = "/fields/sub",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.sub_fields">>
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, sub_fields}],
    validate(Path, Object, ExpectRefs),
    ok.

t_complex_type(_Config) ->
    Path = "/ref/complex_type",
    {OperationId, Spec, Refs, #{}} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    Response = maps:get(responses, maps:get(post, Spec)),
    ResponseBody = maps:get(<<"200">>, Response),
    Content = maps:get(<<"content">>, ResponseBody),
    JsonContent = maps:get(<<"application/json">>, Content),
    Schema = maps:get(<<"schema">>, JsonContent),
    ?assertMatch(#{<<"type">> := object}, Schema),
    Properties = maps:get(<<"properties">>, Schema),
    ?assertMatch(
        [
            {<<"no_neg_integer">>, #{minimum := 0, type := integer}},
            {<<"url">>, #{
                example := <<"http://127.0.0.1">>, type := string
            }},
            {<<"server">>, #{
                example := <<"127.0.0.1:80">>, type := string
            }},
            {<<"connect_timeout">>, #{
                example := _, type := string
            }},
            {<<"pool_type">>, #{
                enum := [random, hash], type := string
            }},
            {<<"timeout">>, #{
                <<"oneOf">> := [
                    #{example := _, type := string},
                    #{enum := [infinity], type := string}
                ]
            }},
            {<<"bytesize">>, #{
                example := <<"32MB">>, type := string
            }},
            {<<"wordsize">>, #{
                example := <<"1024KB">>, type := string
            }},
            {<<"maps">>, #{example := #{}, type := object}},
            {<<"comma_separated_list">>, #{
                example := <<"item1,item2">>, type := string
            }},
            {<<"comma_separated_atoms">>, #{
                example := <<"item1,item2">>, type := string
            }},
            {<<"log_level">>, #{
                enum := [
                    debug,
                    info,
                    notice,
                    warning,
                    error,
                    critical,
                    alert,
                    emergency,
                    all
                ],
                type := string
            }}
        ],
        Properties
    ),
    ?assertEqual([], Refs),
    ok.

t_ref_array_with_key(_Config) ->
    Path = "/ref/array/with/key",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    required => [<<"timeout">>],
                    <<"type">> => object,
                    <<"properties">> => [
                        {<<"per_page">>, #{
                            description => <<"good per page desc">>,
                            maximum => 100,
                            minimum => 1,
                            type => integer
                        }},
                        {<<"timeout">>, #{
                            default => 5,
                            <<"oneOf">> =>
                                [
                                    #{example => <<"1h">>, type => string},
                                    #{enum => [infinity], type => string}
                                ]
                        }},
                        {<<"assert">>, #{description => <<"money">>, type => number}},
                        {<<"number_ex">>, #{description => <<"number example">>, type => number}},
                        {<<"percent_ex">>, #{
                            description => <<"percent example">>,
                            example => <<"12%">>,
                            type => string
                        }},
                        {<<"duration_ms_ex">>, #{
                            description => <<"duration ms example">>,
                            example => <<"32s">>,
                            type => string
                        }},
                        {<<"atom_ex">>, #{description => <<"atom ex">>, type => string}},
                        {<<"array_refs">>, #{
                            items => #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                            },
                            type => array
                        }}
                    ]
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_ref_array_without_key(_Config) ->
    Path = "/ref/array/without/key",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> => #{
                    items => #{
                        <<"$ref">> =>
                            <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>
                    },
                    type => array
                }
            }
        }
    },
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.
t_hocon_schema_function(_Config) ->
    Path = "/ref/hocon/schema/function",
    Object = #{
        <<"content">> => #{
            <<"application/json">> => #{
                <<"schema">> =>
                    #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.root">>}
            }
        }
    },
    ExpectComponents = [
        #{
            <<"emqx_swagger_remote_schema.ref1">> => #{
                <<"type">> => object,
                <<"properties">> => [
                    {<<"protocol">>, #{enum => [http, https], type => string}},
                    {<<"port">>, #{default => 18083, type => integer}}
                ]
            }
        },
        #{
            <<"emqx_swagger_remote_schema.ref2">> => #{
                <<"type">> => object,
                <<"properties">> => [
                    {<<"page">>, #{
                        description => <<"good page">>,
                        maximum => 100,
                        minimum => 1,
                        type => integer
                    }},
                    {<<"another_ref">>, #{
                        <<"$ref">> =>
                            <<"#/components/schemas/emqx_swagger_remote_schema.ref3">>
                    }}
                ]
            }
        },
        #{
            <<"emqx_swagger_remote_schema.ref3">> => #{
                <<"type">> => object,
                <<"properties">> => [
                    {<<"ip">>, #{
                        description => <<"IP:Port">>,
                        example => <<"127.0.0.1:80">>,
                        type => string
                    }},
                    {<<"version">>, #{
                        description => <<"a good version">>,
                        example => <<"1.0.0">>,
                        type => string
                    }}
                ]
            }
        },
        #{
            <<"emqx_swagger_remote_schema.root">> =>
                #{
                    required => [<<"default_password">>, <<"default_username">>],
                    <<"properties">> => [
                        {<<"listeners">>, #{
                            items =>
                                #{
                                    <<"oneOf">> =>
                                        [
                                            #{
                                                <<"$ref">> =>
                                                    <<"#/components/schemas/emqx_swagger_remote_schema.ref2">>
                                            },
                                            #{
                                                <<"$ref">> =>
                                                    <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>
                                            }
                                        ]
                                },
                            type => array
                        }},
                        {<<"default_username">>, #{default => <<"admin">>, type => string}},
                        {<<"default_password">>, #{default => <<"public">>, type => string}},
                        {<<"sample_interval">>, #{
                            default => <<"10s">>, example => <<"1h">>, type => string
                        }},
                        {<<"token_expired_time">>, #{
                            default => <<"30m">>, example => <<"12m">>, type => string
                        }}
                    ],
                    <<"type">> => object
                }
        }
    ],
    ExpectRefs = [{emqx_swagger_remote_schema, "root"}],
    {_, Components} = validate(Path, Object, ExpectRefs),
    ?assertEqual(ExpectComponents, Components),
    ok.

t_api_spec(_Config) ->
    emqx_dashboard_swagger:spec(?MODULE),
    ok.

api_spec() -> emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    [
        "/simple/bin",
        "/object",
        "/nest/object",
        "/ref/local",
        "/ref/nest/ref",
        "/raw/ref/local",
        "/raw/ref/remote",
        "/ref/array/with/key",
        "/ref/array/without/key",
        "/ref/hocon/schema/function"
    ].

schema("/simple/bin") ->
    to_schema(<<"binary ok">>);
schema("/object") ->
    Object = [
        {per_page, mk(range(1, 100), #{required => true, desc => <<"good per page desc">>})},
        {timeout,
            mk(
                hoconsc:union([infinity, emqx_schema:timeout_duration_s()]),
                #{default => 5, required => true}
            )},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}
    ],
    to_schema(Object);
schema("/nest/object") ->
    Response = [
        {per_page, mk(range(1, 100), #{desc => <<"good per page desc">>})},
        {timeout,
            mk(
                hoconsc:union([infinity, emqx_schema:timeout_duration_s()]),
                #{default => 5, required => true}
            )},
        {nest_object, [
            {good_nest_1, mk(integer(), #{})},
            {good_nest_2, mk(hoconsc:ref(?MODULE, good_ref), #{})}
        ]},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}
    ],
    to_schema(Response);
schema("/empty") ->
    to_schema([]);
schema("/raw/ref/local") ->
    to_schema(hoconsc:ref(good_ref));
schema("/raw/ref/remote") ->
    to_schema(hoconsc:ref(emqx_swagger_remote_schema, "ref1"));
schema("/ref/local") ->
    to_schema(mk(hoconsc:ref(good_ref), #{}));
schema("/ref/remote") ->
    to_schema(mk(hoconsc:ref(emqx_swagger_remote_schema, "ref1"), #{}));
schema("/ref/bad") ->
    to_schema(mk(hoconsc:ref(?MODULE, bad_ref), #{}));
schema("/ref/nest/ref") ->
    to_schema(mk(hoconsc:ref(?MODULE, nest_ref), #{}));
schema("/ref/array/with/key") ->
    to_schema([
        {per_page, mk(range(1, 100), #{desc => <<"good per page desc">>})},
        {timeout,
            mk(
                hoconsc:union([infinity, emqx_schema:timeout_duration_s()]),
                #{default => 5, required => true}
            )},
        {assert, mk(float(), #{desc => <<"money">>})},
        {number_ex, mk(number(), #{desc => <<"number example">>})},
        {percent_ex, mk(emqx_schema:percent(), #{desc => <<"percent example">>})},
        {duration_ms_ex,
            mk(emqx_schema:timeout_duration_ms(), #{desc => <<"duration ms example">>})},
        {atom_ex, mk(atom(), #{desc => <<"atom ex">>})},
        {array_refs, mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{})}
    ]);
schema("/ref/array/without/key") ->
    to_schema(mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{}));
schema("/ref/hocon/schema/function") ->
    to_schema(mk(hoconsc:ref(emqx_swagger_remote_schema, "root"), #{}));
schema("/error") ->
    #{
        operationId => test,
        get => #{
            responses => #{
                400 => emqx_dashboard_swagger:error_codes(['Bad1', 'Bad2'], <<"Bad request desc">>),
                404 => emqx_dashboard_swagger:error_codes(['Not-Found'])
            }
        }
    };
schema("/ref/complex_type") ->
    #{
        operationId => test,
        post => #{
            responses => #{
                200 => [
                    {no_neg_integer, hoconsc:mk(non_neg_integer(), #{})},
                    {url, hoconsc:mk(url(), #{})},
                    {server, hoconsc:mk(emqx_schema:ip_port(), #{})},
                    {connect_timeout, hoconsc:mk(emqx_schema:timeout_duration(), #{})},
                    {pool_type, hoconsc:mk(hoconsc:enum([random, hash]), #{})},
                    {timeout,
                        hoconsc:mk(hoconsc:union([infinity, emqx_schema:timeout_duration()]), #{})},
                    {bytesize, hoconsc:mk(emqx_schema:bytesize(), #{})},
                    {wordsize, hoconsc:mk(emqx_schema:wordsize(), #{})},
                    {maps, hoconsc:mk(map(), #{})},
                    {comma_separated_list, hoconsc:mk(emqx_schema:comma_separated_list(), #{})},
                    {comma_separated_atoms, hoconsc:mk(emqx_schema:comma_separated_atoms(), #{})},
                    {log_level, hoconsc:mk(emqx_conf_schema:log_level(), #{})}
                ]
            }
        }
    };
schema("/fields/sub") ->
    to_schema(hoconsc:ref(sub_fields)).

validate(Path, ExpectObject, ExpectRefs) ->
    {OperationId, Spec, Refs, #{}} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    Response = maps:get(responses, maps:get(post, Spec)),
    ?assertEqual(ExpectObject, maps:get(<<"200">>, Response)),
    ?assertEqual(ExpectObject, maps:get(<<"201">>, Response)),
    ?assertEqual(#{}, maps:without([<<"201">>, <<"200">>], Response)),
    ?assertEqual(ExpectRefs, Refs),
    {Spec, emqx_dashboard_swagger:components(Refs, #{})}.

to_schema(Object) ->
    #{
        operationId => test,
        post => #{responses => #{200 => Object, 201 => Object}}
    }.

roots() -> [].
namespace() -> undefined.

fields(good_ref) ->
    [
        {'webhook-host', mk(emqx_schema:ip_port(), #{default => <<"127.0.0.1:80">>})},
        {log_dir, mk(string(), #{example => "var/log/emqx"})},
        {tag, mk(binary(), #{desc => <<"tag">>})}
    ];
fields(nest_ref) ->
    [
        {env, mk(hoconsc:enum([test, dev, prod]), #{})},
        {another_ref, mk(hoconsc:ref(good_ref), #{desc => "nest ref"})}
    ];
%% don't support maps
fields(bad_ref) ->
    #{
        username => mk(string(), #{}),
        is_admin => mk(boolean(), #{})
    };
fields(sub_fields) ->
    #{
        fields => [
            {enable, fun enable/1},
            {init_file, fun init_file/1}
        ],
        desc => <<"test sub fields">>
    }.

enable(type) -> boolean();
enable(desc) -> <<"Whether to enable tls psk support">>;
enable(default) -> false;
enable(_) -> undefined.

init_file(type) -> binary();
init_file(desc) -> <<"test test desc">>;
init_file(required) -> false;
init_file(_) -> undefined.
