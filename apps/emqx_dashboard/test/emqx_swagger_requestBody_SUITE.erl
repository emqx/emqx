%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_swagger_requestBody_SUITE).

-behaviour(minirest_api).
-behaviour(hocon_schema).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

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

t_object(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> =>
                    #{
                        <<"application/json">> =>
                            #{
                                <<"schema">> =>
                                    #{
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
                                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                                            }}
                                        ],
                                        <<"type">> => object
                                    }
                            }
                    }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, good_ref}],
    validate("/object", Spec, Refs),
    ok.

t_deprecated(_Config) ->
    ?assertMatch(
        [
            #{
                <<"emqx_swagger_requestBody_SUITE.deprecated_ref">> :=
                    #{
                        <<"properties">> :=
                            [
                                {<<"tag1">>, #{
                                    deprecated := true
                                }},
                                {<<"tag2">>, #{
                                    deprecated := true
                                }},
                                {<<"tag3">>, #{
                                    deprecated := false
                                }}
                            ]
                    }
            }
        ],
        emqx_dashboard_swagger:components([{?MODULE, deprecated_ref}], #{})
    ).

t_nonempty_list(_Config) ->
    ?assertMatch(
        [
            #{
                <<"emqx_swagger_requestBody_SUITE.nonempty_list_ref">> :=
                    #{
                        <<"properties">> :=
                            [{<<"list">>, #{items := #{type := string}, type := array}}]
                    }
            }
        ],
        emqx_dashboard_swagger:components([{?MODULE, nonempty_list_ref}], #{})
    ).

t_nest_object(_Config) ->
    GoodRef = <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>,
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> =>
                                #{
                                    required => [<<"timeout">>],
                                    <<"properties">> =>
                                        [
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
                                                <<"properties">> =>
                                                    [
                                                        {<<"good_nest_1">>, #{type => integer}},
                                                        {<<"good_nest_2">>, #{
                                                            <<"$ref">> => GoodRef
                                                        }}
                                                    ],
                                                <<"type">> => object
                                            }},
                                            {<<"inner_ref">>, #{
                                                <<"$ref">> =>
                                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                                            }}
                                        ],
                                    <<"type">> => object
                                }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, good_ref}],
    validate("/nest/object", Spec, Refs),
    ok.

t_local_ref(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> => #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                            }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, good_ref}],
    validate("/ref/local", Spec, Refs),
    ok.

t_remote_ref(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> => #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_remote_schema.ref2">>
                            }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{emqx_swagger_remote_schema, "ref2"}],
    {_, Components} = validate("/ref/remote", Spec, Refs),
    ExpectComponents = [
        #{
            <<"emqx_swagger_remote_schema.ref2">> => #{
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
                ],
                <<"type">> => object
            }
        },
        #{
            <<"emqx_swagger_remote_schema.ref3">> => #{
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
                ],
                <<"type">> => object
            }
        }
    ],
    ?assertEqual(ExpectComponents, Components),
    ok.

t_nest_ref(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> => #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.nest_ref">>
                            }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, nest_ref}],
    ExpectComponents = lists:sort([
        #{
            <<"emqx_swagger_requestBody_SUITE.nest_ref">> => #{
                <<"properties">> => [
                    {<<"env">>, #{enum => [test, dev, prod], type => string}},
                    {<<"another_ref">>, #{
                        description => <<"nest ref">>,
                        <<"$ref">> =>
                            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                    }}
                ],
                <<"type">> => object
            }
        },
        #{
            <<"emqx_swagger_requestBody_SUITE.good_ref">> => #{
                <<"properties">> => [
                    {<<"webhook-host">>, #{
                        default => <<"127.0.0.1:80">>,
                        example => <<"127.0.0.1:80">>,
                        type => string
                    }},
                    {<<"log_dir">>, #{example => <<"var/log/emqx">>, type => string}},
                    {<<"tag">>, #{description => <<"tag">>, type => string}}
                ],
                <<"type">> => object
            }
        }
    ]),
    {_, Components} = validate("/ref/nest/ref", Spec, Refs),
    ?assertEqual(ExpectComponents, Components),
    ok.

t_none_ref(_Config) ->
    Path = "/ref/none",
    ?assertError(
        {failed_to_generate_swagger_spec, ?MODULE, Path, error, _FunctionClause, _Stacktrace},
        emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{})
    ),
    ok.

t_sub_fields(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> => #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.sub_fields">>
                            }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, sub_fields}],
    validate("/fields/sub", Spec, Refs),
    ok.

t_bad_ref(_Config) ->
    Path = "/ref/bad",
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> => #{
                        <<"schema">> =>
                            #{
                                <<"$ref">> =>
                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.bad_ref">>
                            }
                    }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, bad_ref}],
    Fields = fields(bad_ref),
    ?assertThrow(
        {error, #{msg := <<"Object only supports non-empty fields list">>, args := Fields}},
        validate(Path, Spec, Refs)
    ),
    ok.

t_ref_array_with_key(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> =>
                        #{
                            <<"schema">> => #{
                                required => [<<"timeout">>],
                                <<"type">> => object,
                                <<"properties">> =>
                                    [
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
                                        {<<"array_refs">>, #{
                                            items => #{
                                                <<"$ref">> =>
                                                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                                            },
                                            type => array
                                        }}
                                    ]
                            }
                        }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, good_ref}],
    validate("/ref/array/with/key", Spec, Refs),
    ok.

t_ref_array_without_key(_Config) ->
    Spec = #{
        post => #{
            parameters => [],
            requestBody => #{
                <<"content">> => #{
                    <<"application/json">> => #{
                        <<"schema">> =>
                            #{
                                items => #{
                                    <<"$ref">> =>
                                        <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>
                                },
                                type => array
                            }
                    }
                }
            },
            responses => #{<<"200">> => #{description => <<"ok">>}}
        }
    },
    Refs = [{?MODULE, good_ref}],
    validate("/ref/array/without/key", Spec, Refs),
    ok.

t_api_spec(_Config) ->
    {Spec0, _} = emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}),
    Path = "/object",
    Body = #{
        <<"per_page">> => 1,
        <<"timeout">> => <<"infinity">>,
        <<"inner_ref">> => #{
            <<"webhook-host">> => <<"127.0.0.1:80">>,
            <<"log_dir">> => <<"var/log/test">>,
            <<"tag">> => <<"god_tag">>
        }
    },

    Filter0 = filter(Spec0, Path),
    ?assertMatch(
        {ok, #{body := #{<<"timeout">> := <<"infinity">>}}},
        trans_requestBody(Path, Body, Filter0)
    ),

    {Spec1, _} = emqx_dashboard_swagger:spec(
        ?MODULE,
        #{check_schema => true, translate_body => true}
    ),
    Filter1 = filter(Spec1, Path),
    ?assertMatch(
        {ok, #{body := #{<<"timeout">> := infinity}}},
        trans_requestBody(Path, Body, Filter1)
    ).

t_object_trans(_Config) ->
    Path = "/object",
    Body = #{
        <<"per_page">> => 1,
        <<"timeout">> => <<"infinity">>,
        <<"inner_ref">> => #{
            <<"webhook-host">> => <<"127.0.0.1:80">>,
            <<"log_dir">> => <<"var/log/test">>,
            <<"tag">> => <<"god_tag">>
        }
    },
    Expect =
        #{
            bindings => #{},
            query_string => #{},
            body =>
                #{
                    <<"per_page">> => 1,
                    <<"timeout">> => infinity,
                    <<"inner_ref">> => #{
                        <<"log_dir">> => "var/log/test",
                        <<"tag">> => <<"god_tag">>,
                        <<"webhook-host">> => {{127, 0, 0, 1}, 80}
                    }
                }
        },
    {ok, ActualBody} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, ActualBody),
    ok.

t_object_notrans(_Config) ->
    Path = "/object",
    Body = #{
        <<"per_page">> => 1,
        <<"timeout">> => <<"infinity">>,
        <<"inner_ref">> => #{
            <<"webhook-host">> => <<"127.0.0.1:80">>,
            <<"log_dir">> => <<"var/log/test">>,
            <<"tag">> => <<"god_tag">>
        }
    },
    {ok, #{body := ActualBody}} = trans_requestBody(
        Path,
        Body,
        fun emqx_dashboard_swagger:filter_check_request/2
    ),
    ?assertEqual(Body, ActualBody),
    ok.

todo_t_nest_object_check(_Config) ->
    Path = "/nest/object",
    Body = #{
        <<"timeout">> => "10m",
        <<"per_page">> => 10,
        <<"inner_ref">> => #{
            <<"webhook-host">> => <<"127.0.0.1:80">>,
            <<"log_dir">> => <<"var/log/test">>,
            <<"tag">> => <<"god_tag">>
        },
        <<"nest_object">> => #{
            <<"good_nest_1">> => 1,
            <<"good_nest_2">> => #{
                <<"webhook-host">> => <<"127.0.0.1:80">>,
                <<"log_dir">> => <<"var/log/test">>,
                <<"tag">> => <<"god_tag">>
            }
        }
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"per_page">> => 10,
            <<"timeout">> => 600
        }
    },
    {ok, NewRequest} = check_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_local_ref_trans(_Config) ->
    Path = "/ref/local",
    Body = #{
        <<"webhook-host">> => <<"127.0.0.1:80">>,
        <<"log_dir">> => <<"var/log/test">>,
        <<"tag">> => <<"A">>
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"log_dir">> => "var/log/test",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => {{127, 0, 0, 1}, 80}
        }
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_remote_ref_trans(_Config) ->
    Path = "/ref/remote",
    Body = #{
        <<"page">> => 10,
        <<"another_ref">> => #{
            <<"version">> => "2.1.0",
            <<"ip">> => <<"198.12.2.1:89">>
        }
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"page">> => 10,
            <<"another_ref">> => #{
                <<"version">> => "2.1.0",
                <<"ip">> => {{198, 12, 2, 1}, 89}
            }
        }
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_nest_ref_trans(_Config) ->
    Path = "/ref/nest/ref",
    Body = #{
        <<"env">> => <<"prod">>,
        <<"another_ref">> => #{
            <<"log_dir">> => "var/log/dev",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => "127.0.0.1:80"
        }
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"another_ref">> => #{
                <<"log_dir">> => "var/log/dev",
                <<"tag">> => <<"A">>,
                <<"webhook-host">> => {{127, 0, 0, 1}, 80}
            },
            <<"env">> => prod
        }
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_ref_array_with_key_trans(_Config) ->
    Path = "/ref/array/with/key",
    Body = #{
        <<"per_page">> => 100,
        <<"timeout">> => "100m",
        <<"array_refs">> => [
            #{
                <<"log_dir">> => "var/log/dev",
                <<"tag">> => <<"A">>,
                <<"webhook-host">> => "127.0.0.1:80"
            },
            #{
                <<"log_dir">> => "var/log/test",
                <<"tag">> => <<"B">>,
                <<"webhook-host">> => "127.0.0.1:81"
            }
        ]
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"per_page">> => 100,
            <<"timeout">> => 6000,
            <<"array_refs">> => [
                #{
                    <<"log_dir">> => "var/log/dev",
                    <<"tag">> => <<"A">>,
                    <<"webhook-host">> => {{127, 0, 0, 1}, 80}
                },
                #{
                    <<"log_dir">> => "var/log/test",
                    <<"tag">> => <<"B">>,
                    <<"webhook-host">> => {{127, 0, 0, 1}, 81}
                }
            ]
        }
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_ref_array_without_key_trans(_Config) ->
    Path = "/ref/array/without/key",
    Body = [
        #{
            <<"log_dir">> => "var/log/dev",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => "127.0.0.1:80"
        },
        #{
            <<"log_dir">> => "var/log/test",
            <<"tag">> => <<"B">>,
            <<"webhook-host">> => "127.0.0.1:81"
        }
    ],
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => [
            #{
                <<"log_dir">> => "var/log/dev",
                <<"tag">> => <<"A">>,
                <<"webhook-host">> => {{127, 0, 0, 1}, 80}
            },
            #{
                <<"log_dir">> => "var/log/test",
                <<"tag">> => <<"B">>,
                <<"webhook-host">> => {{127, 0, 0, 1}, 81}
            }
        ]
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_ref_trans_error(_Config) ->
    Path = "/ref/nest/ref",
    Body = #{
        <<"env">> => <<"prod">>,
        <<"another_ref">> => #{
            <<"log_dir">> => "var/log/dev",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => "127.0..0.1:80"
        }
    },
    {400, 'BAD_REQUEST', _} = trans_requestBody(Path, Body),
    ok.

t_object_trans_error(_Config) ->
    Path = "/object",
    Body = #{
        <<"per_page">> => 99,
        <<"timeout">> => <<"infinity">>,
        <<"inner_ref">> => #{
            <<"webhook-host">> => <<"127.0.0..1:80">>,
            <<"log_dir">> => <<"var/log/test">>,
            <<"tag">> => <<"god_tag">>
        }
    },
    {400, 'BAD_REQUEST', Reason} = trans_requestBody(Path, Body),
    ?assertNotEqual(nomatch, binary:match(Reason, [<<"webhook-host">>])),
    ok.

validate(Path, ExpectSpec, ExpectRefs) ->
    {OperationId, Spec, Refs, #{}} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    ?assertEqual(ExpectSpec, Spec),
    ?assertEqual(ExpectRefs, Refs),
    {Spec, emqx_dashboard_swagger:components(Refs, #{})}.

filter(ApiSpec, Path) ->
    [Filter] = [F || {P, _, _, #{filter := F}} <- ApiSpec, P =:= Path],
    Filter.

trans_requestBody(Path, Body) ->
    trans_requestBody(
        Path,
        Body,
        fun emqx_dashboard_swagger:filter_check_request_and_translate_body/2
    ).

check_requestBody(Path, Body) ->
    trans_requestBody(
        Path,
        Body,
        fun emqx_dashboard_swagger:filter_check_request/2
    ).

trans_requestBody(Path, Body, Filter) ->
    Meta = #{module => ?MODULE, method => post, path => Path},
    Request = #{bindings => #{}, query_string => #{}, body => Body},
    Filter(Request, Meta).

api_spec() -> emqx_dashboard_swagger:spec(?MODULE).
paths() ->
    [
        "/object",
        "/nest/object",
        "/ref/local",
        "/ref/nest/ref",
        "/fields/sub",
        "/ref/array/with/key",
        "/ref/array/without/key"
    ].

schema("/object") ->
    to_schema([
        {per_page, mk(range(1, 100), #{required => true, desc => <<"good per page desc">>})},
        {timeout,
            mk(
                hoconsc:union([infinity, emqx_schema:timeout_duration_s()]),
                #{default => 5, required => true}
            )},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}
    ]);
schema("/nest/object") ->
    to_schema([
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
    ]);
schema("/ref/local") ->
    to_schema(mk(hoconsc:ref(good_ref), #{}));
schema("/fields/sub") ->
    to_schema(mk(hoconsc:ref(sub_fields), #{}));
schema("/ref/remote") ->
    to_schema(mk(hoconsc:ref(emqx_swagger_remote_schema, "ref2"), #{}));
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
        {array_refs, mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{})}
    ]);
schema("/ref/array/without/key") ->
    to_schema(mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{})).

to_schema(Body) ->
    #{
        operationId => test,
        post => #{requestBody => Body, responses => #{200 => <<"ok">>}}
    }.

roots() -> [].

namespace() -> atom_to_list(?MODULE).

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
    };
fields(deprecated_ref) ->
    [
        {tag1, mk(binary(), #{desc => <<"tag1">>, deprecated => {since, "4.3.0"}})},
        {tag2, mk(binary(), #{desc => <<"tag2">>, deprecated => true})},
        {tag3, mk(binary(), #{desc => <<"tag3">>, deprecated => false})}
    ];
fields(nonempty_list_ref) ->
    [
        {list, mk(nonempty_list(binary()), #{})}
    ].

enable(type) -> boolean();
enable(desc) -> <<"Whether to enable tls psk support">>;
enable(default) -> false;
enable(_) -> undefined.

init_file(type) -> binary();
init_file(desc) -> <<"test test desc">>;
init_file(required) -> false;
init_file(_) -> undefined.
