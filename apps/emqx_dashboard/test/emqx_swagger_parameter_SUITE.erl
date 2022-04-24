-module(emqx_swagger_parameter_SUITE).
-behaviour(minirest_api).
-behaviour(hocon_schema).

%% API
-export([paths/0, api_spec/0, schema/1, fields/1]).
-export([init_per_suite/1, end_per_suite/1]).
-export([t_in_path/1, t_in_query/1, t_in_mix/1, t_without_in/1, t_ref/1, t_public_ref/1]).
-export([t_require/1, t_nullable/1, t_method/1, t_api_spec/1]).
-export([t_in_path_trans/1, t_in_query_trans/1, t_in_mix_trans/1, t_ref_trans/1]).
-export([t_in_path_trans_error/1, t_in_query_trans_error/1, t_in_mix_trans_error/1]).
-export([all/0, suite/0, groups/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

-define(METHODS, [get, post, put, head, delete, patch, options, trace]).

all() -> [{group, spec}, {group, validation}].
suite() -> [{timetrap, {minutes, 1}}].
groups() ->
    [
        {spec, [parallel], [
            t_api_spec,
            t_in_path,
            t_ref,
            t_in_query,
            t_in_mix,
            t_without_in,
            t_require,
            t_nullable,
            t_method,
            t_public_ref
        ]},
        {validation, [parallel], [
            t_in_path_trans,
            t_ref_trans,
            t_in_query_trans,
            t_in_mix_trans,
            t_in_path_trans_error,
            t_in_query_trans_error,
            t_in_mix_trans_error
        ]}
    ].

init_per_suite(Config) ->
    mria:start(),
    application:load(emqx_dashboard),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_dashboard], fun set_special_configs/1),
    emqx_dashboard:init_i18n(),
    Config.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.

end_per_suite(Config) ->
    end_suite(),
    Config.

end_suite() ->
    application:unload(emqx_management),
    emqx_common_test_helpers:stop_apps([emqx_dashboard]).

t_in_path(_Config) ->
    Expect =
        [
            #{
                description => <<"Indicates which sorts of issues to return">>,
                example => <<"all">>,
                in => path,
                name => filter,
                required => true,
                schema => #{enum => [assigned, created, mentioned, all], type => string}
            }
        ],
    validate("/test/in/:filter", Expect),
    ok.

t_in_query(_Config) ->
    Expect =
        [
            #{
                description => <<"results per page (max 100)">>,
                example => 1,
                in => query,
                name => per_page,
                schema => #{maximum => 100, minimum => 1, type => integer}
            },
            #{
                description => <<"QOS">>,
                in => query,
                name => qos,
                schema => #{enum => [0, 1, 2], type => string}
            }
        ],
    validate("/test/in/query", Expect),
    ok.

t_ref(_Config) ->
    LocalPath = "/test/in/ref/local",
    Path = "/test/in/ref",
    Expect = [#{<<"$ref">> => <<"#/components/parameters/emqx_swagger_parameter_SUITE.page">>}],
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, LocalPath, #{}),
    ?assertEqual(test, OperationId),
    Params = maps:get(parameters, maps:get(post, Spec)),
    ?assertEqual(Expect, Params),
    ?assertEqual([{?MODULE, page, parameter}], Refs),
    ok.

t_public_ref(_Config) ->
    Path = "/test/in/ref/public",
    Expect = [
        #{<<"$ref">> => <<"#/components/parameters/public.page">>},
        #{<<"$ref">> => <<"#/components/parameters/public.limit">>}
    ],
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    Params = maps:get(parameters, maps:get(post, Spec)),
    ?assertEqual(Expect, Params),
    ?assertEqual(
        [
            {emqx_dashboard_swagger, limit, parameter},
            {emqx_dashboard_swagger, page, parameter}
        ],
        Refs
    ),
    ExpectRefs = [
        #{
            <<"public.limit">> => #{
                description => <<"Results per page(max 1000)">>,
                in => query,
                name => limit,
                example => 50,
                schema => #{
                    default => 100,
                    maximum => 1000,
                    minimum => 1,
                    type => integer
                }
            }
        },
        #{
            <<"public.page">> => #{
                description => <<"Page number of the results to fetch.">>,
                in => query,
                name => page,
                example => 1,
                schema => #{default => 1, minimum => 1, type => integer}
            }
        }
    ],
    ?assertEqual(ExpectRefs, emqx_dashboard_swagger:components(Refs, #{})),
    ok.

t_in_mix(_Config) ->
    Expect =
        [
            #{
                description => <<"Indicates which sorts of issues to return">>,
                example => <<"all">>,
                in => query,
                name => filter,
                schema => #{enum => [assigned, created, mentioned, all], type => string}
            },
            #{
                description => <<"Indicates the state of the issues to return.">>,
                example => <<"12m">>,
                in => path,
                name => state,
                required => true,
                schema => #{example => <<"1h">>, type => string}
            },
            #{
                example => 10,
                in => query,
                name => per_page,
                required => false,
                schema => #{default => 5, maximum => 50, minimum => 1, type => integer}
            },
            #{in => query, name => is_admin, schema => #{type => boolean}},
            #{
                in => query,
                name => timeout,
                schema => #{
                    <<"oneOf">> => [
                        #{enum => [infinity], type => string},
                        #{maximum => 60, minimum => 30, type => integer}
                    ]
                }
            }
        ],
    ExpectMeta = #{
        tags => [tags, good],
        description => <<"good description">>,
        summary => <<"good summary">>,
        security => [],
        deprecated => true,
        responses => #{<<"200">> => #{description => <<"ok">>}}
    },
    GotSpec = validate("/test/in/mix/:state", Expect),
    ?assertEqual(ExpectMeta, maps:without([parameters], maps:get(post, GotSpec))),
    ok.

t_without_in(_Config) ->
    ?assertThrow(
        {error, <<"missing in:path/query field in parameters">>},
        emqx_dashboard_swagger:parse_spec_ref(?MODULE, "/test/without/in", #{})
    ),
    ok.

t_require(_Config) ->
    ExpectSpec = [
        #{
            in => query,
            name => userid,
            required => false,
            schema => #{type => string}
        }
    ],
    validate("/required/false", ExpectSpec),
    ok.

t_nullable(_Config) ->
    NullableFalse = [
        #{
            in => query,
            name => userid,
            required => true,
            schema => #{type => string}
        }
    ],
    NullableTrue = [
        #{
            in => query,
            name => userid,
            schema => #{type => string},
            required => false
        }
    ],
    validate("/nullable/false", NullableFalse),
    validate("/nullable/true", NullableTrue),
    ok.

t_method(_Config) ->
    PathOk = "/method/ok",
    PathError = "/method/error",
    {test, Spec, []} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, PathOk, #{}),
    ?assertEqual(lists:sort(?METHODS), lists:sort(maps:keys(Spec))),
    ?assertThrow(
        {error, #{module := ?MODULE, path := PathError, method := bar}},
        emqx_dashboard_swagger:parse_spec_ref(?MODULE, PathError, #{})
    ),
    ok.

t_in_path_trans(_Config) ->
    Path = "/test/in/:filter",
    Bindings = #{filter => <<"created">>},
    Expect =
        {ok, #{
            bindings => #{filter => created},
            body => #{},
            query_string => #{}
        }},
    ?assertEqual(Expect, trans_parameters(Path, Bindings, #{})),
    ok.

t_in_query_trans(_Config) ->
    Path = "/test/in/query",
    Expect =
        {ok, #{
            bindings => #{},
            body => #{},
            query_string => #{<<"per_page">> => 100, <<"qos">> => 1}
        }},
    ?assertEqual(Expect, trans_parameters(Path, #{}, #{<<"per_page">> => 100, <<"qos">> => 1})),
    ok.

t_ref_trans(_Config) ->
    LocalPath = "/test/in/ref/local",
    Path = "/test/in/ref",
    Expect =
        {ok, #{
            bindings => #{},
            body => #{},
            query_string => #{<<"per_page">> => 100}
        }},
    ?assertEqual(Expect, trans_parameters(Path, #{}, #{<<"per_page">> => 100})),
    ?assertEqual(Expect, trans_parameters(LocalPath, #{}, #{<<"per_page">> => 100})),
    {400, 'BAD_REQUEST', Reason} = trans_parameters(Path, #{}, #{<<"per_page">> => 1010}),
    ?assertNotEqual(nomatch, binary:match(Reason, [<<"per_page">>])),
    {400, 'BAD_REQUEST', Reason} = trans_parameters(LocalPath, #{}, #{<<"per_page">> => 1010}),
    ok.

t_in_mix_trans(_Config) ->
    Path = "/test/in/mix/:state",
    Bindings = #{
        state => <<"12m">>,
        per_page => <<"1">>
    },
    Query = #{
        <<"filter">> => <<"created">>,
        <<"is_admin">> => true,
        <<"timeout">> => <<"34">>
    },
    Expect =
        {ok, #{
            body => #{},
            bindings => #{state => 720},
            query_string => #{
                <<"filter">> => created,
                <<"is_admin">> => true,
                <<"per_page">> => 5,
                <<"timeout">> => 34
            }
        }},
    ?assertEqual(Expect, trans_parameters(Path, Bindings, Query)),
    ok.

t_in_path_trans_error(_Config) ->
    Path = "/test/in/:filter",
    Bindings = #{filter => <<"created1">>},
    ?assertMatch({400, 'BAD_REQUEST', _}, trans_parameters(Path, Bindings, #{})),
    ok.

t_in_query_trans_error(_Config) ->
    Path = "/test/in/query",
    {400, 'BAD_REQUEST', Reason} = trans_parameters(Path, #{}, #{<<"per_page">> => 101}),
    ?assertNotEqual(nomatch, binary:match(Reason, [<<"per_page">>])),
    ok.

t_in_mix_trans_error(_Config) ->
    Path = "/test/in/mix/:state",
    Bindings = #{
        state => <<"1d2m">>,
        per_page => <<"1">>
    },
    Query = #{
        <<"filter">> => <<"cdreated">>,
        <<"is_admin">> => true,
        <<"timeout">> => <<"34">>
    },
    ?assertMatch({400, 'BAD_REQUEST', _}, trans_parameters(Path, Bindings, Query)),
    ok.

t_api_spec(_Config) ->
    {Spec0, _} = emqx_dashboard_swagger:spec(?MODULE),
    assert_all_filters_equal(Spec0, undefined),

    {Spec1, _} = emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}),
    assert_all_filters_equal(Spec1, undefined),

    CustomFilter = fun(Request, _RequestMeta) -> {ok, Request} end,
    {Spec2, _} = emqx_dashboard_swagger:spec(?MODULE, #{check_schema => CustomFilter}),
    assert_all_filters_equal(Spec2, CustomFilter),

    {Spec3, _} = emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}),
    Path = "/test/in/:filter",

    Filter = filter(Spec3, Path),
    Bindings = #{filter => <<"created">>},

    ?assertMatch(
        {ok, #{bindings := #{filter := created}}},
        trans_parameters(Path, Bindings, #{}, Filter)
    ).

assert_all_filters_equal(Spec, Filter) ->
    lists:foreach(
        fun({_, _, _, #{filter := F}}) ->
            ?assertEqual(Filter, F)
        end,
        Spec
    ).

validate(Path, ExpectParams) ->
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path, #{}),
    ?assertEqual(test, OperationId),
    Params = maps:get(parameters, maps:get(post, Spec)),
    ?assertEqual(ExpectParams, Params),
    ?assertEqual([], Refs),
    Spec.

filter(ApiSpec, Path) ->
    [Filter] = [F || {P, _, _, #{filter := F}} <- ApiSpec, P =:= Path],
    Filter.

trans_parameters(Path, Bindings, QueryStr) ->
    trans_parameters(Path, Bindings, QueryStr, fun emqx_dashboard_swagger:filter_check_request/2).

trans_parameters(Path, Bindings, QueryStr, Filter) ->
    Meta = #{module => ?MODULE, method => post, path => Path},
    Request = #{bindings => Bindings, query_string => QueryStr, body => #{}},
    Filter(Request, Meta).

api_spec() -> emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    [
        "/test/in/:filter",
        "/test/in/query",
        "/test/in/mix/:state",
        "/test/in/ref",
        "/required/false",
        "/nullable/false",
        "/nullable/true",
        "/method/ok"
    ].

schema("/test/in/:filter") ->
    #{
        operationId => test,
        post => #{
            parameters => [
                {filter,
                    mk(
                        hoconsc:enum([assigned, created, mentioned, all]),
                        #{
                            in => path,
                            desc => <<"Indicates which sorts of issues to return">>,
                            example => "all"
                        }
                    )}
            ],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/in/query") ->
    #{
        operationId => test,
        post => #{
            parameters => [
                {per_page,
                    mk(
                        range(1, 100),
                        #{
                            in => query,
                            desc => <<"results per page (max 100)">>,
                            example => 1
                        }
                    )},
                {qos, mk(emqx_schema:qos(), #{in => query, desc => <<"QOS">>})}
            ],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/in/ref/local") ->
    #{
        operationId => test,
        post => #{
            parameters => [hoconsc:ref(page)],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/in/ref") ->
    #{
        operationId => test,
        post => #{
            parameters => [hoconsc:ref(?MODULE, page)],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/in/ref/public") ->
    #{
        operationId => test,
        post => #{
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/in/mix/:state") ->
    #{
        operationId => test,
        post => #{
            tags => [tags, good],
            desc => <<"good description">>,
            summary => <<"good summary">>,
            security => [],
            deprecated => true,
            parameters => [
                {filter,
                    hoconsc:mk(
                        hoconsc:enum([assigned, created, mentioned, all]),
                        #{
                            in => query,
                            desc => <<"Indicates which sorts of issues to return">>,
                            example => "all"
                        }
                    )},
                {state,
                    mk(
                        emqx_schema:duration_s(),
                        #{
                            in => path,
                            required => true,
                            example => "12m",
                            desc => <<"Indicates the state of the issues to return.">>
                        }
                    )},
                {per_page,
                    mk(
                        range(1, 50),
                        #{in => query, required => false, example => 10, default => 5}
                    )},
                {is_admin, mk(boolean(), #{in => query})},
                {timeout, mk(hoconsc:union([range(30, 60), infinity]), #{in => query})}
            ],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/test/without/in") ->
    #{
        operationId => test,
        post => #{
            parameters => [
                {'x-request-id', mk(binary(), #{})}
            ],
            responses => #{200 => <<"ok">>}
        }
    };
schema("/required/false") ->
    to_schema([{'userid', mk(binary(), #{in => query, required => false})}]);
schema("/nullable/false") ->
    to_schema([{'userid', mk(binary(), #{in => query, required => true})}]);
schema("/nullable/true") ->
    to_schema([{'userid', mk(binary(), #{in => query, required => false})}]);
schema("/method/ok") ->
    Response = #{responses => #{200 => <<"ok">>}},
    lists:foldl(
        fun(Method, Acc) -> Acc#{Method => Response} end,
        #{operationId => test},
        ?METHODS
    );
schema("/method/error") ->
    #{operationId => test, bar => #{200 => <<"ok">>}}.

fields(page) ->
    [
        {per_page,
            mk(
                range(1, 100),
                #{in => query, desc => <<"results per page (max 100)">>, example => 1}
            )}
    ].
to_schema(Params) ->
    #{
        operationId => test,
        post => #{
            parameters => Params,
            responses => #{200 => <<"ok">>}
        }
    }.
