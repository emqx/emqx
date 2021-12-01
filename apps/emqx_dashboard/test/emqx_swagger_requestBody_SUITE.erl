-module(emqx_swagger_requestBody_SUITE).

-behaviour(minirest_api).
-behaviour(hocon_schema).

%% API
-export([paths/0, api_spec/0, schema/1, fields/1]).
-export([t_object/1, t_nest_object/1, t_api_spec/1,
    t_local_ref/1, t_remote_ref/1, t_bad_ref/1, t_none_ref/1, t_nest_ref/1,
    t_ref_array_with_key/1, t_ref_array_without_key/1, t_sub_fields/1
]).
-export([
    t_object_trans/1, t_object_notrans/1, t_nest_object_trans/1, t_local_ref_trans/1,
    t_remote_ref_trans/1, t_nest_ref_trans/1,
    t_ref_array_with_key_trans/1, t_ref_array_without_key_trans/1,
    t_ref_trans_error/1, t_object_trans_error/1
]).
-export([all/0, suite/0, groups/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

all() -> [{group, spec}, {group, validation}].

suite() -> [{timetrap, {minutes, 1}}].
groups() -> [
    {spec, [parallel], [
        t_api_spec, t_object, t_nest_object,
        t_local_ref, t_remote_ref, t_bad_ref, t_none_ref,
        t_ref_array_with_key, t_ref_array_without_key, t_nest_ref]},
    {validation, [parallel],
        [
            t_object_trans, t_object_notrans, t_local_ref_trans, t_remote_ref_trans,
            t_ref_array_with_key_trans, t_ref_array_without_key_trans, t_nest_ref_trans,
            t_ref_trans_error, t_object_trans_error
            %% t_nest_object_trans,
            ]}
].

t_object(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> =>
            #{<<"application/json">> =>
            #{<<"schema">> =>
            #{required => [<<"timeout">>, <<"per_page">>],
                <<"properties">> =>[
                    {<<"per_page">>, #{description => <<"good per page desc">>,
                        example => 1, maximum => 100, minimum => 1, type => integer}},
                    {<<"timeout">>, #{default => 5, <<"oneOf">> =>
                    [#{example => <<"1h">>, type => string},
                        #{enum => [infinity], type => string}]}},
                    {<<"inner_ref">>,
                        #{<<"$ref">> =>
                        <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}}],
                <<"type">> => object}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, good_ref}],
    validate("/object", Spec, Refs),
    ok.

t_nest_object(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> =>
            #{required => [<<"timeout">>],
                <<"properties">> =>
                [{<<"per_page">>, #{description => <<"good per page desc">>,
                    example => 1, maximum => 100, minimum => 1, type => integer}},
                    {<<"timeout">>, #{default => 5, <<"oneOf">> =>
                    [#{example => <<"1h">>, type => string},
                        #{enum => [infinity], type => string}]}},
                    {<<"nest_object">>,
                        #{<<"properties">> =>
                        [{<<"good_nest_1">>, #{example => 100, type => integer}},
                            {<<"good_nest_2">>, #{<<"$ref">> =>
                            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}}],
                            <<"type">> => object}},
                    {<<"inner_ref">>,
                        #{<<"$ref">> =>
                        <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}}],
                <<"type">> => object}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, good_ref}],
    validate("/nest/object", Spec, Refs),
    ok.

t_local_ref(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, good_ref}],
    validate("/ref/local", Spec, Refs),
    ok.

t_remote_ref(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_remote_schema.ref2">>}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{emqx_swagger_remote_schema, "ref2"}],
    {_, Components} = validate("/ref/remote", Spec, Refs),
    ExpectComponents = [
        #{<<"emqx_swagger_remote_schema.ref2">> => #{<<"properties">> => [
            {<<"page">>, #{description => <<"good page">>,example => 1,
                maximum => 100,minimum => 1,type => integer}},
        {<<"another_ref">>, #{<<"$ref">> =>
        <<"#/components/schemas/emqx_swagger_remote_schema.ref3">>}}], <<"type">> => object}},
        #{<<"emqx_swagger_remote_schema.ref3">> => #{<<"properties">> => [
            {<<"ip">>, #{description => <<"IP:Port">>,
                example => <<"127.0.0.1:80">>,type => string}},
            {<<"version">>, #{description => <<"a good version">>,
                example => <<"1.0.0">>,type => string}}],
            <<"type">> => object}}],
    ?assertEqual(ExpectComponents, Components),
    ok.

t_nest_ref(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.nest_ref">>}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, nest_ref}],
    ExpectComponents = lists:sort([
        #{<<"emqx_swagger_requestBody_SUITE.nest_ref">> => #{<<"properties">> => [
            {<<"env">>, #{enum => [test,dev,prod],type => string}},
            {<<"another_ref">>, #{description => <<"nest ref">>,
                <<"$ref">> => <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}}],
            <<"type">> => object}},
        #{<<"emqx_swagger_requestBody_SUITE.good_ref">> => #{<<"properties">> => [
            {<<"webhook-host">>, #{default => <<"127.0.0.1:80">>,
                example => <<"127.0.0.1:80">>,type => string}},
            {<<"log_dir">>, #{example => <<"var/log/emqx">>,type => string}},
            {<<"tag">>, #{description => <<"tag">>,
                example => <<"binary-example">>,type => string}}],
            <<"type">> => object}}]),
    {_, Components} = validate("/ref/nest/ref", Spec, Refs),
    ?assertEqual(ExpectComponents, Components),
    ok.

t_none_ref(_Config) ->
    Path = "/ref/none",
    ?assertThrow({error, #{mfa := {?MODULE, schema, [Path]}}},
        emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path)),
    ok.

t_sub_fields(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.sub_fields">>}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, sub_fields}],
    validate("/fields/sub", Spec, Refs),
    ok.

t_bad_ref(_Config) ->
    Path = "/ref/bad",
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> => #{<<"schema">> =>
            #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_requestBody_SUITE.bad_ref">>}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, bad_ref}],
    Fields = fields(bad_ref),
    ?assertThrow({error, #{msg := <<"Object only supports not empty proplists">>, args := Fields}},
        validate(Path, Spec, Refs)),
    ok.

t_ref_array_with_key(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> =>
            #{<<"schema">> => #{required => [<<"timeout">>],
                <<"type">> => object, <<"properties">> =>
                [
                    {<<"per_page">>, #{description => <<"good per page desc">>,
                        example => 1, maximum => 100, minimum => 1, type => integer}},
                    {<<"timeout">>, #{default => 5, <<"oneOf">> =>
                    [#{example => <<"1h">>, type => string},
                        #{enum => [infinity], type => string}]}},
                    {<<"array_refs">>, #{items => #{<<"$ref">> =>
                    <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>},
                        type => array}}
                ]}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
    Refs = [{?MODULE, good_ref}],
    validate("/ref/array/with/key", Spec, Refs),
    ok.

t_ref_array_without_key(_Config) ->
    Spec = #{
        post => #{parameters => [],
            requestBody => #{<<"content">> => #{<<"application/json">> => #{<<"schema">> =>
            #{items => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_requestBody_SUITE.good_ref">>}, type => array}}}},
            responses => #{<<"200">> => #{description => <<"ok">>}}}},
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
        trans_requestBody(Path, Body, Filter0)),

    {Spec1, _} = emqx_dashboard_swagger:spec(?MODULE,
        #{check_schema => true, translate_body => true}),
    Filter1 = filter(Spec1, Path),
    ?assertMatch(
        {ok, #{body := #{<<"timeout">> := infinity}}},
        trans_requestBody(Path, Body, Filter1)).


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
                    <<"webhook-host">> => {{127, 0, 0, 1}, 80}}
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
    {ok, #{body := ActualBody}} = trans_requestBody(Path, Body,
        fun emqx_dashboard_swagger:filter_check_request/2),
    ?assertEqual(Body, ActualBody),
    ok.

t_nest_object_trans(_Config) ->
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
        body => #{<<"per_page">> => 10,
            <<"timeout">> => 600}
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
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
            <<"ip">> => <<"198.12.2.1:89">>}
    },
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"page">> => 10,
            <<"another_ref">> => #{
                <<"version">> => "2.1.0",
                <<"ip">> => {{198,12,2,1}, 89}}
        }
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_nest_ref_trans(_Config) ->
    Path = "/ref/nest/ref",
    Body = #{<<"env">> => <<"prod">>,
        <<"another_ref">> => #{
            <<"log_dir">> => "var/log/dev",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => "127.0.0.1:80"
        }},
    Expect = #{
        bindings => #{},
        query_string => #{},
        body => #{
            <<"another_ref">> => #{
                <<"log_dir">> => "var/log/dev", <<"tag">> => <<"A">>,
                <<"webhook-host">> => {{127, 0, 0, 1}, 80}},
            <<"env">> => prod}
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
            }]
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
    Body = [#{
        <<"log_dir">> => "var/log/dev",
        <<"tag">> => <<"A">>,
        <<"webhook-host">> => "127.0.0.1:80"
    },
        #{
            <<"log_dir">> => "var/log/test",
            <<"tag">> => <<"B">>,
            <<"webhook-host">> => "127.0.0.1:81"
        }],
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
            }]
    },
    {ok, NewRequest} = trans_requestBody(Path, Body),
    ?assertEqual(Expect, NewRequest),
    ok.

t_ref_trans_error(_Config) ->
    Path = "/ref/nest/ref",
    Body = #{<<"env">> => <<"prod">>,
        <<"another_ref">> => #{
            <<"log_dir">> => "var/log/dev",
            <<"tag">> => <<"A">>,
            <<"webhook-host">> => "127.0..0.1:80"
        }},
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
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path),
    ?assertEqual(test, OperationId),
    ?assertEqual(ExpectSpec, Spec),
    ?assertEqual(ExpectRefs, Refs),
    {Spec, emqx_dashboard_swagger:components(Refs)}.


filter(ApiSpec, Path) ->
    [Filter] = [F || {P, _, _, #{filter := F}} <- ApiSpec, P =:= Path],
    Filter.

trans_requestBody(Path, Body) ->
    trans_requestBody(Path, Body,
        fun emqx_dashboard_swagger:filter_check_request_and_translate_body/2).

trans_requestBody(Path, Body, Filter) ->
    Meta = #{module => ?MODULE, method => post, path => Path},
    Request = #{bindings => #{}, query_string => #{}, body => Body},
    Filter(Request, Meta).

api_spec() -> emqx_dashboard_swagger:spec(?MODULE).
paths() ->
    ["/object", "/nest/object", "/ref/local", "/ref/nest/ref", "/fields/sub",
        "/ref/array/with/key", "/ref/array/without/key"].

schema("/object") ->
    to_schema([
        {per_page, mk(range(1, 100), #{nullable => false, desc => <<"good per page desc">>})},
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, nullable => false})},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}
    ]);
schema("/nest/object") ->
    to_schema([
        {per_page, mk(range(1, 100), #{desc => <<"good per page desc">>})},
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, nullable => false})},
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
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, required => true})},
        {array_refs, mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{})}
    ]);
schema("/ref/array/without/key") ->
    to_schema(mk(hoconsc:array(hoconsc:ref(?MODULE, good_ref)), #{})).

to_schema(Body) ->
    #{
        operationId => test,
        post => #{requestBody => Body, responses => #{200 => <<"ok">>}}
    }.

fields(good_ref) ->
    [
        {'webhook-host', mk(emqx_schema:ip_port(), #{default => "127.0.0.1:80"})},
        {log_dir, mk(emqx_schema:file(), #{example => "var/log/emqx"})},
        {tag, mk(binary(), #{desc => <<"tag">>})}
    ];
fields(nest_ref) ->
    [
        {env, mk(hoconsc:enum([test, dev, prod]), #{})},
        {another_ref, mk(hoconsc:ref(good_ref), #{desc => "nest ref"})}
    ];

fields(bad_ref) -> %% don't support maps
    #{
        username => mk(string(), #{}),
        is_admin => mk(boolean(), #{})
    };
fields(sub_fields) ->
    #{fields => [
        {enable,     fun enable/1},
        {init_file,  fun init_file/1}
    ],
        desc => <<"test sub fields">>}.

enable(type) -> boolean();
enable(desc) -> <<"Whether to enable tls psk support">>;
enable(default) -> false;
enable(_) -> undefined.

init_file(type) -> binary();
init_file(desc) -> <<"test test desc">>;
init_file(nullable) -> true;
init_file(_) -> undefined.
