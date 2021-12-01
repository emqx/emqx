-module(emqx_swagger_response_SUITE).

-behaviour(minirest_api).
-behaviour(hocon_schema).

-include_lib("eunit/include/eunit.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

-export([all/0, suite/0, groups/0]).
-export([paths/0, api_spec/0, schema/1, fields/1]).
-export([t_simple_binary/1, t_object/1, t_nest_object/1, t_empty/1, t_error/1,
    t_raw_local_ref/1, t_raw_remote_ref/1, t_hocon_schema_function/1, t_complicated_type/1,
    t_local_ref/1, t_remote_ref/1, t_bad_ref/1, t_none_ref/1, t_nest_ref/1, t_sub_fields/1,
    t_ref_array_with_key/1, t_ref_array_without_key/1, t_api_spec/1]).

all() -> [{group, spec}].
suite() -> [{timetrap, {minutes, 1}}].
groups() -> [
    {spec, [parallel], [
        t_api_spec, t_simple_binary, t_object, t_nest_object, t_error, t_complicated_type,
        t_raw_local_ref, t_raw_remote_ref, t_empty, t_hocon_schema_function,
        t_local_ref, t_remote_ref, t_bad_ref, t_none_ref, t_sub_fields,
        t_ref_array_with_key, t_ref_array_without_key, t_nest_ref]}
].

t_simple_binary(_config) ->
    Path = "/simple/bin",
    ExpectSpec = #{description => <<"binary ok">>},
    ExpectRefs = [],
    validate(Path, ExpectSpec, ExpectRefs),
    ok.

t_object(_config) ->
    Path = "/object",
    Object =
        #{<<"content">> => #{<<"application/json">> =>
        #{<<"schema">> => #{required => [<<"timeout">>, <<"per_page">>],
            <<"properties">> => [
                {<<"per_page">>, #{description => <<"good per page desc">>,
                    example => 1, maximum => 100, minimum => 1, type => integer}},
                {<<"timeout">>, #{default => 5, <<"oneOf">> =>
                [#{example => <<"1h">>, type => string}, #{enum => [infinity], type => string}]}},
                {<<"inner_ref">>, #{<<"$ref">> =>
                <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}}],
            <<"type">> => object}}}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_error(_Config) ->
    Path = "/error",
    Error400 = #{<<"content">> =>
    #{<<"application/json">> => #{<<"schema">> => #{<<"type">> => object,
        <<"properties">> =>
        [
            {<<"code">>, #{enum => ['Bad1', 'Bad2'], type => string}},
            {<<"message">>, #{description => <<"Details description of the error.">>,
                example => <<"Bad request desc">>, type => string}}]
    }}}},
    Error404 = #{<<"content">> =>
    #{<<"application/json">> => #{<<"schema">> => #{<<"type">> => object,
        <<"properties">> =>
        [
            {<<"code">>, #{enum => ['Not-Found'], type => string}},
            {<<"message">>, #{description => <<"Details description of the error.">>,
                example => <<"Error code to troubleshoot problems.">>, type => string}}]
    }}}},
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path),
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
        #{<<"content">> => #{<<"application/json">> => #{<<"schema">> =>
        #{required => [<<"timeout">>], <<"type">> => object, <<"properties">> => [
            {<<"per_page">>, #{description => <<"good per page desc">>, example => 1,
                maximum => 100, minimum => 1, type => integer}},
            {<<"timeout">>, #{default => 5, <<"oneOf">> =>
            [#{example => <<"1h">>, type => string}, #{enum => [infinity], type => string}]}},
            {<<"nest_object">>, #{<<"type">> => object, <<"properties">> => [
                {<<"good_nest_1">>, #{example => 100, type => integer}},
                {<<"good_nest_2">>, #{<<"$ref">> =>
                <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}
                }]}},
            {<<"inner_ref">>, #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}}]
        }}}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_empty(_Config) ->
    ?assertThrow({error,
        #{msg := <<"Object only supports not empty proplists">>,
            args := [], module := ?MODULE}}, validate("/empty", error, [])),
    ok.

t_raw_local_ref(_Config) ->
    Path = "/raw/ref/local",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}}}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_raw_remote_ref(_Config) ->
    Path = "/raw/ref/remote",
    Object = #{<<"content">> =>
    #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>}}}},
    ExpectRefs = [{emqx_swagger_remote_schema, "ref1"}],
    validate(Path, Object, ExpectRefs),
    ok.

t_local_ref(_Config) ->
    Path = "/ref/local",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}}}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_remote_ref(_Config) ->
    Path = "/ref/remote",
    Object = #{<<"content">> =>
    #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>}}}},
    ExpectRefs = [{emqx_swagger_remote_schema, "ref1"}],
    validate(Path, Object, ExpectRefs),
    ok.

t_bad_ref(_Config) ->
    Path = "/ref/bad",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> =>
    #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.bad_ref">>}}}},
    ExpectRefs = [{?MODULE, bad_ref}],
    ?assertThrow({error, #{module := ?MODULE,
        msg := <<"Object only supports not empty proplists">>}},
        validate(Path, Object, ExpectRefs)),
    ok.

t_none_ref(_Config) ->
    Path = "/ref/none",
    ?assertThrow({error, #{mfa := {?MODULE, schema, ["/ref/none"]},
        reason := function_clause}}, validate(Path, #{}, [])),
    ok.

t_nest_ref(_Config) ->
    Path = "/ref/nest/ref",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.nest_ref">>}}}},
    ExpectRefs = [{?MODULE, nest_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_sub_fields(_Config) ->
    Path = "/fields/sub",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        <<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.sub_fields">>}}}},
    ExpectRefs = [{?MODULE, sub_fields}],
    validate(Path, Object, ExpectRefs),
    ok.

t_complicated_type(_Config) ->
    Path = "/ref/complicated_type",
    Object = #{<<"content">> => #{<<"application/json">> =>
    #{<<"schema">> => #{<<"properties">> =>
    [
        {<<"no_neg_integer">>, #{example => 100, minimum => 1, type => integer}},
        {<<"url">>, #{example => <<"http://127.0.0.1">>, type => string}},
        {<<"server">>, #{example => <<"127.0.0.1:80">>, type => string}},
        {<<"connect_timeout">>, #{example => infinity, <<"oneOf">> => [
            #{example => infinity, type => string},
            #{example => 100, type => integer}]}},
        {<<"pool_type">>, #{enum => [random, hash], example => hash, type => string}},
        {<<"timeout">>, #{example => infinity,
            <<"oneOf">> =>
            [#{example => infinity, type => string}, #{example => 100, type => integer}]}},
        {<<"bytesize">>, #{example => <<"32MB">>, type => string}},
        {<<"wordsize">>, #{example => <<"1024KB">>, type => string}},
        {<<"maps">>, #{example => #{}, type => object}},
        {<<"comma_separated_list">>, #{example => <<"item1,item2">>, type => string}},
        {<<"comma_separated_atoms">>, #{example => <<"item1,item2">>, type => string}},
        {<<"log_level">>,
            #{enum => [debug, info, notice, warning, error, critical, alert, emergency, all],
                type => string}},
        {<<"fix_integer">>, #{default => 100, enum => [100], example => 100,type => integer}}
    ],
        <<"type">> => object}}}},
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path),
    ?assertEqual(test, OperationId),
    Response = maps:get(responses, maps:get(post, Spec)),
    ?assertEqual(Object, maps:get(<<"200">>, Response)),
    ?assertEqual([], Refs),
    ok.


t_ref_array_with_key(_Config) ->
    Path = "/ref/array/with/key",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        required => [<<"timeout">>], <<"type">> => object, <<"properties">> => [
            {<<"per_page">>, #{description => <<"good per page desc">>,
                example => 1, maximum => 100, minimum => 1, type => integer}},
            {<<"timeout">>, #{default => 5, <<"oneOf">> =>
            [#{example => <<"1h">>, type => string}, #{enum => [infinity], type => string}]}},
            {<<"assert">>, #{description => <<"money">>, example => 3.14159, type => number}},
            {<<"number_ex">>, #{description => <<"number example">>,
                example => 42, type => number}},
            {<<"percent_ex">>, #{description => <<"percent example">>,
                example => <<"12%">>, type => number}},
            {<<"duration_ms_ex">>, #{description => <<"duration ms example">>,
                example => <<"32s">>, type => string}},
            {<<"atom_ex">>, #{description => <<"atom ex">>, example => atom, type => string}},
            {<<"array_refs">>, #{items => #{<<"$ref">> =>
            <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>}, type => array}}
        ]}
    }}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.

t_ref_array_without_key(_Config) ->
    Path = "/ref/array/without/key",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => #{
        items => #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_response_SUITE.good_ref">>},
        type => array}}}},
    ExpectRefs = [{?MODULE, good_ref}],
    validate(Path, Object, ExpectRefs),
    ok.
t_hocon_schema_function(_Config) ->
    Path = "/ref/hocon/schema/function",
    Object = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> =>
    #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.root">>}}}},
    ExpectComponents = [
        #{<<"emqx_swagger_remote_schema.ref1">> => #{<<"type">> => object,
            <<"properties">> => [
                {<<"protocol">>, #{enum => [http, https], type => string}},
                {<<"port">>, #{default => 18083, example => 100, type => integer}}]
        }},
        #{<<"emqx_swagger_remote_schema.ref2">> => #{<<"type">> => object,
            <<"properties">> => [
                {<<"page">>, #{description => <<"good page">>,
                    example => 1, maximum => 100, minimum => 1, type => integer}},
                {<<"another_ref">>, #{<<"$ref">> =>
                <<"#/components/schemas/emqx_swagger_remote_schema.ref3">>}}
            ]
        }},
        #{<<"emqx_swagger_remote_schema.ref3">> => #{<<"type">> => object,
            <<"properties">> => [
                {<<"ip">>, #{description => <<"IP:Port">>,
                    example => <<"127.0.0.1:80">>, type => string}},
                {<<"version">>, #{description => <<"a good version">>,
                    example => <<"1.0.0">>, type => string}}]
        }},
        #{<<"emqx_swagger_remote_schema.root">> =>
        #{required => [<<"default_password">>, <<"default_username">>],
            <<"properties">> => [{<<"listeners">>, #{items =>
            #{<<"oneOf">> =>
            [#{<<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref2">>},
                #{<<"$ref">> => <<"#/components/schemas/emqx_swagger_remote_schema.ref1">>}]},
                type => array}},
                {<<"default_username">>,
                    #{default => <<"admin">>, example => <<"string-example">>, type => string}},
                {<<"default_password">>,
                    #{default => <<"public">>, example => <<"string-example">>, type => string}},
                {<<"sample_interval">>,
                    #{default => <<"10s">>, example => <<"1h">>, type => string}},
                {<<"token_expired_time">>,
                    #{default => <<"30m">>, example => <<"12m">>, type => string}}],
            <<"type">> => object}}],
    ExpectRefs = [{emqx_swagger_remote_schema, "root"}],
    {_, Components} = validate(Path, Object, ExpectRefs),
    ?assertEqual(ExpectComponents, Components),
    ok.

t_api_spec(_Config) ->
    emqx_dashboard_swagger:spec(?MODULE),
    ok.

api_spec() -> emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    ["/simple/bin", "/object", "/nest/object", "/ref/local",
        "/ref/nest/ref", "/raw/ref/local", "/raw/ref/remote",
        "/ref/array/with/key", "/ref/array/without/key",
        "/ref/hocon/schema/function"].

schema("/simple/bin") ->
    to_schema(<<"binary ok">>);
schema("/object") ->
    Object = [
        {per_page, mk(range(1, 100), #{nullable => false, desc => <<"good per page desc">>})},
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, nullable => false})},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}
    ],
    to_schema(Object);
schema("/nest/object") ->
    Response = [
        {per_page, mk(range(1, 100), #{desc => <<"good per page desc">>})},
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, nullable => false})},
        {nest_object, [
            {good_nest_1, mk(integer(), #{})},
            {good_nest_2, mk(hoconsc:ref(?MODULE, good_ref), #{})}
        ]},
        {inner_ref, mk(hoconsc:ref(?MODULE, good_ref), #{})}],
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
        {timeout, mk(hoconsc:union([infinity, emqx_schema:duration_s()]),
            #{default => 5, required => true})},
        {assert, mk(float(), #{desc => <<"money">>})},
        {number_ex, mk(number(), #{desc => <<"number example">>})},
        {percent_ex, mk(emqx_schema:percent(), #{desc => <<"percent example">>})},
        {duration_ms_ex, mk(emqx_schema:duration_ms(), #{desc => <<"duration ms example">>})},
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
        get => #{responses => #{
            400 => emqx_dashboard_swagger:error_codes(['Bad1', 'Bad2'], <<"Bad request desc">>),
            404 => emqx_dashboard_swagger:error_codes(['Not-Found'])
        }}
    };
schema("/ref/complicated_type") ->
    #{
        operationId => test,
        post => #{responses => #{
            200 => [
                {no_neg_integer, hoconsc:mk(non_neg_integer(), #{})},
                {url, hoconsc:mk(emqx_connector_http:url(), #{})},
                {server, hoconsc:mk(emqx_connector_redis:server(), #{})},
                {connect_timeout, hoconsc:mk(emqx_connector_http:connect_timeout(), #{})},
                {pool_type, hoconsc:mk(emqx_connector_http:pool_type(), #{})},
                {timeout, hoconsc:mk(timeout(), #{})},
                {bytesize, hoconsc:mk(emqx_schema:bytesize(), #{})},
                {wordsize, hoconsc:mk(emqx_schema:wordsize(), #{})},
                {maps, hoconsc:mk(map(), #{})},
                {comma_separated_list, hoconsc:mk(emqx_schema:comma_separated_list(), #{})},
                {comma_separated_atoms, hoconsc:mk(emqx_schema:comma_separated_atoms(), #{})},
                {log_level, hoconsc:mk(emqx_conf_schema:log_level(), #{})},
                {fix_integer, hoconsc:mk(typerefl:integer(100), #{})}
            ]
        }}
    };
schema("/fields/sub") ->
    to_schema(hoconsc:ref(sub_fields)).

validate(Path, ExpectObject, ExpectRefs) ->
    {OperationId, Spec, Refs} = emqx_dashboard_swagger:parse_spec_ref(?MODULE, Path),
    ?assertEqual(test, OperationId),
    Response = maps:get(responses, maps:get(post, Spec)),
    ?assertEqual(ExpectObject, maps:get(<<"200">>, Response)),
    ?assertEqual(ExpectObject, maps:get(<<"201">>, Response)),
    ?assertEqual(#{}, maps:without([<<"201">>, <<"200">>], Response)),
    ?assertEqual(ExpectRefs, Refs),
    {Spec, emqx_dashboard_swagger:components(Refs)}.

to_schema(Object) ->
    #{
        operationId => test,
        post => #{responses => #{200 => Object, 201 => Object}}
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
