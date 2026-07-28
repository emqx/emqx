%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_http_schema).

-behaviour(emqx_authn_schema).

-export([
    fields/1,
    validations/0,
    desc/1,
    refs/0,
    select_union_member/1,
    namespace/0
]).

-export([
    url/1,
    headers/1,
    headers_no_content_type/1,
    request_timeout/1,
    allowed_hosts/1,
    hostname_resolution/1
]).

-include("emqx_auth_http.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(NOT_EMPTY(MSG), emqx_resource_validator:not_empty(MSG)).
-define(THROW_VALIDATION_ERROR(ERROR, MESSAGE),
    throw(#{
        error => ERROR,
        message => MESSAGE
    })
).

namespace() -> "authn".

refs() ->
    [?R_REF(http_get), ?R_REF(http_post)].

select_union_member(
    #{<<"mechanism">> := ?AUTHN_MECHANISM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN} = Value
) ->
    Method = maps:get(<<"method">>, Value, undefined),
    case Method of
        <<"get">> ->
            [?R_REF(http_get)];
        <<"post">> ->
            [?R_REF(http_post)];
        Else ->
            throw(#{
                reason => "unknown_http_method",
                expected => "get | post",
                field_name => method,
                got => Else
            })
    end;
select_union_member(_Value) ->
    undefined.

fields(http_get) ->
    [
        {method, #{type => get, required => true, desc => ?DESC(method)}},
        {headers, fun headers_no_content_type/1},
        emqx_connector_oauth2_schema:oauth2_field()
    ] ++ common_fields();
fields(http_post) ->
    [
        {method, #{type => post, required => true, desc => ?DESC(method)}},
        {headers, fun headers/1},
        emqx_connector_oauth2_schema:oauth2_field()
    ] ++ common_fields().

desc(http_get) ->
    ?DESC(get);
desc(http_post) ->
    ?DESC(post);
desc(_) ->
    undefined.

validations() ->
    [
        {check_ssl_opts, fun check_ssl_opts/1},
        {check_headers, fun check_headers/1}
    ].

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {url, fun url/1},
        {allowed_hosts, fun allowed_hosts/1},
        {hostname_resolution, fun hostname_resolution/1},
        {pool_size, fun pool_size/1},
        {body,
            hoconsc:mk(typerefl:alias("map", map([{fuzzy, term(), binary()}])), #{
                required => false, desc => ?DESC(body)
            })},
        {request_timeout, fun request_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++
        maps:to_list(
            maps:without(
                [
                    pool_type,
                    pool_size
                ],
                maps:from_list(emqx_bridge_http_connector:fields(config))
            )
        ).

url(type) -> binary();
url(desc) -> ?DESC(?FUNCTION_NAME);
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(_) -> undefined.

allowed_hosts(type) -> hoconsc:array(binary());
allowed_hosts(desc) -> ?DESC(?FUNCTION_NAME);
allowed_hosts(default) -> [];
allowed_hosts(required) -> false;
allowed_hosts(validator) -> fun emqx_auth_http_utils:validate_allowed_hosts_field/1;
allowed_hosts(_) -> undefined.

hostname_resolution(type) -> hoconsc:enum([static, dynamic]);
hostname_resolution(desc) -> ?DESC(?FUNCTION_NAME);
hostname_resolution(default) -> static;
hostname_resolution(required) -> false;
hostname_resolution(_) -> undefined.

pool_size(type) -> non_neg_integer();
pool_size(desc) -> ?DESC(?FUNCTION_NAME);
pool_size(default) -> 8;
pool_size(required) -> false;
pool_size(_) -> undefined.

headers(type) ->
    map();
headers(desc) ->
    ?DESC(?FUNCTION_NAME);
headers(converter) ->
    fun emqx_auth_http_utils:convert_headers/1;
headers(default) ->
    emqx_auth_http_utils:default_headers();
headers(_) ->
    undefined.

headers_no_content_type(type) ->
    map();
headers_no_content_type(desc) ->
    ?DESC(?FUNCTION_NAME);
headers_no_content_type(converter) ->
    fun emqx_auth_http_utils:convert_headers_no_content_type/1;
headers_no_content_type(default) ->
    emqx_auth_http_utils:default_headers_no_content_type();
headers_no_content_type(_) ->
    undefined.

request_timeout(type) -> emqx_schema:duration_ms();
request_timeout(desc) -> ?DESC(?FUNCTION_NAME);
request_timeout(default) -> <<"5s">>;
request_timeout(_) -> undefined.

check_ssl_opts(#{
    backend := ?AUTHN_BACKEND, url := <<"https://", _/binary>>, ssl := #{enable := false}
}) ->
    ?THROW_VALIDATION_ERROR(
        invalid_ssl_opts,
        <<"it's required to enable the TLS option to establish a https connection">>
    );
check_ssl_opts(#{
    <<"backend">> := ?AUTHN_BACKEND,
    <<"url">> := <<"https://", _/binary>>,
    <<"ssl">> := #{<<"enable">> := false}
}) ->
    ?THROW_VALIDATION_ERROR(
        invalid_ssl_opts,
        <<"it's required to enable the TLS option to establish a https connection">>
    );
check_ssl_opts(_) ->
    ok.

check_headers(#{backend := ?AUTHN_BACKEND, headers := Headers, method := get}) ->
    do_check_get_headers(Headers);
check_headers(#{<<"backend">> := ?AUTHN_BACKEND, <<"headers">> := Headers, <<"method">> := get}) ->
    do_check_get_headers(Headers);
check_headers(_) ->
    ok.

do_check_get_headers(Headers) ->
    case maps:is_key(<<"content-type">>, Headers) of
        false ->
            ok;
        true ->
            ?THROW_VALIDATION_ERROR(
                invalid_headers, <<"HTTP GET requests cannot include content-type header.">>
            )
    end.
