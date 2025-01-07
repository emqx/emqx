%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_scram_restapi_schema).

-behaviour(emqx_authn_schema).

-export([
    fields/1,
    validations/0,
    desc/1,
    refs/0,
    select_union_member/1,
    namespace/0
]).

-include("emqx_auth_http.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() ->
    [?R_REF(scram_restapi_get), ?R_REF(scram_restapi_post)].

select_union_member(
    #{<<"mechanism">> := ?AUTHN_MECHANISM_SCRAM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN} = Value
) ->
    case maps:get(<<"method">>, Value, undefined) of
        <<"get">> ->
            [?R_REF(scram_restapi_get)];
        <<"post">> ->
            [?R_REF(scram_restapi_post)];
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

fields(scram_restapi_get) ->
    [
        {method, #{type => get, required => true, desc => ?DESC(emqx_authn_http_schema, method)}},
        {headers, fun emqx_authn_http_schema:headers_no_content_type/1}
    ] ++ common_fields();
fields(scram_restapi_post) ->
    [
        {method, #{type => post, required => true, desc => ?DESC(emqx_authn_http_schema, method)}},
        {headers, fun emqx_authn_http_schema:headers/1}
    ] ++ common_fields().

desc(scram_restapi_get) ->
    ?DESC(emqx_authn_http_schema, get);
desc(scram_restapi_post) ->
    ?DESC(emqx_authn_http_schema, post);
desc(_) ->
    undefined.

validations() ->
    emqx_authn_http_schema:validations().

common_fields() ->
    emqx_authn_schema:common_fields() ++
        [
            {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_SCRAM)},
            {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
            {algorithm, fun emqx_authn_scram_mnesia_schema:algorithm/1},
            {iteration_count, fun emqx_authn_scram_mnesia_schema:iteration_count/1},
            {url, fun emqx_authn_http_schema:url/1},
            {body,
                hoconsc:mk(typerefl:alias("map", map([{fuzzy, term(), binary()}])), #{
                    required => false, desc => ?DESC(emqx_authn_http_schema, body)
                })},
            {request_timeout, fun emqx_authn_http_schema:request_timeout/1}
        ] ++
        proplists:delete(pool_type, emqx_bridge_http_connector:fields(config)).
