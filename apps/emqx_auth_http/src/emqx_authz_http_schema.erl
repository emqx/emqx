%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_http_schema).

-include("emqx_auth_http.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authz_schema).

-export([
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    select_union_member/1,
    namespace/0
]).

-export([
    headers_no_content_type/1,
    headers/1
]).

-define(NOT_EMPTY(MSG), emqx_resource_validator:not_empty(MSG)).

-import(emqx_schema, [mk_duration/2]).

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

source_refs() ->
    [?R_REF(http_get), ?R_REF(http_post)].

fields(http_get) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        http_common_fields() ++
        [
            {method, method(get)},
            {headers, fun headers_no_content_type/1}
        ];
fields(http_post) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        http_common_fields() ++
        [
            {method, method(post)},
            {headers, fun headers/1}
        ].

desc(http_get) ->
    ?DESC(http_get);
desc(http_post) ->
    ?DESC(http_post);
desc(_) ->
    undefined.

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN} = Value) ->
    Method = maps:get(<<"method">>, Value, undefined),
    case Method of
        <<"get">> ->
            ?R_REF(http_get);
        <<"post">> ->
            ?R_REF(http_post);
        Else ->
            throw(#{
                reason => "unknown_http_method",
                expected => "get | post",
                got => Else
            })
    end;
select_union_member(_Value) ->
    undefined.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

method(Method) ->
    ?HOCON(Method, #{required => true, desc => ?DESC(method)}).

http_common_fields() ->
    [
        {url, fun url/1},
        {request_timeout,
            mk_duration("Request timeout", #{
                required => false, default => <<"30s">>, desc => ?DESC(request_timeout)
            })},
        {body, ?HOCON(hoconsc:map(name, binary()), #{required => false, desc => ?DESC(body)})}
    ] ++
        lists:keydelete(
            pool_type,
            1,
            emqx_bridge_http_connector:fields(config)
        ).

headers(type) ->
    typerefl:alias("map", list({binary(), binary()}), #{}, [binary(), binary()]);
headers(desc) ->
    ?DESC(?FUNCTION_NAME);
headers(converter) ->
    fun(Headers) ->
        maps:to_list(maps:merge(default_headers(), transform_header_name(Headers)))
    end;
headers(default) ->
    default_headers();
headers(_) ->
    undefined.

headers_no_content_type(type) ->
    typerefl:alias("map", list({binary(), binary()}), #{}, [binary(), binary()]);
headers_no_content_type(desc) ->
    ?DESC(?FUNCTION_NAME);
headers_no_content_type(converter) ->
    fun(Headers) ->
        maps:to_list(
            maps:without(
                [<<"content-type">>],
                maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
            )
        )
    end;
headers_no_content_type(default) ->
    default_headers_no_content_type();
headers_no_content_type(validator) ->
    fun(Headers) ->
        case lists:keyfind(<<"content-type">>, 1, Headers) of
            false -> ok;
            _ -> {error, do_not_include_content_type}
        end
    end;
headers_no_content_type(_) ->
    undefined.

url(type) -> binary();
url(desc) -> ?DESC(?FUNCTION_NAME);
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(_) -> undefined.

default_headers() ->
    maps:put(
        <<"content-type">>,
        <<"application/json">>,
        default_headers_no_content_type()
    ).

default_headers_no_content_type() ->
    #{
        <<"accept">> => <<"application/json">>,
        <<"cache-control">> => <<"no-cache">>,
        <<"connection">> => <<"keep-alive">>,
        <<"keep-alive">> => <<"timeout=30, max=1000">>
    }.

transform_header_name(Headers) ->
    maps:fold(
        fun(K0, V, Acc) ->
            K = list_to_binary(string:to_lower(to_list(K0))),
            maps:put(K, V, Acc)
        end,
        #{},
        Headers
    ).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).
