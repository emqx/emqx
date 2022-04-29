%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_schema).

-include("emqx_authz.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-import(hoconsc, [mk/2, enum/1]).
-import(emqx_schema, [mk_duration/2]).

-export([
    fields/1,
    authz_sources_types/1
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

fields(file) ->
    authz_common_fields(file) ++
        [
            {rules, #{
                type => binary(),
                required => true,
                example =>
                    <<"{allow,{username,\"^dashboard?\"},", "subscribe,[\"$SYS/#\"]}.\n",
                        "{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>,
                desc => ?DESC(rules)
            }}
        ];
fields(http_get) ->
    [
        {method, #{type => get, default => get, required => true, desc => ?DESC(method)}},
        {headers, fun headers_no_content_type/1}
    ] ++ authz_http_common_fields();
fields(http_post) ->
    [
        {method, #{type => post, default => post, required => true, desc => ?DESC(method)}},
        {headers, fun headers/1}
    ] ++ authz_http_common_fields();
fields(built_in_database) ->
    authz_common_fields(built_in_database);
fields(mongo_single) ->
    authz_mongo_common_fields() ++
        emqx_connector_mongo:fields(single);
fields(mongo_rs) ->
    authz_mongo_common_fields() ++
        emqx_connector_mongo:fields(rs);
fields(mongo_sharded) ->
    authz_mongo_common_fields() ++
        emqx_connector_mongo:fields(sharded);
fields(mysql) ->
    authz_common_fields(mysql) ++
        [{query, query()}] ++
        proplists:delete(prepare_statement, emqx_connector_mysql:fields(config));
fields(postgresql) ->
    authz_common_fields(postgresql) ++
        [{query, query()}] ++
        proplists:delete(prepare_statement, emqx_connector_pgsql:fields(config));
fields(redis_single) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(single);
fields(redis_sentinel) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(sentinel);
fields(redis_cluster) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(cluster);
fields(position) ->
    [
        {position,
            mk(
                string(),
                #{
                    desc => ?DESC(position),
                    required => true,
                    in => body
                }
            )}
    ].

%%------------------------------------------------------------------------------
%% http type funcs

authz_http_common_fields() ->
    authz_common_fields(http) ++
        [
            {url, fun url/1},
            {body,
                hoconsc:mk(map([{fuzzy, term(), binary()}]), #{
                    required => false, desc => ?DESC(body)
                })},
            {request_timeout,
                mk_duration("Request timeout", #{
                    required => false, default => "30s", desc => ?DESC(request_timeout)
                })}
        ] ++
        maps:to_list(
            maps:without(
                [
                    base_url,
                    pool_type
                ],
                maps:from_list(emqx_connector_http:fields(config))
            )
        ).

url(type) -> binary();
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(desc) -> ?DESC(?FUNCTION_NAME);
url(_) -> undefined.

headers(type) ->
    map();
headers(desc) ->
    ?DESC(?FUNCTION_NAME);
headers(converter) ->
    fun(Headers) ->
        maps:merge(default_headers(), transform_header_name(Headers))
    end;
headers(default) ->
    default_headers();
headers(_) ->
    undefined.

headers_no_content_type(type) ->
    map();
headers_no_content_type(desc) ->
    ?DESC(?FUNCTION_NAME);
headers_no_content_type(converter) ->
    fun(Headers) ->
        maps:without(
            [<<"content-type">>],
            maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
        )
    end;
headers_no_content_type(default) ->
    default_headers_no_content_type();
headers_no_content_type(_) ->
    undefined.

%% headers
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

%%------------------------------------------------------------------------------
%% MonogDB type funcs

authz_mongo_common_fields() ->
    authz_common_fields(mongodb) ++
        [
            {collection, fun collection/1},
            {filter, fun filter/1}
        ].

collection(type) -> binary();
collection(desc) -> ?DESC(?FUNCTION_NAME);
collection(required) -> true;
collection(_) -> undefined.

filter(type) ->
    map();
filter(desc) ->
    ?DESC(?FUNCTION_NAME);
filter(required) ->
    false;
filter(default) ->
    #{};
filter(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Redis type funcs

authz_redis_common_fields() ->
    authz_common_fields(redis) ++
        [
            {cmd,
                mk(binary(), #{
                    required => true,
                    desc => ?DESC(cmd),
                    example => <<"HGETALL mqtt_authz">>
                })}
        ].

%%------------------------------------------------------------------------------
%% Authz api type funcs

authz_common_fields(Type) when is_atom(Type) ->
    [
        {enable, fun enable/1},
        {type, #{
            type => Type,
            default => Type,
            required => true,
            desc => ?DESC(type),
            in => body
        }}
    ].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> ?DESC(?FUNCTION_NAME);
enable(_) -> undefined.

%%------------------------------------------------------------------------------
%% Authz DB query

query() ->
    #{
        type => binary(),
        desc => ?DESC(query),
        required => true,
        validator => fun(S) ->
            case size(S) > 0 of
                true -> ok;
                _ -> {error, "Request query"}
            end
        end
    }.

%%------------------------------------------------------------------------------
%% Internal funcs

authz_sources_types(Type) ->
    case Type of
        simple ->
            [http, mongodb, redis];
        detailed ->
            [
                http_get,
                http_post,
                mongo_single,
                mongo_rs,
                mongo_sharded,
                redis_single,
                redis_sentinel,
                redis_cluster
            ]
    end ++
        [
            built_in_database,
            mysql,
            postgresql,
            file
        ].

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).
