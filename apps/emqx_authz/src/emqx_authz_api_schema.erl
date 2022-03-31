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

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-import(hoconsc, [mk/2, enum/1]).
-import(emqx_schema, [mk_duration/2]).

-export([fields/1, authz_sources_types/1]).

fields(http) ->
    authz_common_fields(http) ++
        [
            {url, fun url/1},
            {method, #{
                type => enum([get, post]),
                default => get
            }},
            {headers, fun headers/1},
            {body, map([{fuzzy, term(), binary()}])},
            {request_timeout, mk_duration("Request timeout", #{default => "30s"})}
        ] ++
        maps:to_list(
            maps:without(
                [
                    base_url,
                    pool_type
                ],
                maps:from_list(emqx_connector_http:fields(config))
            )
        );
fields('built_in_database') ->
    authz_common_fields('built_in_database');
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
        [{query, #{type => binary()}}] ++
        emqx_connector_mysql:fields(config);
fields(postgresql) ->
    authz_common_fields(postgresql) ++
        [{query, #{type => binary()}}] ++
        proplists:delete(named_queries, emqx_connector_pgsql:fields(config));
fields(redis_single) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(single);
fields(redis_sentinel) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(sentinel);
fields(redis_cluster) ->
    authz_redis_common_fields() ++
        emqx_connector_redis:fields(cluster);
fields(file) ->
    authz_common_fields(file) ++
        [
            {rules, #{
                type => binary(),
                required => true,
                example =>
                    <<"{allow,{username,\"^dashboard?\"},", "subscribe,[\"$SYS/#\"]}.\n",
                        "{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>
            }}
        ];
fields(position) ->
    [
        {position,
            mk(
                string(),
                #{
                    desc => <<"Where to place the source">>,
                    required => true,
                    in => body
                }
            )}
    ].

%%------------------------------------------------------------------------------
%% http type funcs

url(type) -> binary();
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(_) -> undefined.

headers(type) ->
    map();
headers(converter) ->
    fun(Headers) ->
        maps:merge(default_headers(), transform_header_name(Headers))
    end;
headers(default) ->
    default_headers();
headers(_) ->
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
            {selector, fun selector/1}
        ].

collection(type) -> binary();
collection(_) -> undefined.

selector(type) -> map();
selector(_) -> undefined.

%%------------------------------------------------------------------------------
%% Redis type funcs

authz_redis_common_fields() ->
    authz_common_fields(redis) ++
        [
            {cmd, #{
                type => binary(),
                example => <<"HGETALL mqtt_authz">>
            }}
        ].

%%------------------------------------------------------------------------------
%% Authz api type funcs

authz_common_fields(Type) when is_atom(Type) ->
    [
        {enable, fun enable/1},
        {type, #{
            type => enum([Type]),
            default => Type,
            in => body
        }}
    ].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> "Set to <code>false</code> to disable this auth provider";
enable(_) -> undefined.

%%------------------------------------------------------------------------------
%% Internal funcs

authz_sources_types(Type) ->
    case Type of
        simple ->
            [mongodb, redis];
        detailed ->
            [
                mongo_single,
                mongo_rs,
                mongo_sharded,
                redis_single,
                redis_sentinel,
                redis_cluster
            ]
    end ++
        [
            http,
            'built_in_database',
            mysql,
            postgresql,
            file
        ].

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).
