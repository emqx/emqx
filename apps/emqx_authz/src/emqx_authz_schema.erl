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

-module(emqx_authz_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-reflect_type([
    permission/0,
    action/0
]).

-type action() :: publish | subscribe | all.
-type permission() :: allow | deny.

-export([
    namespace/0,
    roots/0,
    fields/1,
    validations/0
]).

-export([
    headers_no_content_type/1,
    headers/1
]).

-import(emqx_schema, [mk_duration/2]).
-include_lib("hocon/include/hoconsc.hrl").

%%--------------------------------------------------------------------
%% Hocon Schema
%%--------------------------------------------------------------------

namespace() -> authz.

%% @doc authorization schema is not exported
%% but directly used by emqx_schema
roots() -> [].

fields("authorization") ->
    [
        {sources, #{
            type => union_array(
                [
                    hoconsc:ref(?MODULE, file),
                    hoconsc:ref(?MODULE, http_get),
                    hoconsc:ref(?MODULE, http_post),
                    hoconsc:ref(?MODULE, mnesia),
                    hoconsc:ref(?MODULE, mongo_single),
                    hoconsc:ref(?MODULE, mongo_rs),
                    hoconsc:ref(?MODULE, mongo_sharded),
                    hoconsc:ref(?MODULE, mysql),
                    hoconsc:ref(?MODULE, postgresql),
                    hoconsc:ref(?MODULE, redis_single),
                    hoconsc:ref(?MODULE, redis_sentinel),
                    hoconsc:ref(?MODULE, redis_cluster)
                ]
            ),
            default => [],
            desc =>
                "\n"
                "Authorization data sources.<br>\n"
                "An array of authorization (ACL) data providers.\n"
                "It is designed as an array, not a hash-map, so the sources can be\n"
                "ordered to form a chain of access controls.<br>\n"
                "\n"
                "When authorizing a 'publish' or 'subscribe' action, the configured\n"
                "sources are checked in order. When checking an ACL source,\n"
                "in case the client (identified by username or client ID) is not found,\n"
                "it moves on to the next source. And it stops immediately\n"
                "once an 'allow' or 'deny' decision is returned.<br>\n"
                "\n"
                "If the client is not found in any of the sources,\n"
                "the default action configured in 'authorization.no_match' is applied.<br>\n"
                "\n"
                "NOTE:\n"
                "The source elements are identified by their 'type'.\n"
                "It is NOT allowed to configure two or more sources of the same type.\n"
        }}
    ];
fields(file) ->
    [
        {type, #{type => file}},
        {enable, #{
            type => boolean(),
            default => true
        }},
        {path, #{
            type => string(),
            required => true,
            desc =>
                "\n"
                "Path to the file which contains the ACL rules.<br>\n"
                "If the file provisioned before starting EMQX node,\n"
                "it can be placed anywhere as long as EMQX has read access to it.\n"
                "\n"
                "In case the rule-set is created from EMQX dashboard or management API,\n"
                "the file will be placed in `authz` subdirectory inside EMQX's `data_dir`,\n"
                "and the new rules will override all rules from the old config file.\n"
        }}
    ];
fields(http_get) ->
    [
        {method, #{type => get, default => post}},
        {headers, fun headers_no_content_type/1}
    ] ++ http_common_fields();
fields(http_post) ->
    [
        {method, #{type => post, default => post}},
        {headers, fun headers/1}
    ] ++ http_common_fields();
fields(mnesia) ->
    [
        {type, #{type => 'built_in_database'}},
        {enable, #{
            type => boolean(),
            default => true
        }}
    ];
fields(mongo_single) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(single);
fields(mongo_rs) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(rs);
fields(mongo_sharded) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(sharded);
fields(mysql) ->
    connector_fields(mysql) ++
        [{query, query()}];
fields(postgresql) ->
    [
        {query, query()},
        {type, #{type => postgresql}},
        {enable, #{
            type => boolean(),
            default => true
        }}
    ] ++ emqx_connector_pgsql:fields(config);
fields(redis_single) ->
    connector_fields(redis, single) ++
        [{cmd, query()}];
fields(redis_sentinel) ->
    connector_fields(redis, sentinel) ++
        [{cmd, query()}];
fields(redis_cluster) ->
    connector_fields(redis, cluster) ++
        [{cmd, query()}].

http_common_fields() ->
    [
        {url, fun url/1},
        {request_timeout,
            mk_duration("Request timeout", #{default => "30s", desc => "Request timeout."})},
        {body, #{type => map(), required => false, desc => "HTTP request body."}}
    ] ++
        maps:to_list(
            maps:without(
                [
                    base_url,
                    pool_type
                ],
                maps:from_list(connector_fields(http))
            )
        ).

mongo_common_fields() ->
    [
        {collection, #{
            type => atom(), desc => "`MongoDB` collection containing the authorization data."
        }},
        {selector, #{type => map(), desc => "MQL query used to select the authorization record."}},
        {type, #{type => mongodb, desc => "Database backend."}},
        {enable, #{
            type => boolean(),
            default => true,
            desc => "Enable or disable the backend."
        }}
    ].

validations() ->
    [
        {check_ssl_opts, fun check_ssl_opts/1},
        {check_headers, fun check_headers/1}
    ].

headers(type) ->
    list({binary(), binary()});
headers(desc) ->
    "List of HTTP headers.";
headers(converter) ->
    fun(Headers) ->
        maps:to_list(maps:merge(default_headers(), transform_header_name(Headers)))
    end;
headers(default) ->
    default_headers();
headers(_) ->
    undefined.

headers_no_content_type(type) ->
    list({binary(), binary()});
headers_no_content_type(desc) ->
    "List of HTTP headers.";
headers_no_content_type(converter) ->
    fun(Headers) ->
        maps:to_list(maps:merge(default_headers_no_content_type(), transform_header_name(Headers)))
    end;
headers_no_content_type(default) ->
    default_headers_no_content_type();
headers_no_content_type(_) ->
    undefined.

url(type) -> binary();
url(desc) -> "URL of the auth server.";
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(_) -> undefined.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

check_ssl_opts(Conf) ->
    case hocon_maps:get("config.url", Conf) of
        undefined ->
            true;
        Url ->
            case emqx_authz_http:parse_url(Url) of
                #{scheme := https} ->
                    case hocon_maps:get("config.ssl.enable", Conf) of
                        true -> true;
                        _ -> {error, ssl_not_enable}
                    end;
                #{scheme := http} ->
                    true;
                Bad ->
                    {bad_scheme, Url, Bad}
            end
    end.

check_headers(Conf) ->
    case hocon_maps:get("config.method", Conf) of
        undefined ->
            true;
        Method0 ->
            Method = to_bin(Method0),
            Headers = hocon_maps:get("config.headers", Conf),
            case Method of
                <<"post">> ->
                    true;
                _ when Headers =:= undefined -> true;
                _ when is_list(Headers) ->
                    case lists:member(<<"content-type">>, Headers) of
                        false -> true;
                        true -> {Method0, do_not_include_content_type}
                    end
            end
    end.

union_array(Item) when is_list(Item) ->
    hoconsc:array(hoconsc:union(Item)).

query() ->
    #{
        type => binary(),
        desc => "",
        validator => fun(S) ->
            case size(S) > 0 of
                true -> ok;
                _ -> {error, "Request query"}
            end
        end
    }.

connector_fields(DB) ->
    connector_fields(DB, config).
connector_fields(DB, Fields) ->
    Mod0 = io_lib:format("~ts_~ts", [emqx_connector, DB]),
    Mod =
        try
            list_to_existing_atom(Mod0)
        catch
            error:badarg ->
                list_to_atom(Mod0);
            error:Reason ->
                erlang:error(Reason)
        end,
    [
        {type, #{type => DB, desc => "Database backend."}},
        {enable, #{
            type => boolean(),
            default => true,
            desc => "Enable or disable the backend."
        }}
    ] ++ erlang:apply(Mod, fields, [Fields]).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(B) when is_binary(B) ->
    B;
to_bin(L) when is_list(L) ->
    list_to_binary(L).
