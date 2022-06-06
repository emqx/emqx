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

-include("emqx_authz.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-reflect_type([
    permission/0,
    action/0
]).

-type action() :: publish | subscribe | all.
-type permission() :: allow | deny.

-import(emqx_schema, [mk_duration/2]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    validations/0,
    desc/1
]).

-export([
    headers_no_content_type/1,
    headers/1
]).

%%--------------------------------------------------------------------
%% Hocon Schema
%%--------------------------------------------------------------------

namespace() -> authz.

%% @doc authorization schema is not exported
%% but directly used by emqx_schema
roots() -> [].

fields("authorization") ->
    Types = [
        file,
        http_get,
        http_post,
        mnesia,
        mongo_single,
        mongo_rs,
        mongo_sharded,
        mysql,
        postgresql,
        redis_single,
        redis_sentinel,
        redis_cluster
    ],
    Unions = [?R_REF(Type) || Type <- Types],
    [
        {sources,
            ?HOCON(
                ?ARRAY(?UNION(Unions)),
                #{
                    default => [],
                    desc => ?DESC(sources)
                }
            )}
    ];
fields(file) ->
    authz_common_fields(file) ++
        [{path, ?HOCON(string(), #{required => true, desc => ?DESC(path)})}];
fields(http_get) ->
    authz_common_fields(http) ++
        http_common_fields() ++
        [
            {method, method(get)},
            {headers, fun headers_no_content_type/1}
        ];
fields(http_post) ->
    authz_common_fields(http) ++
        http_common_fields() ++
        [
            {method, method(post)},
            {headers, fun headers/1}
        ];
fields(mnesia) ->
    authz_common_fields(built_in_database);
fields(mongo_single) ->
    authz_common_fields(mongodb) ++
        mongo_common_fields() ++
        emqx_connector_mongo:fields(single);
fields(mongo_rs) ->
    authz_common_fields(mongodb) ++
        mongo_common_fields() ++
        emqx_connector_mongo:fields(rs);
fields(mongo_sharded) ->
    authz_common_fields(mongodb) ++
        mongo_common_fields() ++
        emqx_connector_mongo:fields(sharded);
fields(mysql) ->
    authz_common_fields(mysql) ++
        connector_fields(mysql) ++
        [{query, query()}];
fields(postgresql) ->
    authz_common_fields(postgresql) ++
        emqx_connector_pgsql:fields(config) ++
        [{query, query()}];
fields(redis_single) ->
    authz_common_fields(redis) ++
        connector_fields(redis, single) ++
        [{cmd, cmd()}];
fields(redis_sentinel) ->
    authz_common_fields(redis) ++
        connector_fields(redis, sentinel) ++
        [{cmd, cmd()}];
fields(redis_cluster) ->
    authz_common_fields(redis) ++
        connector_fields(redis, cluster) ++
        [{cmd, cmd()}];
fields("metrics_status_fields") ->
    [
        {"resource_metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})},
        {"node_resource_metrics", array("node_resource_metrics", "node_metrics")},
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})},
        {"node_metrics", array("node_metrics")},
        {"status", ?HOCON(cluster_status(), #{desc => ?DESC("status")})},
        {"node_status", array("node_status")},
        {"node_error", array("node_error")}
    ];
fields("metrics") ->
    [
        {"total", ?HOCON(integer(), #{desc => ?DESC("metrics_total")})},
        {"allow", ?HOCON(integer(), #{desc => ?DESC("allow")})},
        {"deny", ?HOCON(integer(), #{desc => ?DESC("deny")})},
        {"nomatch", ?HOCON(float(), #{desc => ?DESC("nomatch")})}
    ] ++ common_rate_field();
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("resource_metrics") ->
    common_field();
fields("node_resource_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", ?HOCON(status(), #{desc => ?DESC("node_status")})}
    ];
fields("node_error") ->
    [
        node_name(),
        {"error", ?HOCON(string(), #{desc => ?DESC("node_error")})}
    ].

common_field() ->
    [
        {"matched", ?HOCON(integer(), #{desc => ?DESC("matched")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("failed")})}
    ] ++ common_rate_field().

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

cluster_status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", ?HOCON(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})}.

desc(?CONF_NS) ->
    ?DESC(?CONF_NS);
desc(file) ->
    ?DESC(file);
desc(http_get) ->
    ?DESC(http_get);
desc(http_post) ->
    ?DESC(http_post);
desc(mnesia) ->
    ?DESC(mnesia);
desc(mongo_single) ->
    ?DESC(mongo_single);
desc(mongo_rs) ->
    ?DESC(mongo_rs);
desc(mongo_sharded) ->
    ?DESC(mongo_sharded);
desc(mysql) ->
    ?DESC(mysql);
desc(postgresql) ->
    ?DESC(postgresql);
desc(redis_single) ->
    ?DESC(redis_single);
desc(redis_sentinel) ->
    ?DESC(redis_sentinel);
desc(redis_cluster) ->
    ?DESC(redis_cluster);
desc(_) ->
    undefined.

authz_common_fields(Type) ->
    [
        {type, ?HOCON(Type, #{required => true, desc => ?DESC(type)})},
        {enable, ?HOCON(boolean(), #{default => true, desc => ?DESC(enable)})}
    ].

http_common_fields() ->
    [
        {url, fun url/1},
        {request_timeout,
            mk_duration("Request timeout", #{
                required => false, default => "30s", desc => ?DESC(request_timeout)
            })},
        {body, ?HOCON(map(), #{required => false, desc => ?DESC(body)})}
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
        {collection,
            ?HOCON(atom(), #{
                required => true,
                desc => ?DESC(collection)
            })},
        {filter,
            ?HOCON(map(), #{
                required => false,
                default => #{},
                desc => ?DESC(filter)
            })}
    ].

validations() ->
    [
        {check_ssl_opts, fun check_ssl_opts/1}
    ].

headers(type) ->
    list({binary(), binary()});
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
    list({binary(), binary()});
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
    Sources = hocon_maps:get("authorization.sources", Conf, []),
    lists:foreach(
        fun
            (#{<<"url">> := Url} = Source) ->
                case emqx_authz_http:parse_url(Url) of
                    {<<"https", _/binary>>, _, _} ->
                        case emqx_map_lib:deep_find([<<"ssl">>, <<"enable">>], Source) of
                            {ok, true} -> true;
                            {ok, false} -> throw({ssl_not_enable, Url});
                            _ -> throw({ssl_enable_not_found, Url})
                        end;
                    {<<"http", _/binary>>, _, _} ->
                        ok;
                    Bad ->
                        throw({bad_scheme, Url, Bad})
                end;
            (_Source) ->
                ok
        end,
        Sources
    ).

query() ->
    ?HOCON(binary(), #{
        desc => ?DESC(query),
        required => true,
        validator => fun(S) ->
            case size(S) > 0 of
                true -> ok;
                _ -> {error, "Request query"}
            end
        end
    }).

cmd() ->
    ?HOCON(binary(), #{
        desc => ?DESC(cmd),
        required => true,
        validator => fun(S) ->
            case size(S) > 0 of
                true -> ok;
                _ -> {error, "Request query"}
            end
        end
    }).

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
    erlang:apply(Mod, fields, [Fields]).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).

common_rate_field() ->
    [
        {"rate", ?HOCON(float(), #{desc => ?DESC("rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("rate_last5m")})}
    ].

method(Method) ->
    ?HOCON(Method, #{default => Method, required => true, desc => ?DESC(method)}).

array(Ref) -> array(Ref, Ref).

array(Ref, DescId) ->
    ?HOCON(?ARRAY(?R_REF(Ref)), #{desc => ?DESC(DescId)}).
