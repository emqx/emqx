%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-reflect_type([ permission/0
              , action/0
              , url/0
              ]).

-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

-type action() :: publish | subscribe | all.
-type permission() :: allow | deny.
-type url() :: emqx_http_lib:uri_map().

-export([ namespace/0
        , roots/0
        , fields/1
        , validations/0
        ]).

-import(emqx_schema, [mk_duration/2]).

%%--------------------------------------------------------------------
%% Hocon Schema
%%--------------------------------------------------------------------

namespace() -> authz.

%% @doc authorization schema is not exported
%% but directly used by emqx_schema
roots() -> [].

fields("authorization") ->
    [ {sources, #{type => union_array(
                    [ hoconsc:ref(?MODULE, file)
                    , hoconsc:ref(?MODULE, http_get)
                    , hoconsc:ref(?MODULE, http_post)
                    , hoconsc:ref(?MODULE, mnesia)
                    , hoconsc:ref(?MODULE, mongo_single)
                    , hoconsc:ref(?MODULE, mongo_rs)
                    , hoconsc:ref(?MODULE, mongo_sharded)
                    , hoconsc:ref(?MODULE, mysql)
                    , hoconsc:ref(?MODULE, postgresql)
                    , hoconsc:ref(?MODULE, redis_single)
                    , hoconsc:ref(?MODULE, redis_sentinel)
                    , hoconsc:ref(?MODULE, redis_cluster)
                    ]),
                  default => [],
                  desc =>
"""
Authorization data sources.<br>
An array of authorization (ACL) data providers.
It is designed as an array but not a hash-map so the sources can be
ordered to form a chain of access controls.<br>


When authorizing a publish or subscribe action, the configured
sources are checked in order. When checking an ACL source,
in case the client (identified by username or client ID) is not found,
it moves on to the next source. And it stops immediatly
once an 'allow' or 'deny' decision is returned.<br>

If the client is not found in any of the sources,
the default action configured in 'authorization.no_match' is applied.<br>

NOTE:
The source elements are identified by their 'type'.
It is NOT allowed to configure two or more sources of the same type.
"""
                 }
      }
    ];
fields(file) ->
    [ {type, #{type => file}}
    , {enable, #{type => boolean(),
                 default => true}}
    , {path, #{type => string(),
               desc => """
Path to the file which contains the ACL rules.<br>
If the file provisioned before starting EMQ X node, it can be placed anywhere
as long as EMQ X has read access to it.
In case rule set is created from EMQ X dashboard or management HTTP API,
the file will be placed in `certs/authz` sub directory inside EMQ X's `data_dir`,
and the new rules will override all rules from the old config file.
"""
              }}
    ];
fields(http_get) ->
    [ {method,  #{type => get, default => post}}
    , {headers, fun headers_no_content_type/1}
    ] ++ http_common_fields();
fields(http_post) ->
    [ {method,  #{type => post, default => post}}
    , {headers, fun headers/1}
    ] ++ http_common_fields();
fields(mnesia) ->
    [ {type,   #{type => 'built-in-database'}}
    , {enable, #{type => boolean(),
                 default => true}}
    ];
fields(mongo_single) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(single);
fields(mongo_rs) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(rs);
fields(mongo_sharded) ->
    mongo_common_fields() ++ emqx_connector_mongo:fields(sharded);
fields(mysql) ->
    connector_fields(mysql) ++
    [ {query, query()} ];
fields(postgresql) ->
    [ {query, query()}
    , {type, #{type => postgresql}}
    , {enable, #{type => boolean(),
                 default => true}}
    ] ++ emqx_connector_pgsql:fields(config);
fields(redis_single) ->
    connector_fields(redis, single) ++
    [ {cmd, query()} ];
fields(redis_sentinel) ->
    connector_fields(redis, sentinel) ++
    [ {cmd, query()} ];
fields(redis_cluster) ->
    connector_fields(redis, cluster) ++
    [ {cmd, query()} ].

http_common_fields() ->
    [ {type,            #{type => http}}
    , {enable,          #{type => boolean(), default => true}}
    , {url,             #{type => url()}}
    , {request_timeout, mk_duration("request timeout", #{default => "30s"})}
    , {body,            #{type => map(), nullable => true}}
    ] ++ proplists:delete(base_url, emqx_connector_http:fields(config)).

mongo_common_fields() ->
    [ {collection, #{type => atom()}}
    , {selector, #{type => map()}}
    , {type, #{type => mongodb}}
    , {enable, #{type => boolean(),
                 default => true}}
    ].

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1}
    , {check_headers, fun check_headers/1}
    ].

headers(type) -> map();
headers(converter) ->
    fun(Headers) ->
       maps:merge(default_headers(), transform_header_name(Headers))
    end;
headers(default) -> default_headers();
headers(_) -> undefined.

headers_no_content_type(type) -> map();
headers_no_content_type(converter) ->
    fun(Headers) ->
       maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
    end;
headers_no_content_type(default) -> default_headers_no_content_type();
headers_no_content_type(_) -> undefined.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

default_headers() ->
    maps:put(<<"content-type">>,
             <<"application/json">>,
             default_headers_no_content_type()).

default_headers_no_content_type() ->
    #{ <<"accept">> => <<"application/json">>
     , <<"cache-control">> => <<"no-cache">>
     , <<"connection">> => <<"keep-alive">>
     , <<"keep-alive">> => <<"timeout=5">>
     }.

transform_header_name(Headers) ->
    maps:fold(fun(K0, V, Acc) ->
                      K = list_to_binary(string:to_lower(to_list(K0))),
                      maps:put(K, V, Acc)
              end, #{}, Headers).

check_ssl_opts(Conf)
  when Conf =:= #{} ->
    true;
check_ssl_opts(Conf) ->
    case emqx_authz_http:parse_url(hocon_schema:get_value("config.url", Conf)) of
        #{scheme := https} ->
            case hocon_schema:get_value("config.ssl.enable", Conf) of
                true -> ok;
                false -> false
            end;
        #{scheme := http} ->
            ok
    end.

check_headers(Conf)
    when Conf =:= #{} ->
    true;
check_headers(Conf) ->
    Method = to_bin(hocon_schema:get_value("config.method", Conf)),
    Headers = hocon_schema:get_value("config.headers", Conf),
    Method =:= <<"post">> orelse (not maps:is_key(<<"content-type">>, Headers)).

union_array(Item) when is_list(Item) ->
    hoconsc:array(hoconsc:union(Item)).

query() ->
    #{type => binary(),
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
    Mod0 = io_lib:format("~ts_~ts",[emqx_connector, DB]),
    Mod = try
              list_to_existing_atom(Mod0)
          catch
              error:badarg ->
                  list_to_atom(Mod0);
              error:Reason ->
                  erlang:error(Reason)
          end,
    [ {type, #{type => DB}}
    , {enable, #{type => boolean(),
                 default => true}}
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
