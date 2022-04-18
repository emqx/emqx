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
-module(emqx_connector_pgsql).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-export([roots/0, fields/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-export([connect/1]).

-export([ query/3
        , prepared_query/3
        ]).

-export([do_health_check/1]).

-define( PGSQL_HOST_OPTIONS
       , #{ host_type => inet_addr
          , default_port => ?PGSQL_DEFAULT_PORT}).


%%=====================================================================

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [ {named_queries, fun named_queries/1}
    , {server, fun server/1}] ++
    emqx_connector_schema_lib:relational_db_fields() ++
    emqx_connector_schema_lib:ssl_fields().

named_queries(type) -> map();
named_queries(desc) -> ?DESC("name_queries_desc");
named_queries(required) -> false;
named_queries(_) -> undefined.

server(type) -> emqx_schema:ip_port();
server(required) -> true;
server(validator) -> [?NOT_EMPTY("the value of the field 'server' cannot be empty")];
server(converter) -> fun to_server/1;
server(desc) -> ?DESC("server");
server(_) -> undefined.

%% ===================================================================
on_start(InstId, #{server := {Host, Port},
                   database := DB,
                   username := User,
                   password := Password,
                   auto_reconnect := AutoReconn,
                   pool_size := PoolSize,
                   ssl := SSL} = Config) ->
    ?SLOG(info, #{msg => "starting_postgresql_connector",
                  connector => InstId, config => Config}),
    SslOpts = case maps:get(enable, SSL) of
                  true ->
                      [{ssl, true},
                       {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}];
                  false ->
                      [{ssl, false}]
              end,
    Options = [{host, Host},
               {port, Port},
               {username, User},
               {password, Password},
               {database, DB},
               {auto_reconnect, reconn_interval(AutoReconn)},
               {pool_size, PoolSize},
               {named_queries, maps:to_list(maps:get(named_queries, Config, #{}))}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options ++ SslOpts) of
        ok              -> {ok, #{poolname => PoolName}};
        {error, Reason} -> {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{msg => "stopping postgresql connector",
                  connector => InstId}),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {Type, NameOrSQL}, AfterQuery, #{poolname := _PoolName} = State) ->
    on_query(InstId, {Type, NameOrSQL, []}, AfterQuery, State);

on_query(InstId, {Type, NameOrSQL, Params}, AfterQuery, #{poolname := PoolName} = State) ->
    ?SLOG(debug, #{msg => "postgresql connector received sql query",
        connector => InstId, sql => NameOrSQL, state => State}),
    case Result = ecpool:pick_and_do(PoolName, {?MODULE, Type, [NameOrSQL, Params]}, no_handover) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "postgresql connector do sql query failed",
                connector => InstId, sql => NameOrSQL, reason => Reason}),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{poolname := PoolName} = State) ->
    emqx_plugin_libs_pool:health_check(PoolName, fun ?MODULE:do_health_check/1, State).

do_health_check(Conn) ->
    ok == element(1, epgsql:squery(Conn, "SELECT count(1) AS T")).

%% ===================================================================
reconn_interval(true) -> 15;
reconn_interval(false) -> false.

connect(Opts) ->
    Host     = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
    NamedQueries = proplists:get_value(named_queries, Opts),
    case epgsql:connect(Host, Username, Password, conn_opts(Opts)) of
        {ok, Conn} ->
            case parse(Conn, NamedQueries) of
                ok -> {ok, Conn};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

query(Conn, SQL, Params) ->
    epgsql:equery(Conn, SQL, Params).

prepared_query(Conn, Name, Params) ->
    epgsql:prepared_query2(Conn, Name, Params).

parse(_Conn, []) ->
    ok;
parse(Conn, [{Name, Query} | More]) ->
    case epgsql:parse2(Conn, Name, Query, []) of
        {ok, _Statement} ->
            parse(Conn, More);
        Other ->
            Other
    end.

conn_opts(Opts) ->
    conn_opts(Opts, []).
conn_opts([], Acc) ->
    Acc;
conn_opts([Opt = {database, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {ssl, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {port, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {timeout, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {ssl_opts, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([_Opt | Opts], Acc) ->
    conn_opts(Opts, Acc).

%% ===================================================================
%% typereflt funcs

-spec to_server(string())
      -> {inet:ip_address() | inet:hostname(), pos_integer()}.
to_server(Str) ->
    emqx_connector_schema_lib:parse_server(Str, ?PGSQL_HOST_OPTIONS).
