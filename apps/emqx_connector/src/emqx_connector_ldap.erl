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
-module(emqx_connector_ldap).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-export([roots/0, fields/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([connect/1]).

-export([search/4]).
%%=====================================================================
roots() ->
    ldap_fields() ++ emqx_connector_schema_lib:ssl_fields().

%% this schema has no sub-structs
fields(_) -> [].

%% ===================================================================
callback_mode() -> always_sync.

on_start(
    InstId,
    #{
        servers := Servers0,
        port := Port,
        bind_dn := BindDn,
        bind_password := BindPassword,
        timeout := Timeout,
        pool_size := PoolSize,
        auto_reconnect := AutoReconn,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_ldap_connector",
        connector => InstId,
        config => Config
    }),
    Servers = [
        begin
            proplists:get_value(host, S)
        end
     || S <- Servers0
    ],
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [
                    {ssl, true},
                    {sslopts, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                [{ssl, false}]
        end,
    Opts = [
        {servers, Servers},
        {port, Port},
        {bind_dn, BindDn},
        {bind_password, BindPassword},
        {timeout, Timeout},
        {pool_size, PoolSize},
        {auto_reconnect, reconn_interval(AutoReconn)},
        {servers, Servers}
    ],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ SslOpts) of
        ok -> {ok, #{poolname => PoolName, auto_reconnect => AutoReconn}};
        {error, Reason} -> {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_ldap_connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {search, Base, Filter, Attributes}, #{poolname := PoolName} = State) ->
    Request = {Base, Filter, Attributes},
    ?TRACE(
        "QUERY",
        "ldap_connector_received",
        #{request => Request, connector => InstId, state => State}
    ),
    case
        Result = ecpool:pick_and_do(
            PoolName,
            {?MODULE, search, [Base, Filter, Attributes]},
            no_handover
        )
    of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "ldap_connector_do_request_failed",
                request => Request,
                connector => InstId,
                reason => Reason
            });
        _ ->
            ok
    end,
    Result.

on_get_status(_InstId, _State) -> connected.

reconn_interval(true) -> 15;
reconn_interval(false) -> false.

search(Conn, Base, Filter, Attributes) ->
    eldap2:search(Conn, [
        {base, Base},
        {filter, Filter},
        {attributes, Attributes},
        {deref, eldap2:'derefFindingBaseObj'()}
    ]).

%% ===================================================================
connect(Opts) ->
    Servers = proplists:get_value(servers, Opts, ["localhost"]),
    Port = proplists:get_value(port, Opts, 389),
    Timeout = proplists:get_value(timeout, Opts, 30),
    BindDn = proplists:get_value(bind_dn, Opts),
    BindPassword = proplists:get_value(bind_password, Opts),
    SslOpts =
        case proplists:get_value(ssl, Opts, false) of
            true ->
                [{sslopts, proplists:get_value(sslopts, Opts, [])}, {ssl, true}];
            false ->
                [{ssl, false}]
        end,
    LdapOpts =
        [
            {port, Port},
            {timeout, Timeout}
        ] ++ SslOpts,
    {ok, LDAP} = eldap2:open(Servers, LdapOpts),
    ok = eldap2:simple_bind(LDAP, BindDn, BindPassword),
    {ok, LDAP}.

ldap_fields() ->
    [
        {servers, fun servers/1},
        {port, fun port/1},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {bind_dn, fun bind_dn/1},
        {bind_password, fun emqx_connector_schema_lib:password/1},
        {timeout, fun duration/1},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

servers(type) -> list();
servers(validator) -> [?NOT_EMPTY("the value of the field 'servers' cannot be empty")];
servers(converter) -> fun to_servers_raw/1;
servers(required) -> true;
servers(_) -> undefined.

bind_dn(type) -> binary();
bind_dn(default) -> 0;
bind_dn(_) -> undefined.

port(type) -> integer();
port(default) -> 389;
port(_) -> undefined.

duration(type) -> emqx_schema:duration_ms();
duration(_) -> undefined.

to_servers_raw(Servers) ->
    {ok,
        lists:map(
            fun(Server) ->
                case string:tokens(Server, ": ") of
                    [Ip] ->
                        [{host, Ip}];
                    [Ip, Port] ->
                        [{host, Ip}, {port, list_to_integer(Port)}]
                end
            end,
            string:tokens(str(Servers), ", ")
        )}.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
