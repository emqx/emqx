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
-module(emqx_connector_ldap).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

-export([ schema/0
        ]).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_jsonify/1
        ]).

-export([do_health_check/1]).

-export([connect/1]).

-export([search/4]).
%%=====================================================================
schema() ->
    redis_fields() ++
    emqx_connector_schema_lib:ssl_fields().

on_jsonify(Config) ->
    Config.

%% ===================================================================
on_start(InstId, #{servers := Servers,
                   port := Port,
                   bind_dn := BindDn,
                   bind_password :=  BindPassword,
                   timeout := Timeout,
                   pool_size := PoolSize,
                   auto_reconnect := AutoReconn} = Config) ->
    logger:info("starting redis connector: ~p, config: ~p", [InstId, Config]),
    SslOpts = init_ssl_opts(Config, InstId),
    Opts = [{servers, Servers},
            {port, Port},
            {bind_dn, BindDn},
            {bind_password, BindPassword},
            {timeout, Timeout},
            {pool_size, PoolSize},
            {auto_reconnect, reconn_interval(AutoReconn)},
            {servers, Servers}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    _ = emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ SslOpts),
    {ok, #{poolname => PoolName}}.

on_stop(InstId, #{poolname := PoolName}) ->
    logger:info("stopping redis connector: ~p", [InstId]),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {search, Base, Filter, Attributes}, AfterQuery, #{poolname := PoolName} = State) ->
    logger:debug("redis connector ~p received request: ~p, at state: ~p", [InstId, {Base, Filter, Attributes}, State]),
    case Result = ecpool:pick_and_do(PoolName, {?MODULE, search, [Base, Filter, Attributes]}, no_handover) of
        {error, Reason} ->
            logger:debug("redis connector ~p do request failed, request: ~p, reason: ~p", [InstId, {Base, Filter, Attributes}, Reason]),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{poolname := PoolName} = State) ->
    emqx_plugin_libs_pool:health_check(PoolName, fun ?MODULE:do_health_check/1, State).

do_health_check(_Conn) ->
    {ok, true}.

reconn_interval(true) -> 15;
reconn_interval(false) -> false.

search(Conn, Base, Filter, Attributes) ->
    eldap2:search(Conn, [{base, Base},
                         {filter, Filter},
                         {attributes, Attributes},
                         {deref, eldap2:derefFindingBaseObj()}]).

%% ===================================================================
connect(Opts) ->
    Servers      = proplists:get_value(servers, Opts, ["localhost"]),
    Port         = proplists:get_value(port, Opts, 389),
    Timeout      = proplists:get_value(timeout, Opts, 30),
    BindDn       = proplists:get_value(bind_dn, Opts),
    BindPassword = proplists:get_value(bind_password, Opts),
    SslOpts = proplists:get_value(sslopts, Opts),
    LdapOpts = [{port, Port}, {timeout, Timeout}] ++ SslOpts,
    {ok, LDAP} = eldap2:open(Servers, LdapOpts),
    ok = eldap2:simple_bind(LDAP, BindDn, BindPassword),
    {ok, LDAP}.

init_ssl_opts(#{ssl := true} = Config, InstId) ->
    [{ssl, true},
     {sslopts, emqx_plugin_libs_ssl:save_files_return_opts(Config, "connectors", InstId)}
    ];
init_ssl_opts(_Config, _InstId) ->
    [{ssl, false}].

redis_fields() ->
    [ {servers, fun emqx_connector_schema_lib:servers/1}
    , {port, fun port/1}
    , {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    , {bind_dn, fun bind_dn/1}
    , {bind_password, fun emqx_connector_schema_lib:password/1}
    , {timeout, fun duration/1}
    , {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

bind_dn(type) -> binary();
bind_dn(default) -> 0;
bind_dn(_) -> undefined.

port(type) -> integer();
port(default) -> 389;
port(_) -> undefined.

duration(type) -> emqx_schema:duration_ms();
duration(_) -> undefined.
