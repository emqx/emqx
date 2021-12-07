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
-module(emqx_connector_mqtt).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(supervisor).
-behaviour(emqx_resource).

%% API and callbacks for supervisor
-export([ start_link/0
        , init/1
        , create_bridge/1
        , drop_bridge/1
        , bridges/0
        ]).

-export([on_message_received/2]).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-behaviour(hocon_schema).

-import(hoconsc, [mk/2]).

-export([ roots/0
        , fields/1]).

%%=====================================================================
%% Hocon schema
roots() ->
    fields("config").

fields("config") ->
    emqx_connector_mqtt_schema:fields("config");

fields("get") ->
    [{id, mk(binary(),
        #{ desc => "The connector Id"
         , example => <<"mqtt:my_mqtt_connector">>
         })}]
    ++ fields("post");

fields("put") ->
    emqx_connector_mqtt_schema:fields("connector");

fields("post") ->
    [ {type, mk(mqtt, #{desc => "The Connector Type"})}
    , {name, mk(binary(),
        #{ desc => "The Connector Name"
         , example => <<"my_mqtt_connector">>
         })}
    ] ++ fields("put").

%% ===================================================================
%% supervisor APIs
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlag = #{strategy => one_for_one,
                intensity => 100,
                period => 10},
    {ok, {SupFlag, []}}.

bridge_spec(Config) ->
    #{id => maps:get(name, Config),
      start => {emqx_connector_mqtt_worker, start_link, [Config]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [emqx_connector_mqtt_worker]}.

-spec(bridges() -> [{node(), map()}]).
bridges() ->
    [{Name, emqx_connector_mqtt_worker:status(Name)}
     || {Name, _Pid, _, _} <- supervisor:which_children(?MODULE)].

create_bridge(Config) ->
    supervisor:start_child(?MODULE, bridge_spec(Config)).

drop_bridge(Name) ->
    case supervisor:terminate_child(?MODULE, Name) of
        ok ->
            supervisor:delete_child(?MODULE, Name);
        {error, Error} ->
            {error, Error}
    end.

%% ===================================================================
%% When use this bridge as a data source, ?MODULE:on_message_received/2 will be called
%% if the bridge received msgs from the remote broker.
on_message_received(Msg, HookPoint) ->
    emqx:run_hook(HookPoint, [Msg]).

%% ===================================================================
on_start(InstId, Conf) ->
    InstanceId = binary_to_atom(InstId, utf8),
    ?SLOG(info, #{msg => "starting mqtt connector",
                  connector => InstanceId, config => Conf}),
    BasicConf = basic_config(Conf),
    BridgeConf = BasicConf#{
        name => InstanceId,
        clientid => clientid(maps:get(clientid, Conf, InstId)),
        subscriptions => make_sub_confs(maps:get(ingress, Conf, undefined)),
        forwards => make_forward_confs(maps:get(egress, Conf, undefined))
    },
    case ?MODULE:create_bridge(BridgeConf) of
        {ok, _Pid} ->
            case emqx_connector_mqtt_worker:ensure_started(InstanceId) of
                ok -> {ok, #{name => InstanceId}};
                {error, Reason} -> {error, Reason}
            end;
        {error, {already_started, _Pid}} ->
            {ok, #{name => InstanceId}};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(_InstId, #{name := InstanceId}) ->
    ?SLOG(info, #{msg => "stopping mqtt connector",
                  connector => InstanceId}),
    case ?MODULE:drop_bridge(InstanceId) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "stop mqtt connector",
                connector => InstanceId, reason => Reason})
    end.

on_query(_InstId, {send_message, Msg}, AfterQuery, #{name := InstanceId}) ->
    ?SLOG(debug, #{msg => "send msg to remote node", message => Msg,
        connector => InstanceId}),
    emqx_connector_mqtt_worker:send_to_remote(InstanceId, Msg),
    emqx_resource:query_success(AfterQuery).

on_health_check(_InstId, #{name := InstanceId} = State) ->
    case emqx_connector_mqtt_worker:ping(InstanceId) of
        pong -> {ok, State};
        _ -> {error, {connector_down, InstanceId}, State}
    end.

make_sub_confs(EmptyMap) when map_size(EmptyMap) == 0 ->
    undefined;
make_sub_confs(undefined) ->
    undefined;
make_sub_confs(SubRemoteConf) ->
    case maps:take(hookpoint, SubRemoteConf) of
        error -> SubRemoteConf;
        {HookPoint, SubConf} ->
            MFA = {?MODULE, on_message_received, [HookPoint]},
            SubConf#{on_message_received => MFA}
    end.

make_forward_confs(EmptyMap) when map_size(EmptyMap) == 0 ->
    undefined;
make_forward_confs(undefined) ->
    undefined;
make_forward_confs(FrowardConf) ->
    FrowardConf.

basic_config(#{
        server := Server,
        reconnect_interval := ReconnIntv,
        proto_ver := ProtoVer,
        username := User,
        password := Password,
        clean_start := CleanStart,
        keepalive := KeepAlive,
        retry_interval := RetryIntv,
        max_inflight := MaxInflight,
        replayq := ReplayQ,
        ssl := #{enable := EnableSsl} = Ssl}) ->
    #{
        replayq => ReplayQ,
        %% connection opts
        server => Server,
        reconnect_interval => ReconnIntv,
        proto_ver => ProtoVer,
        bridge_mode => true,
        username => User,
        password => Password,
        clean_start => CleanStart,
        keepalive => KeepAlive,
        retry_interval => RetryIntv,
        max_inflight => MaxInflight,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl)),
        if_record_metrics => true
    }.

clientid(Id) ->
    iolist_to_binary([Id, ":", atom_to_list(node())]).
