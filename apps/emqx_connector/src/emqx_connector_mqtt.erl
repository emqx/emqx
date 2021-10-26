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
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(supervisor).

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

-export([ roots/0
        , fields/1]).

%%=====================================================================
%% Hocon schema
roots() ->
    fields("config").

fields("config") ->
    emqx_connector_mqtt_schema:fields("config").

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
on_message_received(Msg, BridgeId) ->
    Name = atom_to_binary(BridgeId, utf8),
    emqx:run_hook(<<"$bridges/", Name/binary>>, [Msg]).

%% ===================================================================
on_start(InstId, Conf) ->
    "bridge:" ++ NamePrefix = binary_to_list(InstId),
    BridgeId = list_to_atom(NamePrefix),
    ?SLOG(info, #{msg => "starting mqtt connector",
                  connector => BridgeId, config => Conf}),
    BasicConf = basic_config(Conf),
    SubRemoteConf = maps:get(ingress, Conf, #{}),
    FrowardConf = maps:get(egress, Conf, #{}),
    BridgeConf = BasicConf#{
        name => BridgeId,
        clientid => clientid(BridgeId),
        subscriptions => SubRemoteConf#{
            to_local_topic => maps:get(to_local_topic, SubRemoteConf, undefined),
            on_message_received => {fun ?MODULE:on_message_received/2, [BridgeId]}
        },
        forwards => FrowardConf#{
            from_local_topic => maps:get(from_local_topic, FrowardConf, undefined)
        }
    },
    case ?MODULE:create_bridge(BridgeConf) of
        {ok, _Pid} ->
            case emqx_connector_mqtt_worker:ensure_started(BridgeId) of
                ok -> {ok, #{name => BridgeId}};
                {error, Reason} -> {error, Reason}
            end;
        {error, {already_started, _Pid}} ->
            {ok, #{name => BridgeId}};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(_InstId, #{name := BridgeId}) ->
    ?SLOG(info, #{msg => "stopping mqtt connector",
                  connector => BridgeId}),
    case ?MODULE:drop_bridge(BridgeId) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "stop mqtt connector",
                connector => BridgeId, reason => Reason})
    end.

on_query(_InstId, {send_message, BridgeId, Msg}, _AfterQuery, _State) ->
    ?SLOG(debug, #{msg => "send msg to remote node", message => Msg,
        connector => BridgeId}),
    emqx_connector_mqtt_worker:send_to_remote(BridgeId, Msg).

on_health_check(_InstId, #{name := BridgeId} = State) ->
    case emqx_connector_mqtt_worker:ping(BridgeId) of
        pong -> {ok, State};
        _ -> {error, {connector_down, BridgeId}, State}
    end.

basic_config(#{
        server := Server,
        reconnect_interval := ReconnIntv,
        proto_ver := ProtoVer,
        bridge_mode := BridgeMod,
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
        bridge_mode => BridgeMod,
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
    list_to_binary(lists:concat([str(Id), ":", node()])).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
