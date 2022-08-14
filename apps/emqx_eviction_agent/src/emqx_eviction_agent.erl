%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("stdlib/include/qlc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0,
         enable/2,
         disable/1,
         status/0,
         connection_count/0,
         session_count/0,
         session_count/1,
         evict_connections/1,
         evict_sessions/2,
         evict_sessions/3,
         evict_session_channel/3
        ]).

-behaviour(gen_server).

-export([init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         code_change/3
        ]).

-export([on_connect/2,
         on_connack/3]).

-export([hook/0,
         unhook/0]).

-export_type([server_reference/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type server_reference() :: binary() | undefined.
-type status() :: {enabled, conn_stats()} | disabled.
-type conn_stats() :: #{connections := non_neg_integer(),
                        sessions := non_neg_integer()}.
-type kind() :: atom().

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec enable(kind(), server_reference()) -> ok_or_error(eviction_agent_busy).
enable(Kind, ServerReference) ->
    gen_server:call(?MODULE, {enable, Kind, ServerReference}).

-spec disable(kind()) -> ok.
disable(Kind) ->
    gen_server:call(?MODULE, {disable, Kind}).

-spec status() -> status().
status() ->
    case enable_status() of
        {enabled, _Kind, _ServerReference} ->
            {enabled, stats()};
        disabled ->
           disabled
    end.

-spec evict_connections(pos_integer()) -> ok_or_error(disabled).
evict_connections(N) ->
    case enable_status() of
        {enabled, _Kind, ServerReference} ->
            ok = do_evict_connections(N, ServerReference);
        disabled ->
            {error, disabled}
    end.

-spec evict_sessions(pos_integer(), node() | [node()]) -> ok_or_error(disabled).
evict_sessions(N, Node) when is_atom(Node) ->
    evict_sessions(N, [Node]);
evict_sessions(N, Nodes) when is_list(Nodes) andalso length(Nodes) > 0 ->
    evict_sessions(N, Nodes, any).

-spec evict_sessions(pos_integer(), node() | [node()], atom()) -> ok_or_error(disabled).
evict_sessions(N, Node, ConnState) when is_atom(Node) ->
    evict_sessions(N, [Node], ConnState);
evict_sessions(N, Nodes, ConnState)
  when is_list(Nodes) andalso length(Nodes) > 0 ->
    case enable_status() of
        {enabled, _Kind, _ServerReference} ->
            ok = do_evict_sessions(N, Nodes, ConnState);
        disabled ->
            {error, disabled}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = persistent_term:erase(?MODULE),
    {ok, #{}}.

%% enable
handle_call({enable, Kind, ServerReference}, _From, St) ->
    Reply = case enable_status() of
                disabled ->
                    ok = persistent_term:put(?MODULE, {enabled, Kind, ServerReference});
                {enabled, Kind, _ServerReference} ->
                    ok = persistent_term:put(?MODULE, {enabled, Kind, ServerReference});
                {enabled, _OtherKind, _ServerReference} ->
                    {error, eviction_agent_busy}
            end,
    {reply, Reply, St};

%% disable
handle_call({disable, Kind}, _From, St) ->
    Reply = case enable_status() of
                disabled ->
                    {error, disabled};
                {enabled, Kind, _ServerReference} ->
                    _ = persistent_term:erase(?MODULE),
                    ok;
                {enabled, _OtherKind, _ServerReference} ->
                    {error, eviction_agent_busy}
            end,
    {reply, Reply, St}.

handle_info(Msg, St) ->
    ?LOG(warning, "Unknown Msg: ~p, State: ~p", [Msg, St]),
    {noreply, St}.

handle_cast(Msg, St) ->
    ?LOG(warning, "Unknown cast Msg: ~p, State: ~p", [Msg, St]),
    {noreply, St}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_connect(_ConnInfo, _Props) ->
    case enable_status() of
        {enabled, _Kind, _ServerReference} ->
            {stop, {error, ?RC_USE_ANOTHER_SERVER}};
        disabled ->
            ignore
    end.

on_connack(#{proto_name := <<"MQTT">>, proto_ver := ?MQTT_PROTO_V5},
           use_another_server,
           Props) ->
    case enable_status() of
        {enabled, _Kind, ServerReference} ->
            {ok, Props#{'Server-Reference' => ServerReference}};
        disabled ->
            {ok, Props}
    end;
on_connack(_ClientInfo, _Reason, Props) ->
    {ok, Props}.

%%--------------------------------------------------------------------
%% Hook funcs
%%--------------------------------------------------------------------

hook() ->
    ?tp(debug, eviction_agent_hook, #{}),
    ok = emqx_hooks:put('client.connack', {?MODULE, on_connack, []}),
    ok = emqx_hooks:put('client.connect', {?MODULE, on_connect, []}).

unhook() ->
    ?tp(debug, eviction_agent_unhook, #{}),
    ok = emqx_hooks:del('client.connect', {?MODULE, on_connect}),
    ok = emqx_hooks:del('client.connack', {?MODULE, on_connack}).

enable_status() ->
    persistent_term:get(?MODULE, disabled).

% connection management
stats() ->
    #{
        connections => connection_count(),
        sessions => session_count()
     }.

connection_table() ->
    emqx_cm:live_connection_table().

connection_count() ->
    table_count(connection_table()).

channel_with_session_table(any) ->
    qlc:q([{ClientId, ConnInfo, ClientInfo}
           || {ClientId, _, ConnInfo, ClientInfo} <- emqx_cm:channel_with_session_table()]);
channel_with_session_table(RequiredConnState) ->
    qlc:q([{ClientId, ConnInfo, ClientInfo}
           || {ClientId, ConnState, ConnInfo, ClientInfo} <- emqx_cm:channel_with_session_table(),
              RequiredConnState =:= ConnState]).

session_count() ->
    session_count(any).

session_count(ConnState) ->
    table_count(channel_with_session_table(ConnState)).

table_count(QH) ->
    qlc:fold(fun(_, Acc) -> Acc + 1 end, 0, QH).

take_connections(N) ->
    ChanQH = qlc:q([ChanPid || {_ClientId, ChanPid} <- connection_table()]),
    ChanPidCursor = qlc:cursor(ChanQH),
    ChanPids = qlc:next_answers(ChanPidCursor, N),
    ok = qlc:delete_cursor(ChanPidCursor),
    ChanPids.

take_channel_with_sessions(N, ConnState) ->
    ChanPidCursor = qlc:cursor(channel_with_session_table(ConnState)),
    Channels = qlc:next_answers(ChanPidCursor, N),
    ok = qlc:delete_cursor(ChanPidCursor),
    Channels.

do_evict_connections(N, ServerReference) when N > 0 ->
    ChanPids = take_connections(N),
    ok = lists:foreach(
           fun(ChanPid) ->
                   disconnect_channel(ChanPid, ServerReference)
           end,
           ChanPids).

do_evict_sessions(N, Nodes, ConnState) when N > 0 ->
    Channels = take_channel_with_sessions(N, ConnState),
    ok = lists:foreach(
           fun({ClientId, ConnInfo, ClientInfo}) ->
                   evict_session_channel(Nodes, ClientId, ConnInfo, ClientInfo)
           end,
           Channels).

evict_session_channel(Nodes, ClientId, ConnInfo, ClientInfo) ->
    Node = select_random(Nodes),
    ?LOG(info, "Evicting client=~p to node=~p, conninfo=~p, clientinfo=~p",
         [ClientId, Node, ConnInfo, ClientInfo]),
    case rpc:call(Node, ?MODULE, evict_session_channel, [ClientId, ConnInfo, ClientInfo]) of
        {badrpc, Reason} ->
            ?LOG(error, "RPC error while evicting client=~p to node=~p: ~p",
                 [ClientId, Node, Reason]),
            {error, Reason};
        {error, Reason} = Error ->
            ?LOG(error, "Error evicting client=~p to node=~p: ~p",
                 [ClientId, Node, Reason]),
            Error;
        Res -> Res
    end.

evict_session_channel(ClientId, ConnInfo, ClientInfo) ->
    ?LOG(info, "Taking up client=~p, conninfo=~p, clientinfo=~p",
         [ClientId, ConnInfo, ClientInfo]),
    Result = emqx_eviction_agent_channel:start_supervised(
               #{conninfo => ConnInfo,
                 clientinfo => ClientInfo}),
    ?LOG(info, "Taking up client=~p, result=~p",
         [ClientId, Result]),
    Result.

disconnect_channel(ChanPid, ServerReference) ->
    ChanPid ! {disconnect,
               ?RC_USE_ANOTHER_SERVER,
               use_another_server,
               #{'Server-Reference' => ServerReference}}.

select_random(List) when length(List) > 0 ->
    lists:nth(rand:uniform(length(List)) , List).
