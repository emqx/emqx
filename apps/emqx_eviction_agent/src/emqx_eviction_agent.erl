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
         evict_connections/1
        ]).

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
                        channels := non_neg_integer()}.
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

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
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
        channels => channel_count()
     }.

connection_table() ->
    emqx_cm:all_connection_table().

connection_count() ->
    emqx_cm:all_connection_count().

channel_count() ->
    emqx_cm:all_channel_count().

take_connections(N) ->
    ChanQH = qlc:q([ChanPid || {_ClientId, ChanPid} <- connection_table()]),
    ChanPidCursor = qlc:cursor(ChanQH),
    ChanPids = qlc:next_answers(ChanPidCursor, N),
    ok = qlc:delete_cursor(ChanPidCursor),
    ChanPids.

do_evict_connections(N, ServerReference) when N > 0 ->
    ChanPids = take_connections(N),
    ok = lists:foreach(
           fun(ChanPid) ->
                   disconnect_channel(ChanPid, ServerReference)
           end,
           ChanPids).

disconnect_channel(ChanPid, ServerReference) ->
    ChanPid ! {disconnect,
               ?RC_USE_ANOTHER_SERVER,
               use_another_server,
               #{'Server-Reference' => ServerReference}}.
