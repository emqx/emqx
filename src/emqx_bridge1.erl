%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge1).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

 -import(proplists, [get_value/2, get_value/3]).

-export([start_link/2, start_bridge/1, stop_bridge/1, status/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {client_pid, options, reconnect_time, reconnect_count,
                def_reconnect_count, type, mountpoint, queue, store_type,
                max_pending_messages}).

-record(mqtt_msg, {qos = ?QOS0, retain = false, dup = false,
                   packet_id, topic, props, payload}).

start_link(Name, Options) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, [Options], []).

start_bridge(Name) ->
    gen_server:call(name(Name), start_bridge).

stop_bridge(Name) ->
    gen_server:call(name(Name), stop_bridge).

status(Pid) ->
    gen_server:call(Pid, status).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Options]) ->
    process_flag(trap_exit, true),
    case get_value(start_type, Options, manual) of
        manual -> ok;
        auto -> erlang:send_after(1000, self(), start)
    end,
    ReconnectCount = get_value(reconnect_count, Options, 10),
    ReconnectTime = get_value(reconnect_time, Options, 30000),
    MaxPendingMsg = get_value(max_pending_messages, Options, 10000),
    Mountpoint = format_mountpoint(get_value(mountpoint, Options)),
    StoreType = get_value(store_type, Options, memory),
    Type = get_value(type, Options, in),
    Queue = [],
    {ok, #state{type                = Type,
                mountpoint          = Mountpoint,
                queue               = Queue,
                store_type          = StoreType,
                options             = Options,
                reconnect_count     = ReconnectCount,
                reconnect_time      = ReconnectTime,
                def_reconnect_count = ReconnectCount,
                max_pending_messages = MaxPendingMsg}}.

handle_call(start_bridge, _From, State = #state{client_pid = undefined}) ->
    {noreply, NewState} = handle_info(start, State),
    {reply, <<"start bridge successfully">>, NewState};

handle_call(start_bridge, _From, State) ->
    {reply, <<"bridge already started">>, State};

handle_call(stop_bridge, _From, State = #state{client_pid = undefined}) ->
    {reply, <<"bridge not started">>, State};

handle_call(stop_bridge, _From, State = #state{client_pid = Pid}) ->
    emqx_client:disconnect(Pid),
    {reply, <<"stop bridge successfully">>, State};

handle_call(status, _From, State = #state{client_pid = undefined}) ->
    {reply, <<"Stopped">>, State};
handle_call(status, _From, State = #state{client_pid = _Pid})->
    {reply, <<"Running">>, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[Bridge] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Bridge] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(start, State = #state{reconnect_count = 0}) ->
    {noreply, State};

%%----------------------------------------------------------------
%% start in message bridge
%%----------------------------------------------------------------
handle_info(start, State = #state{options = Options,
                                  client_pid = undefined,
                                  reconnect_time = ReconnectTime,
                                  reconnect_count = ReconnectCount,
                                  type = in}) ->
    case emqx_client:start_link([{owner, self()}|options(Options)]) of
        {ok, ClientPid, _} ->
            Subs = get_value(subscriptions, Options, []),
            [emqx_client:subscribe(ClientPid, {i2b(Topic), Qos}) || {Topic, Qos} <- Subs],
            {noreply, State#state{client_pid = ClientPid}};
        {error,_} ->
            erlang:send_after(ReconnectTime, self(), start),
            {noreply, State = #state{reconnect_count = ReconnectCount-1}}
    end;

%%----------------------------------------------------------------
%% start out message bridge
%%----------------------------------------------------------------
handle_info(start, State = #state{options = Options,
                                  client_pid = undefined,
                                  reconnect_time = ReconnectTime,
                                  reconnect_count = ReconnectCount,
                                  type = out}) ->
    case emqx_client:start_link([{owner, self()}|options(Options)]) of
        {ok, ClientPid, _} ->
            Subs = get_value(subscriptions, Options, []),
            [emqx_client:subscribe(ClientPid, {i2b(Topic), Qos}) || {Topic, Qos} <- Subs],
            ForwardRules = string:tokens(get_value(forward_rule, Options, ""), ","),
            [emqx_broker:subscribe(i2b(Topic)) || Topic <- ForwardRules, emqx_topic:validate({filter, i2b(Topic)})],
            {noreply, State#state{client_pid = ClientPid}};
        {error,_} ->
            erlang:send_after(ReconnectTime, self(), start),
            {noreply, State = #state{reconnect_count = ReconnectCount-1}}
    end;

%%----------------------------------------------------------------
%% received local node message
%%----------------------------------------------------------------
handle_info({dispatch, _, #message{topic = Topic, payload = Payload, flags = #{retain := Retain}}},
             State = #state{client_pid = Pid, mountpoint = Mountpoint, queue = Queue,
                            store_type = StoreType, max_pending_messages = MaxPendingMsg}) ->
    Msg = #mqtt_msg{qos     = 1,
                    retain  = Retain,
                    topic   = mountpoint(Mountpoint, Topic),
                    payload = Payload},
    case emqx_client:publish(Pid, Msg) of
        {ok, PkgId} ->
            {noreply, State#state{queue = store(StoreType, {PkgId, Msg}, Queue, MaxPendingMsg)}};
        {error, Reason} ->
            emqx_logger:error("Publish fail:~p", [Reason]),
            {noreply, State}
    end;

%%----------------------------------------------------------------
%% received remote node message
%%----------------------------------------------------------------
handle_info({publish, #{qos := QoS, dup := Dup, retain := Retain, topic := Topic,
                        properties := Props, payload := Payload}}, State) ->
    NewMsg0 = emqx_message:make(bridge, QoS, Topic, Payload),
    NewMsg1 = emqx_message:set_headers(Props, emqx_message:set_flags(#{dup => Dup, retain=> Retain}, NewMsg0)),
    emqx_broker:publish(NewMsg1),
    {noreply, State};

%%----------------------------------------------------------------
%% received remote puback message
%%----------------------------------------------------------------
handle_info({puback, #{packet_id := PkgId}}, State = #state{queue = Queue, store_type = StoreType}) ->
    % lists:keydelete(PkgId, 1, Queue)
    {noreply, State#state{queue = delete(StoreType, PkgId, Queue)}};

handle_info({'EXIT', Pid, normal}, State = #state{client_pid = Pid}) ->
    {noreply, State#state{client_pid = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = Pid,
                                                  reconnect_time = ReconnectTime,
                                                  def_reconnect_count = DefReconnectCount}) ->
    lager:warning("emqx bridge stop reason:~p", [Reason]),
    erlang:send_after(ReconnectTime, self(), start),
    {noreply, State#state{client_pid = undefined, reconnect_count = DefReconnectCount}};

handle_info(Info, State) ->
    emqx_logger:error("[Bridge] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

proto_ver(mqtt3) -> v3;
proto_ver(mqtt4) -> v4;
proto_ver(mqtt5) -> v5.
address(Address) ->
    case string:tokens(Address, ":") of
        [Host] -> {Host, 1883};
        [Host, Port] -> {Host, list_to_integer(Port)}
    end.
options(Options) ->
    options(Options, []).
options([], Acc) ->
    Acc;
options([{username, Username}| Options], Acc) ->
    options(Options, [{username, Username}|Acc]);
options([{proto_ver, ProtoVer}| Options], Acc) ->
    options(Options, [{proto_ver, proto_ver(ProtoVer)}|Acc]);
options([{password, Password}| Options], Acc) ->
    options(Options, [{password, Password}|Acc]);
options([{keepalive, Keepalive}| Options], Acc) ->
    options(Options, [{keepalive, Keepalive}|Acc]);
options([{client_id, ClientId}| Options], Acc) ->
    options(Options, [{client_id, ClientId}|Acc]);
options([{clean_start, CleanStart}| Options], Acc) ->
    options(Options, [{clean_start, CleanStart}|Acc]);
options([{address, Address}| Options], Acc) ->
    {Host, Port} = address(Address),
    options(Options, [{host, Host}, {port, Port}|Acc]);
options([_Option | Options], Acc) ->
    options(Options, Acc).

name(Id) ->
    list_to_atom(lists:concat([?MODULE, "_", Id])).

i2b(L) -> iolist_to_binary(L).

mountpoint(undefined, Topic) ->
    Topic;
mountpoint(Prefix, Topic) ->
    <<Prefix/binary, Topic/binary>>.

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(i2b(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

store(memory, Data, Queue, MaxPendingMsg) when length(Queue) =< MaxPendingMsg ->
    [Data | Queue];
store(memory, _Data, Queue, _MaxPendingMsg) ->
    lager:error("Beyond max pending messages"),
    Queue;
store(disk, Data, Queue, _MaxPendingMsg)->
    [Data | Queue].

delete(memory, PkgId, Queue) ->
    lists:keydelete(PkgId, 1, Queue);
delete(disk, PkgId, Queue) ->
    lists:keydelete(PkgId, 1, Queue).