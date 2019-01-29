%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([start_link/2, start_bridge/1, stop_bridge/1, status/1]).

-export([show_forwards/1, add_forward/2, del_forward/2]).

-export([show_subscriptions/1, add_subscription/3, del_subscription/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {client_pid         :: pid(),
                options            :: list(),
                reconnect_interval :: pos_integer(),
                mountpoint         :: binary(),
                readq              :: list(),
                writeq             :: list(),
                replayq            :: map(),
                ackref             :: replayq:ack_ref(),
                queue_option       :: map(),
                forwards           :: list(),
                subscriptions      :: list()}).

-record(mqtt_msg, {qos = ?QOS_0, retain = false, dup = false,
                   packet_id, topic, props, payload}).

start_link(Name, Options) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, [Options], []).

start_bridge(Name) ->
    gen_server:call(name(Name), start_bridge).

stop_bridge(Name) ->
    gen_server:call(name(Name), stop_bridge).

-spec(show_forwards(atom()) -> list()).
show_forwards(Name) ->
    gen_server:call(name(Name), show_forwards).

-spec(add_forward(atom(), binary()) -> ok | {error, already_exists | validate_fail}).
add_forward(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {add_forward, Topic});
        {'EXIT', _Reason} ->
            {error, validate_fail}
    end.

-spec(del_forward(atom(), binary()) -> ok | {error, validate_fail}).
del_forward(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {del_forward, Topic});
        _ ->
            {error, validate_fail}
    end.

-spec(show_subscriptions(atom()) -> list()).
show_subscriptions(Name) ->
    gen_server:call(name(Name), show_subscriptions).

-spec(add_subscription(atom(), binary(), integer()) -> ok | {error, already_exists | validate_fail}).
add_subscription(Name, Topic, QoS) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {add_subscription, Topic, QoS});
        {'EXIT', _Reason} ->
            {error, validate_fail}
    end.

-spec(del_subscription(atom(), binary()) -> ok | {error, validate_fail}).
del_subscription(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {del_subscription, Topic});
        _ ->
            {error, validate_fail}
    end.

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
    ReconnectInterval = get_value(reconnect_interval, Options, 30000),
    Mountpoint = format_mountpoint(get_value(mountpoint, Options)),
    QueueOptions = get_value(queue, Options),
    {ok, #state{mountpoint         = Mountpoint,
                queue_option       = QueueOptions,
                readq              = [],
                writeq             = [],
                options            = Options,
                reconnect_interval = ReconnectInterval}}.

handle_call(start_bridge, _From, State = #state{client_pid = undefined}) ->
    {Msg, NewState} = bridge(start, State),
    {reply, #{msg => Msg}, NewState};

handle_call(start_bridge, _From, State) ->
    {reply, #{msg => <<"bridge already started">>}, State};

handle_call(stop_bridge, _From, State = #state{client_pid = undefined}) ->
    {reply, #{msg => <<"bridge not started">>}, State};

handle_call(stop_bridge, _From, State = #state{client_pid = Pid}) ->
    emqx_client:disconnect(Pid),
    {reply, #{msg => <<"stop bridge successfully">>}, State};

handle_call(status, _From, State = #state{client_pid = undefined}) ->
    {reply, #{status => <<"Stopped">>}, State};
handle_call(status, _From, State = #state{client_pid = _Pid})->
    {reply, #{status => <<"Running">>}, State};

handle_call(show_forwards, _From, State = #state{forwards = Forwards}) ->
    {reply, Forwards, State};

handle_call({add_forward, Topic}, _From, State = #state{forwards = Forwards}) ->
    case not lists:member(Topic, Forwards) of
        true ->
            emqx_broker:subscribe(Topic),
            {reply, ok, State#state{forwards = [Topic | Forwards]}};
        false ->
            {reply, {error, already_exists}, State}
    end;

handle_call({del_forward, Topic}, _From, State = #state{forwards = Forwards}) ->
    case lists:member(Topic, Forwards) of
        true ->
            emqx_broker:unsubscribe(Topic),
            {reply, ok, State#state{forwards = lists:delete(Topic, Forwards)}};
        false ->
            {reply, ok, State}
    end;

handle_call(show_subscriptions, _From, State = #state{subscriptions = Subscriptions}) ->
    {reply, Subscriptions, State};

handle_call({add_subscription, Topic, Qos}, _From, State = #state{subscriptions = Subscriptions, client_pid = ClientPid}) ->
    case not lists:keymember(Topic, 1, Subscriptions) of
        true ->
            emqx_client:subscribe(ClientPid, {Topic, Qos}),
            {reply, ok, State#state{subscriptions = [{Topic, Qos} | Subscriptions]}};
        false ->
            {reply, {error, already_exists}, State}
    end;

handle_call({del_subscription, Topic}, _From, State = #state{subscriptions = Subscriptions, client_pid = ClientPid}) ->
    case lists:keymember(Topic, 1, Subscriptions) of
        true ->
            emqx_client:unsubscribe(ClientPid, Topic),
            {reply, ok, State#state{subscriptions = lists:keydelete(Topic, 1, Subscriptions)}};
        false ->
            {reply, ok, State}
    end;

handle_call(Req, _From, State) ->
    emqx_logger:error("[Bridge] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Bridge] unexpected cast: ~p", [Msg]),
    {noreply, State}.

%%----------------------------------------------------------------
%% Start or restart bridge
%%----------------------------------------------------------------
handle_info(start, State) ->
    {_Msg, NewState} = bridge(start, State),
    {noreply, NewState};

handle_info(restart, State) ->
    {_Msg, NewState} = bridge(restart, State),
    {noreply, NewState};

%%----------------------------------------------------------------
%% pop message from replayq and publish again
%%----------------------------------------------------------------
handle_info(pop, State = #state{writeq = WriteQ, replayq = ReplayQ,
                                queue_option = #{batch_size := BatchSize}}) ->
    {NewReplayQ, AckRef, NewReadQ} = replayq:pop(ReplayQ, #{count_limit => BatchSize}),
    {NewReadQ1, NewWriteQ} = case NewReadQ of
                                 [] -> {WriteQ, []};
                                 _ -> {NewReadQ, WriteQ}
                             end,
    self() ! replay,
    {noreply, State#state{readq = NewReadQ1, writeq = NewWriteQ, replayq = NewReplayQ, ackref = AckRef}};

handle_info(dump, State = #state{writeq = WriteQ, replayq = ReplayQ}) ->
    NewReplayQueue = replayq:append(ReplayQ, lists:reverse(WriteQ)),
    {noreply, State#state{replayq = NewReplayQueue, writeq = []}};

%%----------------------------------------------------------------
%% replay message from replayq
%%----------------------------------------------------------------
handle_info(replay, State = #state{client_pid = ClientPid, readq = ReadQ}) ->
    {ok, NewReadQ} = publish_readq_msg(ClientPid, ReadQ, []),
    {noreply, State#state{readq = NewReadQ}};

%%----------------------------------------------------------------
%% received local node message
%%----------------------------------------------------------------
handle_info({dispatch, _, #message{topic = Topic, qos = QoS, payload = Payload, flags = #{retain := Retain}}},
            State = #state{client_pid = undefined,
                           mountpoint = Mountpoint})
  when QoS =< 1 ->
    Msg = #mqtt_msg{qos = 1,
                    retain = Retain,
                    topic = mountpoint(Mountpoint, Topic),
                    payload = Payload},
    {noreply, en_writeq({undefined, Msg}, State)};
handle_info({dispatch, _, #message{topic = Topic, qos = QoS ,payload = Payload, flags = #{retain := Retain}}},
            State = #state{client_pid = Pid,
                           mountpoint = Mountpoint})
  when QoS =< 1 ->
    Msg = #mqtt_msg{qos     = 1,
                    retain  = Retain,
                    topic   = mountpoint(Mountpoint, Topic),
                    payload = Payload},
    case emqx_client:publish(Pid, Msg) of
        {ok, PktId} ->
            {noreply, en_writeq({PktId, Msg}, State)};
        {error, {PktId, Reason}} ->
            emqx_logger:error("[Bridge] Publish fail:~p", [Reason]),
            {noreply, en_writeq({PktId, Msg}, State)}
    end;

%%----------------------------------------------------------------
%% received remote node message
%%----------------------------------------------------------------
handle_info({publish, #{qos := QoS, dup := Dup, retain := Retain, topic := Topic,
                        properties := Props, payload := Payload}}, State) ->
    NewMsg0 = emqx_message:make(bridge, QoS, Topic, Payload),
    NewMsg1 = emqx_message:set_headers(Props, emqx_message:set_flags(#{dup => Dup, retain => Retain}, NewMsg0)),
    emqx_broker:publish(NewMsg1),
    {noreply, State};

%%----------------------------------------------------------------
%% received remote puback message
%%----------------------------------------------------------------
handle_info({puback, #{packet_id := PktId}}, State) ->
    {noreply, delete(PktId, State)};

handle_info({'EXIT', Pid, normal}, State = #state{client_pid = Pid}) ->
    emqx_logger:warning("[Bridge] stop ~p", [normal]),
    self() ! dump,
    {noreply, State#state{client_pid = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = Pid,
                                                  reconnect_interval = ReconnectInterval}) ->
    emqx_logger:error("[Bridge] stop ~p", [Reason]),
    self() ! dump,
    erlang:send_after(ReconnectInterval, self(), restart),
    {noreply, State#state{client_pid = undefined}};

handle_info(Info, State) ->
    emqx_logger:error("[Bridge] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

subscribe_remote_topics(ClientPid, Subscriptions) ->
    [begin emqx_client:subscribe(ClientPid, {bin(Topic), Qos}), {bin(Topic), Qos} end
        || {Topic, Qos} <- Subscriptions, emqx_topic:validate({filter, bin(Topic)})].

subscribe_local_topics(Options) ->
    Topics = get_value(forwards, Options, []),
    Subid = get_value(client_id, Options, <<"bridge">>),
    [begin emqx_broker:subscribe(bin(Topic), #{qos => 1, subid => Subid}), bin(Topic) end
        || Topic <- Topics, emqx_topic:validate({filter, bin(Topic)})].

proto_ver(mqttv3) -> v3;
proto_ver(mqttv4) -> v4;
proto_ver(mqttv5) -> v5.
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
options([{ssl, Ssl}| Options], Acc) ->
    options(Options, [{ssl, Ssl}|Acc]);
options([{ssl_opts, SslOpts}| Options], Acc) ->
    options(Options, [{ssl_opts, SslOpts}|Acc]);
options([_Option | Options], Acc) ->
    options(Options, Acc).

name(Id) ->
    list_to_atom(lists:concat([?MODULE, "_", Id])).

bin(L) -> iolist_to_binary(L).

mountpoint(undefined, Topic) ->
    Topic;
mountpoint(Prefix, Topic) ->
    <<Prefix/binary, Topic/binary>>.

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(bin(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

en_writeq(Msg, State = #state{replayq = ReplayQ,
                              queue_option = #{mem_cache := false}}) ->
    NewReplayQ = replayq:append(ReplayQ, [Msg]),
    State#state{replayq = NewReplayQ};
en_writeq(Msg, State = #state{writeq = WriteQ,
                              queue_option = #{batch_size := BatchSize,
                                               mem_cache := true}})
  when length(WriteQ) < BatchSize->
    State#state{writeq = [Msg | WriteQ]} ;
en_writeq(Msg, State = #state{writeq = WriteQ, replayq = ReplayQ,
                              queue_option = #{mem_cache := true}}) ->
    NewReplayQ =replayq:append(ReplayQ, lists:reverse(WriteQ)),
    State#state{writeq = [Msg], replayq = NewReplayQ}.

publish_readq_msg(_ClientPid, [], NewReadQ) ->
    {ok, NewReadQ};
publish_readq_msg(ClientPid, [{_PktId, Msg} | ReadQ], NewReadQ) ->
    {ok, PktId} = emqx_client:publish(ClientPid, Msg),
    publish_readq_msg(ClientPid, ReadQ, [{PktId, Msg} | NewReadQ]).

delete(PktId, State = #state{ replayq = ReplayQ,
                              readq = [],
                              queue_option = #{ mem_cache := false}}) ->
    {NewReplayQ, NewAckRef, Msgs} = replayq:pop(ReplayQ, #{count_limit => 1}),
    logger:debug("[Msg] PacketId ~p, Msg: ~p", [PktId, Msgs]),
    ok = replayq:ack(NewReplayQ, NewAckRef),
    case Msgs of
        [{PktId, _Msg}] ->
            self() ! pop,
            State#state{ replayq = NewReplayQ, ackref = NewAckRef };
        [{_PktId, _Msg}] ->
            NewReplayQ1 = replayq:append(NewReplayQ, Msgs),
            self() ! pop,
            State#state{ replayq = NewReplayQ1, ackref = NewAckRef };
        _Empty ->
            State#state{ replayq = NewReplayQ, ackref = NewAckRef}
    end;
delete(_PktId, State = #state{readq = [], writeq = [], replayq = ReplayQ, ackref = AckRef}) ->
    ok = replayq:ack(ReplayQ, AckRef),
    self() ! pop,
    State;

delete(PktId, State = #state{readq = [], writeq = WriteQ}) ->
    State#state{writeq = lists:keydelete(PktId, 1, WriteQ)};

delete(PktId, State = #state{readq = ReadQ, replayq = ReplayQ, ackref = AckRef}) ->
    NewReadQ = lists:keydelete(PktId, 1, ReadQ),
    case NewReadQ of
        [] ->
            ok = replayq:ack(ReplayQ, AckRef),
            self() ! pop;
        _NewReadQ ->
            ok
    end,
    State#state{ readq = NewReadQ }.

bridge(Action, State = #state{options = Options,
                              replayq = ReplayQ,
                              queue_option
                              = QueueOption
                              = #{batch_size := BatchSize}})
  when BatchSize > 0 ->
    case emqx_client:start_link([{owner, self()} | options(Options)]) of
        {ok, ClientPid} ->
            case emqx_client:connect(ClientPid) of
                {ok, _} ->
                    emqx_logger:info("[Bridge] connected to remote successfully"),
                    Subs = subscribe_remote_topics(ClientPid, get_value(subscriptions, Options, [])),
                    Forwards = subscribe_local_topics(Options),
                    {NewReplayQ, AckRef, ReadQ} = open_replayq(ReplayQ, QueueOption),
                    {ok, NewReadQ} = publish_readq_msg(ClientPid, ReadQ, []),
                    {<<"start bridge successfully">>,
                     State#state{client_pid = ClientPid,
                                 subscriptions = Subs,
                                 readq = NewReadQ,
                                 replayq = NewReplayQ,
                                 ackref = AckRef,
                                 forwards = Forwards}};
                {error, Reason} ->
                    emqx_logger:error("[Bridge] connect to remote failed! error: ~p", [Reason]),
                    {<<"connect to remote failed">>,
                     State#state{client_pid = ClientPid}}
            end;
        {error, Reason} ->
            emqx_logger:error("[Bridge] ~p failed! error: ~p", [Action, Reason]),
            {<<"start bridge failed">>, State}
    end;
bridge(Action, State) ->
    emqx_logger:error("[Bridge] ~p failed! error: batch_size should greater than zero", [Action]),
    {<<"Open Replayq failed">>, State}.

open_replayq(undefined, #{batch_size := BatchSize,
                          replayq_dir := ReplayqDir,
                          replayq_seg_bytes := ReplayqSegBytes}) ->
    ReplayQ = replayq:open(#{dir => ReplayqDir,
                             seg_bytes => ReplayqSegBytes,
                             sizer => fun(Term) ->
                                          size(term_to_binary(Term))
                                      end,
                             marshaller => fun({PktId, Msg}) ->
                                               term_to_binary({PktId, Msg});
                                              (Bin) ->
                                               binary_to_term(Bin)
                                           end}),
    replayq:pop(ReplayQ, #{count_limit => BatchSize});
open_replayq(ReplayQ, #{batch_size := BatchSize}) ->
    replayq:pop(ReplayQ, #{count_limit => BatchSize}).
