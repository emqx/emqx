%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc emqttd bridge
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_bridge).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API Function Exports
-export([start_link/3]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PING_DOWN_INTERVAL, 1000).

-record(state, {node, subtopic,
                qos                = ?QOS_2,
                topic_suffix       = <<>>,
                topic_prefix       = <<>>,
                mqueue            :: emqttd_mqueue:mqueue(),
                max_queue_len      = 10000,
                ping_down_interval = ?PING_DOWN_INTERVAL,
                status             = up}).

-type option()  :: {qos, mqtt_qos()} |
                   {topic_suffix, binary()} |
                   {topic_prefix, binary()} |
                   {max_queue_len, pos_integer()} |
                   {ping_down_interval, pos_integer()}.

-export_type([option/0]).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start a bridge
%% @end
%%------------------------------------------------------------------------------
-spec start_link(atom(), binary(), [option()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Node, SubTopic, Options) ->
    gen_server:start_link(?MODULE, [Node, SubTopic, Options], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Node, SubTopic, Options]) ->
    process_flag(trap_exit, true),
    case net_kernel:connect_node(Node) of
        true -> 
            true = erlang:monitor_node(Node, true),
            State = parse_opts(Options, #state{node = Node, subtopic = SubTopic}),
            MQueue = emqttd_mqueue:new(qname(Node, SubTopic),
                                       [{max_len, State#state.max_queue_len}],
                                       emqttd_alarm:alarm_fun()),
            emqttd_pubsub:subscribe({SubTopic, State#state.qos}),
            {ok, State#state{mqueue = MQueue}};
        false -> 
            {stop, {cannot_connect, Node}}
    end.

parse_opts([], State) ->
    State;
parse_opts([{qos, Qos} | Opts], State) ->
    parse_opts(Opts, State#state{qos = Qos});
parse_opts([{topic_suffix, Suffix} | Opts], State) ->
    parse_opts(Opts, State#state{topic_suffix= Suffix});
parse_opts([{topic_prefix, Prefix} | Opts], State) ->
    parse_opts(Opts, State#state{topic_prefix = Prefix});
parse_opts([{max_queue_len, Len} | Opts], State) ->
    parse_opts(Opts, State#state{max_queue_len = Len});
parse_opts([{ping_down_interval, Interval} | Opts], State) ->
    parse_opts(Opts, State#state{ping_down_interval = Interval*1000});
parse_opts([_Opt | Opts], State) ->
    parse_opts(Opts, State).

qname(Node, SubTopic) when is_atom(Node) ->
    qname(atom_to_list(Node), SubTopic);
qname(Node, SubTopic) ->
    list_to_binary(["Bridge:", Node, ":", SubTopic]).

handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({dispatch, Msg}, State = #state{mqueue = MQ, status = down}) ->
    {noreply, State#state{mqueue = emqttd_mqueue:in(Msg, MQ)}};

handle_info({dispatch, Msg}, State = #state{node = Node, status = up}) ->
    rpc:cast(Node, emqttd_pubsub, publish, [transform(Msg, State)]),
    {noreply, State};

handle_info({nodedown, Node}, State = #state{node = Node, ping_down_interval = Interval}) ->
    lager:warning("Bridge Node Down: ~p", [Node]),
    erlang:send_after(Interval, self(), ping_down_node),
    {noreply, State#state{status = down}};

handle_info({nodeup, Node}, State = #state{node = Node}) ->
    %% TODO: Really fast??
    case emqttd:is_running(Node) of
        true -> 
            lager:warning("Bridge Node Up: ~p", [Node]),
            {noreply, dequeue(State#state{status = up})};
        false ->
            self() ! {nodedown, Node},
            {noreply, State#state{status = down}}
    end;

handle_info(ping_down_node, State = #state{node = Node, ping_down_interval = Interval}) ->
    Self = self(),
    spawn_link(fun() ->
                 case net_kernel:connect_node(Node) of
                     true -> %%TODO: this is not right... fixme later
                         Self ! {nodeup, Node};
                     false ->
                         erlang:send_after(Interval, Self, ping_down_node)
                 end
               end),
    {noreply, State};

handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

dequeue(State = #state{mqueue = MQ}) ->
    case emqttd_mqueue:out(MQ) of
        {empty, MQ1} ->
            State#state{mqueue = MQ1};
        {{value, Msg}, MQ1} ->
            handle_info({dispatch, Msg}, State),
            dequeue(State#state{mqueue = MQ1})
    end.

transform(Msg = #mqtt_message{topic = Topic}, #state{topic_prefix = Prefix,
                                                     topic_suffix = Suffix}) ->
    Msg#mqtt_message{topic = <<Prefix/binary, Topic/binary, Suffix/binary>>}.

