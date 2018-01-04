%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_bridge).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

%% API Function Exports
-export([start_link/5]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PING_DOWN_INTERVAL, 1000).

-record(state, {pool, id,
                node, subtopic,
                topic_suffix       = <<>>,
                topic_prefix       = <<>>,
                mqueue             :: emqttd_mqueue:mqueue(),
                max_queue_len      = 10000,
                ping_down_interval = ?PING_DOWN_INTERVAL,
                status             = up}).

-type(option() :: {topic_suffix, binary()} |
                  {topic_prefix, binary()} |
                  {max_queue_len, pos_integer()} |
                  {ping_down_interval, pos_integer()}).

-export_type([option/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a bridge
-spec(start_link(any(), pos_integer(), atom(), binary(), [option()]) ->
    {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id, Node, Topic, Options) ->
    gen_server2:start_link(?MODULE, [Pool, Id, Node, Topic, Options], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Node, Topic, Options]) ->
    ?GPROC_POOL(join, Pool, Id),
    process_flag(trap_exit, true),
    case net_kernel:connect_node(Node) of
        true -> 
            true = erlang:monitor_node(Node, true),
            Share = iolist_to_binary(["$bridge:", atom_to_list(Node), ":", Topic]),
            emqttd:subscribe(Topic, self(), [local, {share, Share}, {qos, ?QOS_0}]),
            State = parse_opts(Options, #state{node = Node, subtopic = Topic}),
            MQueue = emqttd_mqueue:new(qname(Node, Topic),
                                       [{max_len, State#state.max_queue_len}],
                                       emqttd_alarm:alarm_fun()),
            {ok, State#state{pool = Pool, id = Id, mqueue = MQueue},
             hibernate, {backoff, 1000, 1000, 10000}};
        false -> 
            {stop, {cannot_connect_node, Node}}
    end.

parse_opts([], State) ->
    State;
parse_opts([{topic_suffix, Suffix} | Opts], State) ->
    parse_opts(Opts, State#state{topic_suffix= Suffix});
parse_opts([{topic_prefix, Prefix} | Opts], State) ->
    parse_opts(Opts, State#state{topic_prefix = Prefix});
parse_opts([{max_queue_len, Len} | Opts], State) ->
    parse_opts(Opts, State#state{max_queue_len = Len});
parse_opts([{ping_down_interval, Interval} | Opts], State) ->
    parse_opts(Opts, State#state{ping_down_interval = Interval});
parse_opts([_Opt | Opts], State) ->
    parse_opts(Opts, State).

qname(Node, Topic) when is_atom(Node) ->
    qname(atom_to_list(Node), Topic);
qname(Node, Topic) ->
    iolist_to_binary(["Bridge:", Node, ":", Topic]).

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({dispatch, _Topic, Msg}, State = #state{mqueue = MQ, status = down}) ->
    {noreply, State#state{mqueue = emqttd_mqueue:in(Msg, MQ)}};

handle_info({dispatch, _Topic, Msg}, State = #state{node = Node, status = up}) ->
    rpc:cast(Node, emqttd, publish, [transform(Msg, State)]),
    {noreply, State, hibernate};

handle_info({nodedown, Node}, State = #state{node = Node, ping_down_interval = Interval}) ->
    lager:warning("Bridge Node Down: ~p", [Node]),
    erlang:send_after(Interval, self(), ping_down_node),
    {noreply, State#state{status = down}, hibernate};

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
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

dequeue(State = #state{mqueue = MQ}) ->
    case emqttd_mqueue:out(MQ) of
        {empty, MQ1} ->
            State#state{mqueue = MQ1};
        {{value, Msg}, MQ1} ->
            handle_info({dispatch, Msg#mqtt_message.topic, Msg}, State),
            dequeue(State#state{mqueue = MQ1})
    end.

transform(Msg = #mqtt_message{topic = Topic}, #state{topic_prefix = Prefix,
                                                     topic_suffix = Suffix}) ->
    Msg#mqtt_message{topic = <<Prefix/binary, Topic/binary, Suffix/binary>>}.

