%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_bridge).

-behaviour(gen_server).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

%% API Function Exports
-export([start_link/5]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PING_DOWN_INTERVAL, 1000).

-record(state, {pool, id,
                node, subtopic,
                qos                = ?QOS_0,
                topic_suffix       = <<>>,
                topic_prefix       = <<>>,
                mqueue            :: emqx_mqueue:mqueue(),
                max_queue_len      = 10000,
                ping_down_interval = ?PING_DOWN_INTERVAL,
                status             = up}).

-type(option() :: {qos, mqtt_qos()} |
                  {topic_suffix, binary()} |
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
    gen_server:start_link(?MODULE, [Pool, Id, Node, Topic, Options], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Node, Topic, Options]) ->
    gproc_pool:connect_worker(Pool, {Pool, Id}),
    process_flag(trap_exit, true),
    case net_kernel:connect_node(Node) of
        true -> 
            true = erlang:monitor_node(Node, true),
            Share = iolist_to_binary(["$bridge:", atom_to_list(Node), ":", Topic]),
            %% TODO:: local???
            emqx_broker:subscribe(Topic, self(), [local, {share, Share}, {qos, ?QOS_0}]),
            State = parse_opts(Options, #state{node = Node, subtopic = Topic}),
            MQueue = emqx_mqueue:new(qname(Node, Topic),
                                     [{max_len, State#state.max_queue_len}],
                                     emqx_alarm:alarm_fun()),
            {ok, State#state{pool = Pool, id = Id, mqueue = MQueue},
             hibernate, {backoff, 1000, 1000, 10000}};
        false -> 
            {stop, {cannot_connect_node, Node}}
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
    parse_opts(Opts, State#state{ping_down_interval = Interval});
parse_opts([_Opt | Opts], State) ->
    parse_opts(Opts, State).

qname(Node, Topic) when is_atom(Node) ->
    qname(atom_to_list(Node), Topic);
qname(Node, Topic) ->
    iolist_to_binary(["Bridge:", Node, ":", Topic]).

handle_call(Req, _From, State) ->
    lager:error("[~s] Unexpected Call: ~p", [?MODULE, Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    lager:error("[~s] Unexpected Cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info({dispatch, _Topic, Msg}, State = #state{mqueue = MQ, status = down}) ->
    {noreply, State#state{mqueue = emqx_mqueue:in(Msg, MQ)}};

handle_info({dispatch, _Topic, Msg}, State = #state{node = Node, status = up}) ->
    emqx_rpc:cast(Node, emqx, publish, [transform(Msg, State)]),
    {noreply, State, hibernate};

handle_info({nodedown, Node}, State = #state{node = Node, ping_down_interval = Interval}) ->
    lager:warning("Bridge Node Down: ~p", [Node]),
    erlang:send_after(Interval, self(), ping_down_node),
    {noreply, State#state{status = down}, hibernate};

handle_info({nodeup, Node}, State = #state{node = Node}) ->
    %% TODO: Really fast??
    case emqx:is_running(Node) of
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
    lager:error("[~s] Unexpected Info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

dequeue(State = #state{mqueue = MQ}) ->
    case emqx_mqueue:out(MQ) of
        {empty, MQ1} ->
            State#state{mqueue = MQ1};
        {{value, Msg}, MQ1} ->
            handle_info({dispatch, Msg#message.topic, Msg}, State),
            dequeue(State#state{mqueue = MQ1})
    end.

transform(Msg = #message{topic = Topic}, #state{topic_prefix = Prefix,
                                                topic_suffix = Suffix}) ->
    Msg#message{topic = <<Prefix/binary, Topic/binary, Suffix/binary>>}.

