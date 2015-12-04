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
%%% @doc PubSub Route Aging Helper
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub_helper).

-behaviour(gen_server2).

-include("emqttd.hrl").

%% API Function Exports
-export([start_link/1, aging/1, setstats/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-record(aging, {topics, time, tref}).

-record(state, {aging :: #aging{}}).

-define(SERVER, ?MODULE).

-define(ROUTER, emqttd_router).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start pubsub helper. 
%% @end
%%------------------------------------------------------------------------------
-spec start_link(list(tuple())) -> {ok, pid()} | ignore | {error, any()}. 
start_link(Opts) ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

%%------------------------------------------------------------------------------
%% @doc Aging topics
%% @end
%%------------------------------------------------------------------------------
-spec aging(list(binary())) -> ok.
aging(Topics) ->
    gen_server2:cast(?SERVER, {aging, Topics}).

setstats(topic) ->
    emqttd_stats:setstats('topics/count', 'topics/max',
                          mnesia:table_info(topic, size));
setstats(subscription) ->
    emqttd_stats:setstats('subscriptions/count', 'subscriptions/max',
                          mnesia:table_info(subscription, size)).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Opts]) ->

    mnesia:subscribe(system),

    AgingSecs = proplists:get_value(route_aging, Opts, 5),

    %% Aging Timer
    {ok, AgingTref} = start_tick(AgingSecs div 2),

    {ok, #state{aging = #aging{topics = dict:new(),
                               time   = AgingSecs,
                               tref   = AgingTref}}}.

start_tick(Secs) ->
    timer:send_interval(timer:seconds(Secs), {clean, aged}).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({aging, Topics}, State = #state{aging = Aging}) ->
    #aging{topics = Dict} = Aging,
    TS = emqttd_util:now_to_secs(),
    Dict1 =
    lists:foldl(fun(Topic, Acc) ->
                    case dict:find(Topic, Acc) of
                        {ok, _} -> Acc;
                        error   -> dict:store(Topic, TS, Acc)
                    end
                end, Dict, Topics),
    {noreply, State#state{aging = Aging#aging{topics = Dict1}}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({clean, aged}, State = #state{aging = Aging}) ->

    #aging{topics = Dict, time = Time} = Aging,

    ByTime  = emqttd_util:now_to_secs() - Time,

    Dict1 = try_clean(ByTime, dict:to_list(Dict)),

    NewAging = Aging#aging{topics = dict:from_list(Dict1)},

    {noreply, State#state{aging = NewAging}, hibernate};

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    Pattern = #mqtt_topic{_ = '_', node = Node},
    F = fun() ->
            [mnesia:delete_object(topic, R, write) ||
                R <- mnesia:match_object(topic, Pattern, write)]
        end,
    mnesia:async_dirty(F),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{aging = #aging{tref = TRef}}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal Functions
%%%=============================================================================

try_clean(ByTime, List) ->
    try_clean(ByTime, List, []).

try_clean(_ByTime, [], Acc) ->
    Acc;

try_clean(ByTime, [{Topic, TS} | Left], Acc) ->
    case ?ROUTER:has_route(Topic) of
        false ->
            try_clean2(ByTime, {Topic, TS}, Left, Acc);
        true  ->
            try_clean(ByTime, Left, Acc)
    end.

try_clean2(ByTime, {Topic, TS}, Left, Acc) when TS > ByTime ->
    try_clean(ByTime, Left, [{Topic, TS}|Acc]);

try_clean2(ByTime, {Topic, _TS}, Left, Acc) ->
    TopicR = #mqtt_topic{topic = Topic, node = node()},
    io:format("Try to remove topic: ~p~n", [Topic]),
    mnesia:transaction(fun try_remove_topic/1, [TopicR]),
    try_clean(ByTime, Left, Acc).

try_remove_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    %% Lock topic first
    case mnesia:wread({topic, Topic}) of
        [] -> ok;
        [TopicR] ->
            if_no_route(Topic, fun() ->
                %% Remove topic and trie
                mnesia:delete_object(topic, TopicR, write),
                emqttd_trie:delete(Topic)
            end);
        _More ->
            if_no_route(Topic, fun() ->
                %% Remove topic
                mnesia:delete_object(topic, TopicR, write)
            end)
    end.
        
if_no_route(Topic, Fun) ->
    case ?ROUTER:has_route(Topic) of
        true -> ok;
        false -> Fun()
    end.

