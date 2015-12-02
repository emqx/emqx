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
%%% @doc MQTT Message Router on Local Node
%%%
%%% Route Table:
%%%
%%%   Topic -> {Pid1, Qos}, {Pid2, Qos}, ... 
%%%
%%% Reverse Route Table:
%%%
%%%   Pid -> {Topic1, Qos}, {Topic2, Qos}, ...
%%%
%%% @end
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%
%%%-----------------------------------------------------------------------------
-module(emqttd_router).

-behaviour(gen_server2).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([start_link/2, add_routes/1, add_routes/2, route/2,
         delete_routes/1, delete_routes/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%TODO: test...
-compile(export_all).

%%%=============================================================================
%%% API Function Definitions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start router.
%% @end
%%------------------------------------------------------------------------------
start_link(Id, Opts) ->
    gen_server2:start_link(?MODULE, [Id, Opts], []).

%%------------------------------------------------------------------------------
%% @doc Add Routes.
%% @end
%%------------------------------------------------------------------------------
-spec add_routes(list({binary(), mqtt_qos()})) -> ok.
add_routes(TopicTable) ->
    add_routes(TopicTable, self()).
    
-spec add_routes(list({binary(), mqtt_qos()}), pid()) -> ok.
add_routes(TopicTable, Pid) ->
    Router = gproc_pool:pick_worker(router, Pid),
    gen_server2:cast(Router, {add_routes, TopicTable, Pid}).

%%------------------------------------------------------------------------------
%% @doc Lookup topics that a pid subscribed.
%% @end
%%------------------------------------------------------------------------------
-spec lookup(pid()) -> list({binary(), mqtt_qos()}).
lookup(Pid) when is_pid(Pid) ->
    [{Topic, Qos} || {_, Topic, Qos} <- ets:lookup(reverse_route, Pid)].

%%------------------------------------------------------------------------------
%% @doc Route Message on Local Node.
%% @end
%%------------------------------------------------------------------------------
-spec route(Topic :: binary(), Msg :: mqtt_message()) -> non_neg_integer().
route(Queue = <<"$Q/", _Q>>, Msg) ->
    case ets:lookup(route, Queue) of
        [] ->
            setstats(dropped, true);
        Routes ->
            Idx = random:uniform(length(Routes)),
            {_, SubPid, SubQos} = lists:nth(Idx, Routes),
            SubPid ! {dispatch, tune_qos(SubQos, Msg)}
    end;

route(Topic, Msg) ->
    Routes = ets:lookup(route, Topic),
    setstats(dropped, Routes =:= []),
    lists:foreach(
        fun({_Topic, SubPid, SubQos}) ->
            SubPid ! {dispatch, tune_qos(SubQos, Msg)}
        end, Routes).

tune_qos(SubQos, Msg = #mqtt_message{qos = PubQos}) when PubQos > SubQos ->
    Msg#mqtt_message{qos = SubQos};
tune_qos(_SubQos, Msg) ->
    Msg.

%%------------------------------------------------------------------------------
%% @doc Delete Routes.
%% @end
%%------------------------------------------------------------------------------
-spec delete_routes(list(binary())) -> ok.
delete_routes(Topics) ->
    delete_routes(Topics, self()). 

-spec delete_routes(list(binary()), pid()) -> ok.
delete_routes(Topics, Pid) ->
    Router = gproc_pool:pick_worker(router, Pid),
    gen_server2:cast(Router, {delete_routes, Topics, Pid}).

%%%=============================================================================
%%% gen_server Function Definitions
%%%=============================================================================

init([Id, Opts]) ->
    %% Only ETS Operations
    process_flag(priority, high),

    %% Aging Timer
    AgingSecs = proplists:get_value(aging, Opts, 5),

    {ok, TRef} = timer:send_interval(timer:seconds(AgingSecs), aging),

    gproc_pool:connect_worker(router, {?MODULE, Id}),

    {ok, #state{aging = #aging{topics = [], timer = TRef}}}.

handle_call(Req, _From, State) ->
    lager:error("Unexpected Request: ~p", [Req]),
    {reply, {error, unsupported_req}, State}.

handle_cast({add_routes, TopicTable, Pid}, State) ->
    case lookup(Pid) of
        [] ->
            erlang:monitor(process, Pid),
            ets_add_routes(TopicTable, Pid);
        TopicInEts ->
            {NewTopics, UpdatedTopics} = diff(TopicTable, TopicInEts),
            ets_update_routes(UpdatedTopics, Pid),
            ets_add_routes(NewTopics, Pid)
    end,
    {noreply, State};

handle_cast({delete, Topics, Pid}, State) ->
    Routes = [{Topic, Pid} || Topic <- Topics],
    lists:foreach(fun ets_delete_route/1, Routes),
    %% TODO: aging route......
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State) ->
    Topics = [Topic || {Topic, _Qos} <- lookup(DownPid)],
    ets:delete(reverse_route, DownPid),
    lists:foreach(fun(Topic) ->
            ets:match_delete(route, {Topic, DownPid, '_'})
        end, Topics),
    %% TODO: aging route......
    {noreply, State};

handle_info(aging, State = #state{aging = #aging{topics = Topics}}) ->
    %%TODO.. aging
    %%io:format("Aging Topics: ~p~n", [Topics]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{id = Id, aging = #aging{timer = TRef}}) ->
    timer:cancel(TRef),
    gproc_pool:connect_worker(route, {?MODULE, Id}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal Functions
%%%=============================================================================

diff(TopicTable, TopicInEts) ->
    diff(TopicTable, TopicInEts, [], []).

diff([], _TopicInEts, NewAcc, UpAcc) ->
    {NewAcc, UpAcc};

diff([{Topic, Qos}|TopicTable], TopicInEts, NewAcc, UpAcc) ->
    case lists:keyfind(Topic, 1, TopicInEts) of
        {Topic, Qos}  ->
            diff(TopicTable, TopicInEts, NewAcc, UpAcc);
        {Topic, _Qos} ->
            diff(TopicTable, TopicInEts, NewAcc, [{Topic, Qos}|UpAcc]);
        false ->
            diff(TopicTable, TopicInEts, [{Topic, Qos}|NewAcc], UpAcc)
    end.

ets_add_routes([], _Pid) ->
    ok;
ets_add_routes(TopicTable, Pid) ->
    {Routes, ReverseRoutes} = routes(TopicTable, Pid),
    ets:insert(route, Routes),
    ets:insert(reverse_route, ReverseRoutes).

ets_update_routes([], _Pid) ->
    ok;
ets_update_routes(TopicTable, Pid) ->
    {Routes, ReverseRoutes} = routes(TopicTable, Pid),
    lists:foreach(fun ets_update_route/1, Routes),
    lists:foreach(fun ets_update_reverse_route/1, ReverseRoutes).

ets_update_route(Route = {Topic, Pid, _Qos}) ->
    ets:match_delete(route, {Topic, Pid, '_'}),
    ets:insert(route, Route).

ets_update_reverse_route(RevRoute = {Pid, Topic, _Qos}) ->
    ets:match_delete(reverse_route, {Pid, Topic, '_'}),
    ets:insert(reverse_route, RevRoute).

ets_delete_route({Topic, Pid}) ->
    ets:match_delete(reverse_route, {Pid, Topic, '_'}),
    ets:match_delete(route, {Topic, Pid, '_'}).

routes(TopicTable, Pid) ->
    F = fun(Topic, Qos) -> {{Topic, Pid, Qos}, {Pid, Topic, Qos}} end,
    lists:unzip([F(Topic, Qos) || {Topic, Qos} <- TopicTable]).

setstats(dropped, false) ->
    ignore;

setstats(dropped, true) ->
    emqttd_metrics:inc('messages/dropped').

