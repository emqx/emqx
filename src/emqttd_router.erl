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
%%%-----------------------------------------------------------------------------
-module(emqttd_router).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([init/1, route/2, lookup_routes/1,
         add_routes/2, delete_routes/1, delete_routes/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%------------------------------------------------------------------------------
%% @doc Create route tables.
%% @end
%%------------------------------------------------------------------------------
init(_Opts) ->
    TabOpts = [bag, public, named_table,
               {write_concurrency, true}],
    %% Route Table: Topic -> {Pid, QoS}
    %% Route Shard: {Topic, Shard} -> {Pid, QoS}
    ensure_tab(route, TabOpts),

    %% Reverse Route Table: Pid -> {Topic, QoS}
    ensure_tab(reverse_route, TabOpts).

ensure_tab(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined ->
            ets:new(Tab, Opts);
        _ ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @doc Add Routes.
%% @end
%%------------------------------------------------------------------------------
-spec add_routes(list({binary(), mqtt_qos()}), pid()) -> ok.
add_routes(TopicTable, Pid) when is_pid(Pid) ->
    case lookup_routes(Pid) of
        [] ->
            erlang:monitor(process, Pid),
            insert_routes(TopicTable, Pid);
        TopicInEts ->
            {NewTopics, UpdatedTopics} = diff(TopicTable, TopicInEts),
            update_routes(UpdatedTopics, Pid),
            insert_routes(NewTopics, Pid)
    end.

%%------------------------------------------------------------------------------
%% @doc Lookup Routes
%% @end
%%------------------------------------------------------------------------------
-spec lookup_routes(pid()) -> list({binary(), mqtt_qos()}).
lookup_routes(Pid) when is_pid(Pid) ->
    [{Topic, Qos} || {_, Topic, Qos} <- ets:lookup(reverse_route, Pid)].

%%------------------------------------------------------------------------------
%% @doc Delete Routes.
%% @end
%%------------------------------------------------------------------------------
-spec delete_routes(list(binary()), pid()) -> ok.
delete_routes(Topics, Pid) ->
    Routes = [{Topic, Pid} || Topic <- Topics],
    lists:foreach(fun delete_route/1, Routes).

-spec delete_routes(pid()) -> ok.
delete_routes(Pid) when is_pid(Pid) ->
    Routes = [{Topic, Pid} || {Topic, _Qos} <- lookup_routes(Pid)],
    ets:delete(reverse_route, Pid),
    lists:foreach(fun delete_route_only/1, Routes).

%%------------------------------------------------------------------------------
%% @doc Route Message on Local Node.
%% @end
%%------------------------------------------------------------------------------
-spec route(binary(), mqtt_message()) -> non_neg_integer().
route(Queue = <<"$Q/", _Q>>, Msg) ->
    case ets:lookup(route, Queue) of
        [] ->
            emqttd_metrics:inc('messages/dropped');
        Routes ->
            Idx = crypto:rand_uniform(1, length(Routes) + 1),
            {_, SubPid, SubQos} = lists:nth(Idx, Routes),
            SubPid ! {dispatch, tune_qos(SubQos, Msg)}
    end;

route(Topic, Msg) ->
    case ets:lookup(route, Topic) of
        [] ->
            emqttd_metrics:inc('messages/dropped');
        Routes ->
            lists:foreach(
                fun({_Topic, SubPid, SubQos}) ->
                    SubPid ! {dispatch, tune_qos(SubQos, Msg)}
                end, Routes)
    end.

tune_qos(SubQos, Msg = #mqtt_message{qos = PubQos}) when PubQos > SubQos ->
    Msg#mqtt_message{qos = SubQos};
tune_qos(_SubQos, Msg) ->
    Msg.

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

insert_routes([], _Pid) ->
    ok;
insert_routes(TopicTable, Pid) ->
    {Routes, ReverseRoutes} = routes(TopicTable, Pid),
    ets:insert(route, Routes),
    ets:insert(reverse_route, ReverseRoutes).

update_routes([], _Pid) ->
    ok;
update_routes(TopicTable, Pid) ->
    {Routes, ReverseRoutes} = routes(TopicTable, Pid),
    lists:foreach(fun update_route/1, Routes),
    lists:foreach(fun update_reverse_route/1, ReverseRoutes).

update_route(Route = {Topic, Pid, _Qos}) ->
    ets:match_delete(route, {Topic, Pid, '_'}),
    ets:insert(route, Route).

update_reverse_route(RevRoute = {Pid, Topic, _Qos}) ->
    ets:match_delete(reverse_route, {Pid, Topic, '_'}),
    ets:insert(reverse_route, RevRoute).

routes(TopicTable, Pid) ->
    F = fun(Topic, Qos) -> {{Topic, Pid, Qos}, {Pid, Topic, Qos}} end,
    lists:unzip([F(Topic, Qos) || {Topic, Qos} <- TopicTable]).

delete_route({Topic, Pid}) ->
    ets:match_delete(reverse_route, {Pid, Topic, '_'}),
    ets:match_delete(route, {Topic, Pid, '_'}).

delete_route_only({Topic, Pid}) ->
    ets:match_delete(route, {Topic, Pid, '_'}).

