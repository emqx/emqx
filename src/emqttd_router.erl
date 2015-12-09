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
%%%   Topic -> Pid1, Pid2, ...
%%%
%%% Reverse Route Table:
%%%
%%%   Pid -> Topic1, Topic2, ...
%%%
%%% @end
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_router).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([init/1, route/2, lookup_routes/1, has_route/1,
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
-spec add_routes(list(binary()), pid()) -> ok.
add_routes(Topics, Pid) when is_pid(Pid) ->
    with_stats(fun() ->
        case lookup_routes(Pid) of
            [] ->
                erlang:monitor(process, Pid),
                insert_routes(Topics, Pid);
            InEts ->
                insert_routes(Topics -- InEts, Pid)
        end
    end).

%%------------------------------------------------------------------------------
%% @doc Lookup Routes
%% @end
%%------------------------------------------------------------------------------
-spec lookup_routes(pid()) -> list(binary()).
lookup_routes(Pid) when is_pid(Pid) ->
    [Topic || {_, Topic} <- ets:lookup(reverse_route, Pid)].

%%------------------------------------------------------------------------------
%% @doc Has Route?
%% @end
%%------------------------------------------------------------------------------
-spec has_route(binary()) -> boolean().
has_route(Topic) ->
    ets:member(route, Topic).

%%------------------------------------------------------------------------------
%% @doc Delete Routes
%% @end
%%------------------------------------------------------------------------------
-spec delete_routes(list(binary()), pid()) -> ok.
delete_routes(Topics, Pid) ->
    with_stats(fun() ->
        Routes = [{Topic, Pid} || Topic <- Topics],
        lists:foreach(fun delete_route/1, Routes)
    end).

-spec delete_routes(pid()) -> ok.
delete_routes(Pid) when is_pid(Pid) ->
    with_stats(fun() ->
        Routes = [{Topic, Pid} || Topic <- lookup_routes(Pid)],
        ets:delete(reverse_route, Pid),
        lists:foreach(fun delete_route_only/1, Routes)
    end).

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
            {_, SubPid} = lists:nth(Idx, Routes),
            dispatch(SubPid, Queue, Msg)
    end;

route(Topic, Msg) ->
    case ets:lookup(route, Topic) of
        [] ->
            emqttd_metrics:inc('messages/dropped');
        Routes ->
            lists:foreach(fun({_Topic, SubPid}) ->
                            dispatch(SubPid, Topic, Msg)
                          end, Routes)
    end.

dispatch(SubPid, Topic, Msg) -> SubPid ! {dispatch, Topic, Msg}.

%%%=============================================================================
%%% Internal Functions
%%%=============================================================================

insert_routes([], _Pid) ->
    ok;
insert_routes(Topics, Pid) ->
    {Routes, ReverseRoutes} = routes(Topics, Pid),
    ets:insert(route, Routes),
    ets:insert(reverse_route, ReverseRoutes).

routes(Topics, Pid) ->
    lists:unzip([{{Topic, Pid}, {Pid, Topic}} || Topic <- Topics]).

delete_route({Topic, Pid}) ->
    ets:delete_object(reverse_route, {Pid, Topic}),
    ets:delete_object(route, {Topic, Pid}).

delete_route_only({Topic, Pid}) ->
    ets:delete_object(route, {Topic, Pid}).

with_stats(Fun) ->
    Ok = Fun(), setstats(), Ok.

setstats() ->
    lists:foreach(fun setstat/1, [{route, 'routes/count'},
                                  {reverse_route, 'routes/reverse'}]).

setstat({Tab, Stat}) ->
    emqttd_stats:setstat(Stat, ets:info(Tab, size)).

