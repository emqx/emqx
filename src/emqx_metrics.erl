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

-module(emqx_metrics).

-behaviour(gen_server).

%% API Function Exports
-export([start_link/0, create/1]).

-export([all/0, value/1, inc/1, inc/2, inc/3, dec/2, dec/3, set/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tick}).

-define(TAB, ?MODULE).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the metrics server
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

create({gauge, Name}) ->
    ets:insert(?TAB, {{Name, 0}, 0});

create({counter, Name}) ->
    Schedulers = lists:seq(1, erlang:system_info(schedulers)),
    ets:insert(?TAB, [{{Name, I}, 0} || I <- Schedulers]).


%% @doc Get all metrics
-spec(all() -> [{atom(), non_neg_integer()}]).
all() ->
    maps:to_list(
        ets:foldl(
            fun({{Metric, _N}, Val}, Map) ->
                    case maps:find(Metric, Map) of
                        {ok, Count} -> maps:put(Metric, Count+Val, Map);
                        error -> maps:put(Metric, Val, Map)
                    end
            end, #{}, ?TAB)).

%% @doc Get metric value
-spec(value(atom()) -> non_neg_integer()).
value(Metric) ->
    lists:sum(ets:select(?TAB, [{{{Metric, '_'}, '$1'}, [], ['$1']}])).

%% @doc Increase counter
-spec(inc(atom()) -> non_neg_integer()).
inc(Metric) ->
    inc(counter, Metric, 1).

%% @doc Increase metric value
-spec(inc({counter | gauge, atom()} | atom(), pos_integer()) -> non_neg_integer()).
inc({gauge, Metric}, Val) ->
    inc(gauge, Metric, Val);
inc({counter, Metric}, Val) ->
    inc(counter, Metric, Val);
inc(Metric, Val) when is_atom(Metric) ->
    inc(counter, Metric, Val).

%% @doc Increase metric value
-spec(inc(counter | gauge, atom(), pos_integer()) -> pos_integer()).
inc(gauge, Metric, Val) ->
    ets:update_counter(?TAB, key(gauge, Metric), {2, Val});
inc(counter, Metric, Val) ->
    ets:update_counter(?TAB, key(counter, Metric), {2, Val}).

%% @doc Decrease metric value
-spec(dec(gauge, atom()) -> integer()).
dec(gauge, Metric) ->
    dec(gauge, Metric, 1).

%% @doc Decrease metric value
-spec(dec(gauge, atom(), pos_integer()) -> integer()).
dec(gauge, Metric, Val) ->
    ets:update_counter(?TAB, key(gauge, Metric), {2, -Val}).

%% @doc Set metric value
set(Metric, Val) when is_atom(Metric) ->
    set(gauge, Metric, Val).
set(gauge, Metric, Val) ->
    ets:insert(?TAB, {key(gauge, Metric), Val}).

%% @doc Metric Key
key(gauge, Metric) ->
    {Metric, 0};
key(counter, Metric) ->
    {Metric, erlang:system_info(scheduler_id)}.

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqx_time:seed(),
    % Create metrics table
    ets:new(?TAB, [set, public, named_table, {write_concurrency, true}]),
    % Tick to publish metrics
    {ok, #state{tick = emqx_broker:start_tick(tick)}, hibernate}.

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    % publish metric message
    [publish(Metric, Val) || {Metric, Val} <- all()],
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tick = TRef}) ->
    emqx_broker:stop_tick(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish(Metric, Val) ->
    Msg = emqx_message:make(metrics, metric_topic(Metric), bin(Val)),
    emqx_broker:publish(emqx_message:set_flag(sys, Msg)).

metric_topic(Metric) ->
    emqx_topic:systop(list_to_binary(lists:concat(['metrics/', Metric]))).

bin(I) when is_integer(I) -> list_to_binary(integer_to_list(I)).

