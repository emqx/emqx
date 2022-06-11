%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_metrics_worker).

-behaviour(gen_server).

%% API functions
-export([
    start_link/1,
    stop/1,
    child_spec/1,
    child_spec/2
]).

-export([
    inc/3,
    inc/4,
    get/3,
    get_rate/2,
    get_counters/2,
    create_metrics/3,
    create_metrics/4,
    clear_metrics/2,
    reset_metrics/2,
    has_metrics/2
]).

-export([get_metrics/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_info/2,
    handle_cast/2,
    code_change/3,
    terminate/2
]).

-ifndef(TEST).
-define(SECS_5M, 300).
-define(SAMPLING, 10).
-else.
%% Use 5 secs average rate instead of 5 mins in case of testing
-define(SECS_5M, 5).
-define(SAMPLING, 1).
-endif.

-export_type([metrics/0, handler_name/0, metric_id/0]).

-type rate() :: #{
    current := float(),
    max := float(),
    last5m := float()
}.
-type metrics() :: #{
    counters := #{atom() => integer()},
    rate := #{atom() => rate()}
}.
-type handler_name() :: atom().
-type metric_id() :: binary() | atom().

-define(CntrRef(Name), {?MODULE, Name}).
-define(SAMPCOUNT_5M, (?SECS_5M div ?SAMPLING)).

-record(rate, {
    max = 0 :: number(),
    current = 0 :: number(),
    last5m = 0 :: number(),
    %% metadata for calculating the avg rate
    tick = 1 :: number(),
    last_v = 0 :: number(),
    %% metadata for calculating the 5min avg rate
    last5m_acc = 0 :: number(),
    last5m_smpl = [] :: list()
}).

-record(state, {
    metric_ids = sets:new(),
    rates :: undefined | #{metric_id() => #rate{}}
}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec child_spec(handler_name()) -> supervisor:child_spec().
child_spec(Name) ->
    child_spec(emqx_metrics_worker, Name).

child_spec(ChldName, Name) ->
    #{
        id => ChldName,
        start => {emqx_metrics_worker, start_link, [Name]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_metrics_worker]
    }.

-spec create_metrics(handler_name(), metric_id(), [atom()]) -> ok | {error, term()}.
create_metrics(Name, Id, Metrics) ->
    create_metrics(Name, Id, Metrics, Metrics).

-spec create_metrics(handler_name(), metric_id(), [atom()], [atom()]) -> ok | {error, term()}.
create_metrics(Name, Id, Metrics, RateMetrics) ->
    gen_server:call(Name, {create_metrics, Id, Metrics, RateMetrics}).

-spec clear_metrics(handler_name(), metric_id()) -> ok.
clear_metrics(Name, Id) ->
    gen_server:call(Name, {delete_metrics, Id}).

-spec reset_metrics(handler_name(), metric_id()) -> ok.
reset_metrics(Name, Id) ->
    gen_server:call(Name, {reset_metrics, Id}).

-spec has_metrics(handler_name(), metric_id()) -> boolean().
has_metrics(Name, Id) ->
    case get_ref(Name, Id) of
        not_found -> false;
        _ -> true
    end.

-spec get(handler_name(), metric_id(), atom() | integer()) -> number().
get(Name, Id, Metric) ->
    case get_ref(Name, Id) of
        not_found ->
            0;
        Ref when is_atom(Metric) ->
            counters:get(Ref, idx_metric(Name, Id, Metric));
        Ref when is_integer(Metric) ->
            counters:get(Ref, Metric)
    end.

-spec get_rate(handler_name(), metric_id()) -> map().
get_rate(Name, Id) ->
    gen_server:call(Name, {get_rate, Id}).

-spec get_counters(handler_name(), metric_id()) -> map().
get_counters(Name, Id) ->
    maps:map(
        fun(_Metric, Index) ->
            get(Name, Id, Index)
        end,
        get_indexes(Name, Id)
    ).

-spec reset_counters(handler_name(), metric_id()) -> ok.
reset_counters(Name, Id) ->
    Indexes = maps:values(get_indexes(Name, Id)),
    Ref = get_ref(Name, Id),
    lists:foreach(fun(Idx) -> counters:put(Ref, Idx, 0) end, Indexes).

-spec get_metrics(handler_name(), metric_id()) -> metrics().
get_metrics(Name, Id) ->
    #{rate => get_rate(Name, Id), counters => get_counters(Name, Id)}.

-spec inc(handler_name(), metric_id(), atom()) -> ok.
inc(Name, Id, Metric) ->
    inc(Name, Id, Metric, 1).

-spec inc(handler_name(), metric_id(), atom(), pos_integer()) -> ok.
inc(Name, Id, Metric, Val) ->
    counters:add(get_ref(Name, Id), idx_metric(Name, Id, Metric), Val).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, Name, []).

init(Name) ->
    erlang:process_flag(trap_exit, true),
    %% the rate metrics
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    persistent_term:put(?CntrRef(Name), #{}),
    {ok, #state{}}.

handle_call({get_rate, _Id}, _From, State = #state{rates = undefined}) ->
    {reply, make_rate(0, 0, 0), State};
handle_call({get_rate, Id}, _From, State = #state{rates = Rates}) ->
    {reply,
        case maps:get(Id, Rates, undefined) of
            undefined -> make_rate(0, 0, 0);
            RatesPerId -> format_rates_of_id(RatesPerId)
        end, State};
handle_call(
    {create_metrics, Id, Metrics, RateMetrics},
    _From,
    State = #state{metric_ids = MIDs, rates = Rates}
) ->
    case RateMetrics -- Metrics of
        [] ->
            RatePerId = maps:from_list([{M, #rate{}} || M <- RateMetrics]),
            Rate1 =
                case Rates of
                    undefined -> #{Id => RatePerId};
                    _ -> Rates#{Id => RatePerId}
                end,
            {reply, create_counters(get_self_name(), Id, Metrics), State#state{
                metric_ids = sets:add_element(Id, MIDs),
                rates = Rate1
            }};
        _ ->
            {reply, {error, not_super_set_of, {RateMetrics, Metrics}}, State}
    end;
handle_call(
    {delete_metrics, Id},
    _From,
    State = #state{metric_ids = MIDs, rates = Rates}
) ->
    {reply, delete_counters(get_self_name(), Id), State#state{
        metric_ids = sets:del_element(Id, MIDs),
        rates =
            case Rates of
                undefined -> undefined;
                _ -> maps:remove(Id, Rates)
            end
    }};
handle_call(
    {reset_metrics, Id},
    _From,
    State = #state{rates = Rates}
) ->
    {reply, reset_counters(get_self_name(), Id), State#state{
        rates =
            case Rates of
                undefined ->
                    undefined;
                _ ->
                    ResetRate =
                        maps:map(
                            fun(_Key, _Value) -> #rate{} end,
                            maps:get(Id, Rates, #{})
                        ),
                    maps:put(Id, ResetRate, Rates)
            end
    }};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ticking, State = #state{rates = undefined}) ->
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State};
handle_info(ticking, State = #state{rates = Rates0}) ->
    Rates =
        maps:map(
            fun(Id, RatesPerID) ->
                maps:map(
                    fun(Metric, Rate) ->
                        calculate_rate(get(get_self_name(), Id, Metric), Rate)
                    end,
                    RatesPerID
                )
            end,
            Rates0
        ),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State#state{rates = Rates}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{metric_ids = MIDs}) ->
    Name = get_self_name(),
    [delete_counters(Name, Id) || Id <- sets:to_list(MIDs)],
    persistent_term:erase(?CntrRef(Name)).

stop(Name) ->
    try
        gen_server:stop(Name)
    catch
        exit:noproc ->
            ok;
        exit:timeout ->
            %% after timeout, the process killed by gen.erl
            ok
    end.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

create_counters(_Name, _Id, []) ->
    error({create_counter_error, must_provide_a_list_of_metrics});
create_counters(Name, Id, Metrics) ->
    %% backup the old counters
    OlderCounters = maps:with(Metrics, get_counters(Name, Id)),
    %% create the new counter
    Size = length(Metrics),
    Indexes = maps:from_list(lists:zip(Metrics, lists:seq(1, Size))),
    Counters = get_pterm(Name),
    CntrRef = counters:new(Size, [write_concurrency]),
    persistent_term:put(
        ?CntrRef(Name),
        Counters#{Id => #{ref => CntrRef, indexes => Indexes}}
    ),
    %% restore the old counters
    lists:foreach(
        fun({Metric, N}) ->
            inc(Name, Id, Metric, N)
        end,
        maps:to_list(OlderCounters)
    ).

delete_counters(Name, Id) ->
    persistent_term:put(?CntrRef(Name), maps:remove(Id, get_pterm(Name))).

get_ref(Name, Id) ->
    case maps:find(Id, get_pterm(Name)) of
        {ok, #{ref := Ref}} -> Ref;
        error -> not_found
    end.

idx_metric(Name, Id, Metric) ->
    maps:get(Metric, get_indexes(Name, Id)).

get_indexes(Name, Id) ->
    case maps:find(Id, get_pterm(Name)) of
        {ok, #{indexes := Indexes}} -> Indexes;
        error -> #{}
    end.

get_pterm(Name) ->
    persistent_term:get(?CntrRef(Name), #{}).

calculate_rate(_CurrVal, undefined) ->
    undefined;
calculate_rate(CurrVal, #rate{
    max = MaxRate0,
    last_v = LastVal,
    tick = Tick,
    last5m_acc = AccRate5Min0,
    last5m_smpl = Last5MinSamples0
}) ->
    %% calculate the current rate based on the last value of the counter
    CurrRate = (CurrVal - LastVal) / ?SAMPLING,

    %% calculate the max rate since the emqx startup
    MaxRate =
        case MaxRate0 >= CurrRate of
            true -> MaxRate0;
            false -> CurrRate
        end,

    %% calculate the average rate in last 5 mins
    {Last5MinSamples, Acc5Min, Last5Min} =
        case Tick =< ?SAMPCOUNT_5M of
            true ->
                Acc = AccRate5Min0 + CurrRate,
                {lists:reverse([CurrRate | lists:reverse(Last5MinSamples0)]), Acc, Acc / Tick};
            false ->
                [FirstRate | Rates] = Last5MinSamples0,
                Acc = AccRate5Min0 + CurrRate - FirstRate,
                {lists:reverse([CurrRate | lists:reverse(Rates)]), Acc, Acc / ?SAMPCOUNT_5M}
        end,

    #rate{
        max = MaxRate,
        current = CurrRate,
        last5m = Last5Min,
        last_v = CurrVal,
        last5m_acc = Acc5Min,
        last5m_smpl = Last5MinSamples,
        tick = Tick + 1
    }.

format_rates_of_id(RatesPerId) ->
    maps:map(
        fun(_Metric, Rates) ->
            format_rate(Rates)
        end,
        RatesPerId
    ).

format_rate(#rate{max = Max, current = Current, last5m = Last5Min}) ->
    make_rate(Current, Max, Last5Min).

make_rate(Current, Max, Last5Min) ->
    #{
        current => precision(Current, 2),
        max => precision(Max, 2),
        last5m => precision(Last5Min, 2)
    }.

precision(Float, N) ->
    Base = math:pow(10, N),
    round(Float * Base) / Base.

get_self_name() ->
    {registered_name, Name} = process_info(self(), registered_name),
    Name.
