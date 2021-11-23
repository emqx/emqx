%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_libs_metrics).

-behaviour(gen_server).

%% API functions
-export([ start_link/1
        , stop/1
        , child_spec/1
        ]).

-export([ inc/3
        , inc/4
        , get/3
        , get_speed/2
        , create_metrics/2
        , clear_metrics/2
        ]).

-export([ get_metrics/2
        , get_matched/2
        , get_success/2
        , get_failed/2
        , inc_matched/2
        , inc_success/2
        , inc_failed/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_info/2
        , handle_cast/2
        , code_change/3
        , terminate/2
        ]).

-ifndef(TEST).
-define(SECS_5M, 300).
-define(SAMPLING, 10).
-else.
%% Use 5 secs average speed instead of 5 mins in case of testing
-define(SECS_5M, 5).
-define(SAMPLING, 1).
-endif.

-export_type([metrics/0]).

-type metrics() :: #{
    matched => integer(),
    success => integer(),
    failed => integer(),
    speed => float(),
    speed_max => float(),
    speed_last5m => float()
}.
-type handler_name() :: atom().
-type metric_id() :: binary().

-define(CntrRef(Name), {?MODULE, Name}).
-define(SAMPCOUNT_5M, (?SECS_5M div ?SAMPLING)).

%% the speed of 'matched'
-record(speed, {
            max = 0 :: number(),
            current = 0 :: number(),
            last5m = 0 :: number(),
            %% metadata for calculating the avg speed
            tick = 1 :: number(),
            last_v = 0 :: number(),
            %% metadata for calculating the 5min avg speed
            last5m_acc = 0 :: number(),
            last5m_smpl = [] :: list()
        }).

-record(state, {
            metric_ids = sets:new(),
            speeds :: undefined | #{metric_id() => #speed{}}
        }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec(child_spec(handler_name()) -> supervisor:child_spec()).
child_spec(Name) ->
    #{ id => emqx_plugin_libs_metrics
     , start => {emqx_plugin_libs_metrics, start_link, [Name]}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     , modules => [emqx_plugin_libs_metrics]
     }.

-spec(create_metrics(handler_name(), metric_id()) -> ok).
create_metrics(Name, Id) ->
    gen_server:call(Name, {create_metrics, Id}).

-spec(clear_metrics(handler_name(), metric_id()) -> ok).
clear_metrics(Name, Id) ->
    gen_server:call(Name, {delete_metrics, Id}).

-spec(get(handler_name(), metric_id(), atom()) -> number()).
get(Name, Id, Metric) ->
    case get_couters_ref(Name, Id) of
        not_found -> 0;
        Ref -> counters:get(Ref, metrics_idx(Metric))
    end.

-spec(get_speed(handler_name(), metric_id()) -> map()).
get_speed(Name, Id) ->
    gen_server:call(Name, {get_speed, Id}).

-spec(get_metrics(handler_name(), metric_id()) -> metrics()).
get_metrics(Name, Id) ->
    #{max := Max, current := Current, last5m := Last5M} = get_speed(Name, Id),
    #{matched => get_matched(Name, Id),
      success => get_success(Name, Id),
      failed => get_failed(Name, Id),
      speed => Current,
      speed_max => Max,
      speed_last5m => Last5M
    }.

-spec inc(handler_name(), metric_id(), atom()) -> ok.
inc(Name, Id, Metric) ->
    inc(Name, Id, Metric, 1).

-spec inc(handler_name(), metric_id(), atom(), pos_integer()) -> ok.
inc(Name, Id, Metric, Val) ->
    case get_couters_ref(Name, Id) of
        not_found ->
            %% this may occur when increasing a counter for
            %% a rule that was created from a remove node.
            create_metrics(Name, Id),
            counters:add(get_couters_ref(Name, Id), metrics_idx(Metric), Val);
        Ref ->
            counters:add(Ref, metrics_idx(Metric), Val)
    end.

inc_matched(Name, Id) ->
    inc(Name, Id, 'matched', 1).

inc_success(Name, Id) ->
    inc(Name, Id, 'success', 1).

inc_failed(Name, Id) ->
    inc(Name, Id, 'failed', 1).

get_matched(Name, Id) ->
    get(Name, Id, 'matched').

get_success(Name, Id) ->
    get(Name, Id, 'success').

get_failed(Name, Id) ->
    get(Name, Id, 'failed').

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, Name, []).

init(Name) ->
    erlang:process_flag(trap_exit, true),
    %% the speed metrics
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    persistent_term:put(?CntrRef(Name), #{}),
    {ok, #state{}}.

handle_call({get_speed, _Id}, _From, State = #state{speeds = undefined}) ->
    {reply, format_speed(#speed{}), State};
handle_call({get_speed, Id}, _From, State = #state{speeds = Speeds}) ->
    {reply, case maps:get(Id, Speeds, undefined) of
                undefined -> format_speed(#speed{});
                Speed -> format_speed(Speed)
            end, State};

handle_call({create_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, speeds = Speeds}) ->
    {reply, create_counters(get_self_name(), Id),
     State#state{metric_ids = sets:add_element(Id, MIDs),
                 speeds =  case Speeds of
                                    undefined -> #{Id => #speed{}};
                                    _ -> Speeds#{Id => #speed{}}
                                end}};

handle_call({delete_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, speeds = Speeds}) ->
    {reply, delete_counters(get_self_name(), Id),
     State#state{metric_ids = sets:del_element(Id, MIDs),
                 speeds =  case Speeds of
                                    undefined -> undefined;
                                    _ -> maps:remove(Id, Speeds)
                                end}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ticking, State = #state{speeds = undefined}) ->
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State};

handle_info(ticking, State = #state{speeds = Speeds0}) ->
    Speeds = maps:map(
                    fun(Id, Speed) ->
                        calculate_speed(get_matched(get_self_name(), Id), Speed)
                    end, Speeds0),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State#state{speeds = Speeds}};

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{metric_ids = MIDs}) ->
    Name = get_self_name(),
    [delete_counters(Name, Id) || Id <- sets:to_list(MIDs)],
    persistent_term:erase(?CntrRef(Name)).

stop(Name) ->
    gen_server:stop(Name).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

create_counters(Name, Id) ->
    case get_couters_ref(Name, Id) of
        not_found ->
            Counters = get_all_counters(Name),
            CntrRef = counters:new(max_counters_size(), [write_concurrency]),
            persistent_term:put(?CntrRef(Name), Counters#{Id => CntrRef});
        _Ref -> ok
    end.

delete_counters(Name, Id) ->
    persistent_term:put(?CntrRef(Name), maps:remove(Id, get_all_counters(Name))).

get_couters_ref(Name, Id) ->
    maps:get(Id, get_all_counters(Name), not_found).

get_all_counters(Name) ->
    persistent_term:get(?CntrRef(Name), #{}).

calculate_speed(_CurrVal, undefined) ->
    undefined;
calculate_speed(CurrVal, #speed{max = MaxSpeed0, last_v = LastVal,
                                     tick = Tick, last5m_acc = AccSpeed5Min0,
                                     last5m_smpl = Last5MinSamples0}) ->
    %% calculate the current speed based on the last value of the counter
    CurrSpeed = (CurrVal - LastVal) / ?SAMPLING,

    %% calculate the max speed since the emqx startup
    MaxSpeed =
        if MaxSpeed0 >= CurrSpeed -> MaxSpeed0;
           true -> CurrSpeed
        end,

    %% calculate the average speed in last 5 mins
    {Last5MinSamples, Acc5Min, Last5Min} =
        if Tick =< ?SAMPCOUNT_5M ->
                Acc = AccSpeed5Min0 + CurrSpeed,
                {lists:reverse([CurrSpeed | lists:reverse(Last5MinSamples0)]),
                 Acc, Acc / Tick};
           true ->
                [FirstSpeed | Speeds] = Last5MinSamples0,
                Acc =  AccSpeed5Min0 + CurrSpeed - FirstSpeed,
                {lists:reverse([CurrSpeed | lists:reverse(Speeds)]),
                 Acc, Acc / ?SAMPCOUNT_5M}
        end,

    #speed{max = MaxSpeed, current = CurrSpeed, last5m = Last5Min,
                last_v = CurrVal, last5m_acc = Acc5Min,
                last5m_smpl = Last5MinSamples, tick = Tick + 1}.

format_speed(#speed{max = Max, current = Current, last5m = Last5Min}) ->
    #{max => Max, current => precision(Current, 2), last5m => precision(Last5Min, 2)}.

precision(Float, N) ->
    Base = math:pow(10, N),
    round(Float * Base) / Base.

get_self_name() ->
    {registered_name, Name} = process_info(self(), registered_name),
    Name.

%%------------------------------------------------------------------------------
%% Metrics Definitions
%%------------------------------------------------------------------------------

max_counters_size() -> 32.
metrics_idx('matched') -> 1;
metrics_idx('success') -> 2;
metrics_idx('failed') -> 3;
metrics_idx(_) -> 32.

