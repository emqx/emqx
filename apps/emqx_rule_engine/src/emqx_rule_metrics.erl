%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_metrics).

-behaviour(gen_server).

-include("rule_engine.hrl").

%% API functions
-export([ start_link/0
        , stop/0
        ]).

-export([ inc/2
        , inc/3
        , get/2
        , get_overall/1
        , get_rule_speed/1
        , get_overall_rule_speed/0
        , create/1
        , clear/1
        , overall_metrics/0
        ]).

-export([ get_rule_metrics/1
        , get_action_metrics/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_info/2
        , handle_cast/2
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

-define(CRefID(ID), {?MODULE, ID}).
-define(SAMPCOUNT_5M, (?SECS_5M div ?SAMPLING)).

-record(rule_speed, {
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
            rule_speeds :: #{rule_id() => #rule_speed{}},
            overall_rule_speed :: #rule_speed{}
        }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
-spec(create(rule_id()) -> Ref :: reference()).
create(<<"rule:", _/binary>> = Id) ->
    gen_server:call(?MODULE, {create_rule_metrics, Id});
create(Id) ->
    gen_server:call(?MODULE, {create_metrics, Id}).

-spec(clear(rule_id()) -> ok).
clear(<<"rule:", _/binary>> = Id) ->
    gen_server:call(?MODULE, {delete_rule_metrics, Id});
clear(Id) ->
    gen_server:call(?MODULE, {delete_metrics, Id}).

-spec(get(rule_id(), atom()) -> number()).
get(Id, Metric) ->
    case couters_ref(Id) of
        not_found -> 0;
        Ref -> counters:get(Ref, metrics_idx(Metric))
    end.

-spec(get_overall(atom()) -> number()).
get_overall(Metric) ->
    emqx_metrics:val(Metric).

-spec(get_rule_speed(atom()) -> map()).
get_rule_speed(Id) ->
    gen_server:call(?MODULE, {get_rule_speed, Id}).

-spec(get_overall_rule_speed() -> map()).
get_overall_rule_speed() ->
    gen_server:call(?MODULE, get_overall_rule_speed).

-spec(get_rule_metrics(rule_id()) -> map()).
get_rule_metrics(Id) ->
    #{max := Max, current := Current, last5m := Last5M} = get_rule_speed(Id),
    #{matched => get(Id, 'rules.matched'),
      speed => Current,
      speed_max => Max,
      speed_last5m => Last5M
    }.

-spec(get_action_metrics(action_instance_id()) -> map()).
get_action_metrics(Id) ->
    #{success => get(Id, 'actions.success'),
      failed => get(Id, 'actions.failure')
     }.

-spec(inc(rule_id(), atom()) -> ok).
inc(Id, Metric) ->
    inc(Id, Metric, 1).
inc(Id, Metric, Val) ->
    case couters_ref(Id) of
        not_found ->
            counters:add(create(Id), metrics_idx(Metric), Val);
        Ref ->
            counters:add(Ref, metrics_idx(Metric), Val)
    end,
    inc_overall(Metric, Val).

-spec(inc_overall(rule_id(), atom()) -> ok).
inc_overall(Metric, Val) ->
    emqx_metrics:inc(Metric, Val).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    %% the overall counters
    [ok = emqx_metrics:ensure(Metric)|| Metric <- overall_metrics()],
    %% the speed metrics
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {ok, #state{overall_rule_speed = #rule_speed{}}}.

handle_call({get_rule_speed, _Id}, _From, State = #state{rule_speeds = undefined}) ->
    {reply, format_rule_speed(#rule_speed{}), State};
handle_call({get_rule_speed, Id}, _From, State = #state{rule_speeds = RuleSpeeds}) ->
    {reply, case maps:get(Id, RuleSpeeds, undefined) of
                undefined -> format_rule_speed(#rule_speed{});
                Speed -> format_rule_speed(Speed)
            end, State};

handle_call(get_overall_rule_speed, _From, State = #state{overall_rule_speed = RuleSpeed}) ->
    {reply, format_rule_speed(RuleSpeed), State};

handle_call({create_metrics, Id}, _From, State = #state{metric_ids = MIDs}) ->
    {reply, create_counters(Id), State#state{metric_ids = sets:add_element(Id, MIDs)}};

handle_call({create_rule_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, rule_speeds = RuleSpeeds}) ->
    {reply, create_counters(Id),
     State#state{metric_ids = sets:add_element(Id, MIDs),
                 rule_speeds =  case RuleSpeeds of
                                    undefined -> #{Id => #rule_speed{}};
                                    _ -> RuleSpeeds#{Id => #rule_speed{}}
                                end}};

handle_call({delete_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, rule_speeds = undefined}) ->
    {reply, delete_counters(Id), State#state{metric_ids = sets:del_element(Id, MIDs)}};

handle_call({delete_rule_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, rule_speeds = RuleSpeeds}) ->
    {reply, delete_counters(Id),
     State#state{metric_ids = sets:del_element(Id, MIDs),
                 rule_speeds =  case RuleSpeeds of
                                    undefined -> undefined;
                                    _ -> maps:remove(Id, RuleSpeeds)
                                end}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ticking, State = #state{rule_speeds = undefined}) ->
    async_refresh_resource_status(),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State};

handle_info(ticking, State = #state{rule_speeds = RuleSpeeds0,
                                       overall_rule_speed = OverallRuleSpeed0}) ->
    RuleSpeeds = maps:map(
                    fun(Id, RuleSpeed) ->
                        calculate_speed(get(Id, 'rules.matched'), RuleSpeed)
                    end, RuleSpeeds0),
    OverallRuleSpeed = calculate_speed(get_overall('rules.matched'), OverallRuleSpeed0),
    async_refresh_resource_status(),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State#state{rule_speeds = RuleSpeeds,
                          overall_rule_speed = OverallRuleSpeed}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{metric_ids = MIDs}) ->
    [delete_counters(Id) || Id <- sets:to_list(MIDs)],
    persistent_term:erase(?MODULE).

stop() ->
    gen_server:stop(?MODULE).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

async_refresh_resource_status() ->
    spawn(emqx_rule_engine, refresh_resource_status, []).

create_counters(Id) ->
    CRef = counters:new(max_counters_size(), [write_concurrency]),
    ok = persistent_term:put(?CRefID(Id), CRef),
    CRef.

delete_counters(Id) ->
    persistent_term:erase(?CRefID(Id)),
    ok.

couters_ref(Id) ->
    try persistent_term:get(?CRefID(Id))
    catch
        error:badarg -> not_found
    end.

calculate_speed(_CurrVal, undefined) ->
    undefined;
calculate_speed(CurrVal, #rule_speed{max = MaxSpeed0, last_v = LastVal,
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

    #rule_speed{max = MaxSpeed, current = CurrSpeed, last5m = Last5Min,
                last_v = CurrVal, last5m_acc = Acc5Min,
                last5m_smpl = Last5MinSamples, tick = Tick + 1}.

format_rule_speed(#rule_speed{max = Max, current = Current, last5m = Last5Min}) ->
    #{max => Max, current => precision(Current, 2), last5m => precision(Last5Min, 2)}.

precision(Float, N) ->
    Base = math:pow(10, N),
    round(Float * Base) / Base.

%%------------------------------------------------------------------------------
%% Metrics Definitions
%%------------------------------------------------------------------------------

max_counters_size() -> 4.

metrics_idx('rules.matched') ->      1;
metrics_idx('actions.success') ->   2;
metrics_idx('actions.failure') ->   3;
metrics_idx(_) ->                   4.

overall_metrics() ->
    ['rules.matched', 'actions.success', 'actions.failure'].
