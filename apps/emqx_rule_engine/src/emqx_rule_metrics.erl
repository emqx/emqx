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

-module(emqx_rule_metrics).

-behaviour(gen_server).

-include("rule_engine.hrl").

%% API functions
-export([ start_link/0
        , stop/0
        ]).

-export([ get_rules_matched/1
        ]).

-export([ inc/2
        , inc/3
        , get/2
        , get_rule_speed/1
        , create_rule_metrics/1
        , clear_rule_metrics/1
        ]).

-export([ get_rule_metrics/1
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

-define(CntrRef, ?MODULE).
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
            rule_speeds :: undefined | #{rule_id() => #rule_speed{}}
        }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec(create_rule_metrics(rule_id()) -> ok).
create_rule_metrics(Id) ->
    gen_server:call(?MODULE, {create_rule_metrics, Id}).

-spec(clear_rule_metrics(rule_id()) -> ok).
clear_rule_metrics(Id) ->
    gen_server:call(?MODULE, {delete_rule_metrics, Id}).

-spec(get(rule_id(), atom()) -> number()).
get(Id, Metric) ->
    case get_couters_ref(Id) of
        not_found -> 0;
        Ref -> counters:get(Ref, metrics_idx(Metric))
    end.

-spec(get_rule_speed(rule_id()) -> map()).
get_rule_speed(Id) ->
    gen_server:call(?MODULE, {get_rule_speed, Id}).

-spec(get_rule_metrics(rule_id()) -> map()).
get_rule_metrics(Id) ->
    #{max := Max, current := Current, last5m := Last5M} = get_rule_speed(Id),
    #{matched => get_rules_matched(Id),
      speed => Current,
      speed_max => Max,
      speed_last5m => Last5M
    }.

-spec inc(rule_id(), atom()) -> ok.
inc(Id, Metric) ->
    inc(Id, Metric, 1).

-spec inc(rule_id(), atom(), pos_integer()) -> ok.
inc(Id, Metric, Val) ->
    case get_couters_ref(Id) of
        not_found ->
            %% this may occur when increasing a counter for
            %% a rule that was created from a remove node.
            create_rule_metrics(Id),
            counters:add(get_couters_ref(Id), metrics_idx(Metric), Val);
        Ref ->
            counters:add(Ref, metrics_idx(Metric), Val)
    end.

get_rules_matched(Id) ->
    get(Id, 'rules.matched').

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    %% the speed metrics
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    persistent_term:put(?CntrRef, #{}),
    {ok, #state{}}.

handle_call({get_rule_speed, _Id}, _From, State = #state{rule_speeds = undefined}) ->
    {reply, format_rule_speed(#rule_speed{}), State};
handle_call({get_rule_speed, Id}, _From, State = #state{rule_speeds = RuleSpeeds}) ->
    {reply, case maps:get(Id, RuleSpeeds, undefined) of
                undefined -> format_rule_speed(#rule_speed{});
                Speed -> format_rule_speed(Speed)
            end, State};

handle_call({create_rule_metrics, Id}, _From,
            State = #state{metric_ids = MIDs, rule_speeds = RuleSpeeds}) ->
    {reply, create_counters(Id),
     State#state{metric_ids = sets:add_element(Id, MIDs),
                 rule_speeds =  case RuleSpeeds of
                                    undefined -> #{Id => #rule_speed{}};
                                    _ -> RuleSpeeds#{Id => #rule_speed{}}
                                end}};

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
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State};

handle_info(ticking, State = #state{rule_speeds = RuleSpeeds0}) ->
    RuleSpeeds = maps:map(
                    fun(Id, RuleSpeed) ->
                        calculate_speed(get_rules_matched(Id), RuleSpeed)
                    end, RuleSpeeds0),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State#state{rule_speeds = RuleSpeeds}};

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{metric_ids = MIDs}) ->
    [delete_counters(Id) || Id <- sets:to_list(MIDs)],
    persistent_term:erase(?CntrRef).

stop() ->
    gen_server:stop(?MODULE).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

create_counters(Id) ->
    case get_couters_ref(Id) of
        not_found ->
            Counters = get_all_counters(),
            CntrRef = counters:new(max_counters_size(), [write_concurrency]),
            persistent_term:put(?CntrRef, Counters#{Id => CntrRef});
        _Ref -> ok
    end.

delete_counters(Id) ->
    persistent_term:put(?CntrRef, maps:remove(Id, get_all_counters())).

get_couters_ref(Id) ->
    maps:get(Id, get_all_counters(), not_found).

get_all_counters() ->
    persistent_term:get(?CntrRef, #{}).

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

max_counters_size() -> 2.
metrics_idx('rules.matched') ->       1;
metrics_idx(_) ->                     2.

