%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API functions
-export([ start_link/0
        , stop/0
        ]).

-export([ get_actions_taken/1
        , get_actions_success/1
        , get_actions_error/1
        , get_actions_exception/1
        , get_actions_retry/1
        , get_rules_matched/1
        , get_rules_failed/1
        , get_rules_passed/1
        , get_rules_exception/1
        , get_rules_no_result/1
        ]).

-export([ inc_rules_matched/2
        , inc_actions_taken/1
        , inc_actions_taken/2
        , inc_actions_success/1
        , inc_actions_success/2
        , inc_actions_error/1
        , inc_actions_error/2
        , inc_actions_exception/1
        , inc_actions_exception/2
        , inc_actions_retry/1
        , inc_actions_retry/2
        , inc_rules_matched/1
        , inc_rules_failed/1
        , inc_rules_passed/1
        , inc_rules_exception/1
        , inc_rules_no_result/1
        ]).

-export([ inc/2
        , inc/3
        , get/2
        , get_rule_speed/1
        , create_rule_metrics/1
        , create_metrics/1
        , clear_rule_metrics/1
        , clear_metrics/1
        , reset_metrics/1
        ]).

-export([ get_rule_metrics/1
        , get_action_metrics/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_info/2
        , handle_cast/2
        , code_change/3
        , terminate/2
        ]).

-elvis([{elvis_style, god_modules, disable}]).

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
            max = 0.0 :: float(),
            current = 0.0 :: float(),
            last5m = 0.0 :: float(),
            %% metadata for calculating the avg speed
            tick = 1 :: integer(),
            last_v = 0 :: integer(),
            %% metadata for calculating the 5min avg speed
            last5m_acc = 0.0 :: float(),
            last5m_smpl = [] :: list()
        }).

-record(state, {
            metric_ids = sets:new(),
            rule_speeds :: undefined | #{rule_id() => #rule_speed{}},
            overall_rule_speed :: #rule_speed{}
        }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
-spec(create_rule_metrics(rule_id()) -> ok).
create_rule_metrics(Id) ->
    gen_server:call(?MODULE, {create_rule_metrics, Id}).

-spec(create_metrics(rule_id()) -> ok).
create_metrics(Id) ->
    gen_server:call(?MODULE, {create_metrics, Id}).

-spec(clear_rule_metrics(rule_id()) -> ok).
clear_rule_metrics(Id) ->
    gen_server:call(?MODULE, {delete_rule_metrics, Id}).

-spec(clear_metrics(rule_id()) -> ok).
clear_metrics(Id) ->
    gen_server:call(?MODULE, {delete_metrics, Id}).

-spec(reset_metrics(rule_id()) -> ok).
reset_metrics(Id) ->
    reset_speeds(Id),
    do_reset_metrics(Id, rule_metrics()),
    case emqx_rule_registry:get_rule(Id) of
        not_found -> ok;
        {ok, #rule{actions = Actions}} ->
            reset_action_metrics(Actions),
            ok
    end.

do_reset_metrics(Id, Metrics) ->
    case couters_ref(Id) of
        not_found -> ok;
        Ref -> [counters:put(Ref, metrics_idx(Idx), 0)
                || Idx <- Metrics],
               ok
    end.

reset_action_metrics(Actions) ->
    lists:foreach(fun(#action_instance{id = ActionId, fallbacks = FallbackActions}) ->
            do_reset_metrics(ActionId, action_metrics()),
            reset_action_metrics(FallbackActions)
        end, Actions).

reset_speeds(Id) ->
    gen_server:call(?MODULE, {reset_speeds, Id}).

rule_metrics() ->
    [ 'rules.matched'
    , 'rules.failed'
    , 'rules.passed'
    , 'rules.exception'
    , 'rules.no_result'
    ].

action_metrics() ->
    [ 'actions.success'
    , 'actions.error'
    , 'actions.taken'
    , 'actions.exception'
    , 'actions.retry'
    ].

-spec(get(rule_id(), atom()) -> number()).
get(Id, Metric) ->
    case couters_ref(Id) of
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
      failed => get_rules_failed(Id),
      passed => get_rules_passed(Id),
      exception => get_rules_exception(Id),
      no_result => get_rules_no_result(Id),
      speed => Current,
      speed_max => Max,
      speed_last5m => Last5M
    }.

-spec(get_action_metrics(action_instance_id()) -> map()).
get_action_metrics(Id) ->
    #{success => get_actions_success(Id),
      failed => get_actions_error(Id) + get_actions_exception(Id),
      taken => get_actions_taken(Id)
     }.

-spec inc(rule_id(), atom()) -> ok.
inc(Id, Metric) ->
    inc(Id, Metric, 1).

-spec inc(rule_id(), atom(), pos_integer()) -> ok.
inc(Id, Metric, Val) ->
    case couters_ref(Id) of
        not_found ->
            %% this may occur when increasing a counter for
            %% a rule that was created from a remove node.
            case atom_to_list(Metric) of
                "rules." ++ _ -> create_rule_metrics(Id);
                _ -> create_metrics(Id)
            end,
            counters:add(couters_ref(Id), metrics_idx(Metric), Val);
        Ref ->
            counters:add(Ref, metrics_idx(Metric), Val)
    end,
    ?tp(rule_metrics_inc, #{id => Id, metric => Metric, value => Val}),
    ok.

inc_actions_taken(Id) ->
    inc_actions_taken(Id, 1).
inc_actions_taken(Id, Val) ->
    inc(Id, 'actions.taken', Val).

inc_actions_success(Id) ->
    inc_actions_success(Id, 1).
inc_actions_success(Id, Val) ->
    inc(Id, 'actions.success', Val).

inc_actions_error(Id) ->
    inc_actions_error(Id, 1).
inc_actions_error(Id, Val) ->
    inc(Id, 'actions.error', Val).

inc_actions_exception(Id) ->
    inc_actions_exception(Id, 1).
inc_actions_exception(Id, Val) ->
    inc(Id, 'actions.exception', Val).

inc_actions_retry(Id) ->
    inc_actions_retry(Id, 1).
inc_actions_retry(Id, Val) ->
    inc(Id, 'actions.retry', Val).

inc_rules_matched(Id) ->
    inc_rules_matched(Id, 1).
inc_rules_matched(Id, Val) ->
    inc(Id, 'rules.matched', Val).

inc_rules_failed(Id) ->
    inc_rules_failed(Id, 1).
inc_rules_failed(Id, Val) ->
    inc(Id, 'rules.failed', Val).

inc_rules_passed(Id) ->
    inc_rules_passed(Id, 1).
inc_rules_passed(Id, Val) ->
    inc(Id, 'rules.passed', Val).

inc_rules_exception(Id) ->
    inc_rules_exception(Id, 1).
inc_rules_exception(Id, Val) ->
    inc(Id, 'rules.failed', Val),
    inc(Id, 'rules.exception', Val).

inc_rules_no_result(Id) ->
    inc_rules_no_result(Id, 1).
inc_rules_no_result(Id, Val) ->
    inc(Id, 'rules.failed', Val),
    inc(Id, 'rules.no_result', Val).

get_actions_taken(Id) ->
    get(Id, 'actions.taken').

get_actions_success(Id) ->
    get(Id, 'actions.success').

get_actions_error(Id) ->
    get(Id, 'actions.error').

get_actions_exception(Id) ->
    get(Id, 'actions.exception').

get_actions_retry(Id) ->
    get(Id, 'actions.retry').

get_rules_matched(Id) ->
    get(Id, 'rules.matched').

get_rules_failed(Id) ->
    get(Id, 'rules.failed').

get_rules_passed(Id) ->
    get(Id, 'rules.passed').

get_rules_exception(Id) ->
    get(Id, 'rules.exception').

get_rules_no_result(Id) ->
    get(Id, 'rules.no_result').

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
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

handle_call({reset_speeds, _Id}, _From, State = #state{rule_speeds = undefined}) ->
    {reply, ok, State};
handle_call({reset_speeds, Id}, _From, State = #state{rule_speeds = RuleSpeedMap}) ->
    {reply, ok, State#state{rule_speeds = maps:put(Id, #rule_speed{}, RuleSpeedMap)}};

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

handle_info(ticking, State = #state{rule_speeds = RuleSpeeds0}) ->
    RuleSpeeds = maps:map(
                    fun(Id, RuleSpeed) ->
                        calculate_speed(get_rules_matched(Id), RuleSpeed)
                    end, RuleSpeeds0),
    async_refresh_resource_status(),
    erlang:send_after(timer:seconds(?SAMPLING), self(), ticking),
    {noreply, State#state{rule_speeds = RuleSpeeds}};

handle_info(_Info, State) ->
    {noreply, State}.

code_change({down, _Vsn}, State = #state{metric_ids = MIDs}, [Vsn]) ->
    case string:tokens(Vsn, ".") of
        ["4", "4", SVal] ->
            {Val, []} = string:to_integer(SVal),
            case Val == 0 of
                true ->
                    [begin
                        Passed = get_rules_passed(Id),
                        Take = get_actions_taken(Id),
                        Success = get_actions_success(Id),
                        Error = get_actions_error(Id),
                        Exception = get_actions_exception(Id),
                        Retry = get_actions_retry(Id),
                        ok = delete_counters(Id),
                        ok = create_counters(Id, max_counters_size_old()),
                        inc_rules_matched(Id, Passed),
                        inc_actions_taken(Id, Take),
                        inc_actions_success(Id, Success),
                        inc_actions_error(Id, Error),
                        inc_actions_exception(Id, Exception),
                        inc_actions_retry(Id, Retry)
                    end || Id <- sets:to_list(MIDs)],
                    {ok, State};
                false -> {ok, State}
            end;
        _ -> {ok, State}
    end;

code_change(_Vsn, State = #state{metric_ids = MIDs}, [Vsn]) ->
    case string:tokens(Vsn, ".") of
        ["4", "4", SVal] ->
            {Val, []} = string:to_integer(SVal),
            case Val == 0 of
                true ->
                    [begin
                        Matched = get_rules_matched(Id),
                        Take = get_actions_taken(Id),
                        Success = get_actions_success(Id),
                        Error = get_actions_error(Id),
                        Exception = get_actions_exception(Id),
                        Retry = get_actions_retry(Id),
                        ok = delete_counters(Id),
                        ok = create_counters(Id),
                        inc_rules_matched(Id, Matched),
                        inc_rules_passed(Id, Matched),
                        inc_actions_taken(Id, Take),
                        inc_actions_success(Id, Success),
                        inc_actions_error(Id, Error),
                        inc_actions_exception(Id, Exception),
                        inc_actions_retry(Id, Retry)
                    end || Id <- sets:to_list(MIDs)],
                    {ok, State};
                false -> {ok, State}
            end;
        _ -> {ok, State}
    end;

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
    create_counters(Id, max_counters_size()).
create_counters(Id, Size) ->
    case couters_ref(Id) of
        not_found ->
            ok = persistent_term:put(?CRefID(Id),
                    counters:new(Size, [write_concurrency]));
        _Ref -> ok
    end.

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
        case MaxSpeed0 >= CurrSpeed of
           true -> MaxSpeed0;
           false -> CurrSpeed
        end,

    %% calculate the average speed in last 5 mins
    {Last5MinSamples, Acc5Min, Last5Min} =
        case Tick =< ?SAMPCOUNT_5M of
            true ->
                Acc = AccSpeed5Min0 + CurrSpeed,
                {lists:reverse([CurrSpeed | lists:reverse(Last5MinSamples0)]),
                 Acc, Acc / Tick};
            false ->
                [FirstSpeed | Speeds] = Last5MinSamples0,
                Acc =  AccSpeed5Min0 + CurrSpeed - FirstSpeed,
                {lists:reverse([CurrSpeed | lists:reverse(Speeds)]),
                 Acc, Acc / ?SAMPCOUNT_5M}
        end,

    #rule_speed{max = MaxSpeed, current = CurrSpeed, last5m = Last5Min,
                last_v = CurrVal, last5m_acc = Acc5Min,
                last5m_smpl = Last5MinSamples, tick = Tick + 1}.

format_rule_speed(#rule_speed{max = Max, current = Current, last5m = Last5Min}) ->
    #{max => round2(Max), current => round2(Current), last5m => round2(Last5Min)}.

round2(Float) ->
    round(Float * 100) / 100.

%%------------------------------------------------------------------------------
%% Metrics Definitions
%%------------------------------------------------------------------------------

%% for code hot upgrade
max_counters_size_old() -> 7.

max_counters_size() -> 11.

metrics_idx('rules.matched') ->       1;
metrics_idx('actions.success') ->     2;
metrics_idx('actions.error') ->       3;
metrics_idx('actions.taken') ->       4;
metrics_idx('actions.exception') ->   5;
metrics_idx('actions.retry') ->       6;
metrics_idx('rules.failed') ->        7;
metrics_idx('rules.passed') ->        8;
metrics_idx('rules.exception') ->     9;
metrics_idx('rules.no_result') ->     10;
metrics_idx(_) ->                     11.
