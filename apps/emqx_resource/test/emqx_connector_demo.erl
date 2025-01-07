%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_demo).

-include_lib("typerefl/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_get_status/2,

    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([counter_loop/0, set_callback_mode/1]).

%% callbacks for emqx_resource config schema
-export([roots/0]).

-define(CM_KEY, {?MODULE, callback_mode}).
-define(PT_CHAN_KEY(CONN_RES_ID), {?MODULE, chans, CONN_RES_ID}).

roots() ->
    [
        {name, fun name/1},
        {register, fun register/1}
    ].

name(type) -> atom();
name(required) -> true;
name(_) -> undefined.

register(type) -> boolean();
register(required) -> true;
register(default) -> false;
register(_) -> undefined.

resource_type() -> demo.

callback_mode() ->
    persistent_term:get(?CM_KEY).

set_callback_mode(Mode) ->
    persistent_term:put(?CM_KEY, Mode).

on_start(_InstId, #{create_error := true}) ->
    ?tp(connector_demo_start_error, #{}),
    error("some error");
on_start(InstId, #{create_error := {delay, Delay, Agent}} = State0) ->
    ?tp(connector_demo_start_delay, #{}),
    State = maps:remove(create_error, State0),
    case emqx_utils_agent:get_and_update(Agent, fun(St) -> {St, called} end) of
        not_called ->
            emqx_resource:allocate_resource(InstId, i_should_be_deallocated, yep),
            timer:sleep(Delay),
            on_start(InstId, State);
        called ->
            on_start(InstId, State)
    end;
on_start(InstId, #{name := Name} = Opts) ->
    Register = maps:get(register, Opts, false),
    StopError = maps:get(stop_error, Opts, false),
    {ok, Opts#{
        id => InstId,
        stop_error => StopError,
        channels => #{},
        pid => spawn_counter_process(Name, Register)
    }}.

on_stop(_InstId, undefined) ->
    ?tp(connector_demo_free_resources_without_state, #{}),
    ok;
on_stop(_InstId, #{stop_error := true}) ->
    {error, stop_error};
on_stop(InstId, #{stop_error := {ask, HowToStop}} = State) ->
    case HowToStop() of
        continue ->
            on_stop(InstId, maps:remove(stop_error, State))
    end;
on_stop(InstId, #{pid := Pid}) ->
    persistent_term:erase(?PT_CHAN_KEY(InstId)),
    stop_counter_process(Pid).

on_query(_InstId, get_state, State) ->
    {ok, State};
on_query(_InstId, get_state_failed, State) ->
    {error, State};
on_query(_InstId, block, #{pid := Pid}) ->
    Pid ! block,
    ok;
on_query(_InstId, block_now, #{pid := Pid}) ->
    Pid ! block,
    {error, {resource_error, #{reason => blocked, msg => blocked}}};
on_query(_InstId, resume, #{pid := Pid}) ->
    Pid ! resume,
    ok;
on_query(_InstId, {big_payload, Payload}, #{pid := Pid}) ->
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, {big_payload, Payload}},
    receive
        {ReqRef, ok} ->
            ?tp(connector_demo_big_payload, #{payload => Payload}),
            ok;
        {ReqRef, incorrect_status} ->
            {error, {recoverable_error, incorrect_status}}
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, {inc_counter, N}, #{pid := Pid}) ->
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, {inc, N}},
    receive
        {ReqRef, ok} ->
            ?tp(connector_demo_inc_counter, #{n => N}),
            ok;
        {ReqRef, incorrect_status} ->
            {error, {recoverable_error, incorrect_status}}
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, get_incorrect_status_count, #{pid := Pid}) ->
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, get_incorrect_status_count},
    receive
        {ReqRef, Count} -> {ok, Count}
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, get_counter, #{pid := Pid}) ->
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, get},
    receive
        {ReqRef, Num} -> {ok, Num}
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, {individual_reply, IsSuccess}, #{pid := Pid}) ->
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, {individual_reply, IsSuccess}},
    receive
        {ReqRef, Res} -> Res
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, {sleep_before_reply, For}, #{pid := Pid}) ->
    ?tp(connector_demo_sleep, #{mode => sync, for => For}),
    ReqRef = make_ref(),
    From = {self(), ReqRef},
    Pid ! {From, {sleep_before_reply, For}},
    receive
        {ReqRef, Result} ->
            Result
    after 1000 ->
        {error, timeout}
    end;
on_query(_InstId, {sync_sleep_before_reply, SleepFor}, _State) ->
    %% This simulates a slow sync call
    timer:sleep(SleepFor),
    {ok, slept}.

on_query_async(_InstId, block, ReplyFun, #{pid := Pid}) ->
    Pid ! {block, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, resume, ReplyFun, #{pid := Pid}) ->
    Pid ! {resume, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, {inc_counter, N}, ReplyFun, #{pid := Pid}) ->
    Pid ! {inc, N, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, get_counter, ReplyFun, #{pid := Pid}) ->
    Pid ! {get, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, block_now, ReplyFun, #{pid := Pid}) ->
    Pid ! {block_now, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, {big_payload, Payload}, ReplyFun, #{pid := Pid}) ->
    Pid ! {big_payload, Payload, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, {individual_reply, IsSuccess}, ReplyFun, #{pid := Pid}) ->
    Pid ! {individual_reply, IsSuccess, ReplyFun},
    {ok, Pid};
on_query_async(_InstId, {sleep_before_reply, For}, ReplyFun, #{pid := Pid}) ->
    ?tp(connector_demo_sleep, #{mode => async, for => For}),
    Pid ! {{sleep_before_reply, For}, ReplyFun},
    {ok, Pid}.

on_batch_query(InstId, BatchReq, State) ->
    %% Requests can be either 'get_counter' or 'inc_counter', but
    %% cannot be mixed.
    case hd(BatchReq) of
        {inc_counter, _} ->
            batch_inc_counter(sync, InstId, BatchReq, State);
        get_counter ->
            batch_get_counter(sync, InstId, State);
        {big_payload, _Payload} ->
            batch_big_payload(sync, InstId, BatchReq, State);
        {individual_reply, _IsSuccess} ->
            batch_individual_reply(sync, InstId, BatchReq, State);
        {random_reply, Num} ->
            %% async batch retried
            make_random_reply(Num)
    end.

on_batch_query_async(InstId, BatchReq, ReplyFunAndArgs, #{pid := Pid} = State) ->
    %% Requests can be of multiple types, but cannot be mixed.
    case hd(BatchReq) of
        {inc_counter, _} ->
            batch_inc_counter({async, ReplyFunAndArgs}, InstId, BatchReq, State);
        get_counter ->
            batch_get_counter({async, ReplyFunAndArgs}, InstId, State);
        block_now ->
            on_query_async(InstId, block_now, ReplyFunAndArgs, State);
        {big_payload, _Payload} ->
            batch_big_payload({async, ReplyFunAndArgs}, InstId, BatchReq, State);
        {individual_reply, _IsSuccess} ->
            batch_individual_reply({async, ReplyFunAndArgs}, InstId, BatchReq, State);
        {random_reply, Num} ->
            %% only take the first Num in the batch should be random enough
            Pid ! {{random_reply, Num}, ReplyFunAndArgs},
            {ok, Pid}
    end.

batch_inc_counter(CallMode, InstId, BatchReq, State) ->
    TotalN = lists:foldl(
        fun
            ({inc_counter, N}, Total) ->
                ?tp(connector_demo_batch_inc_individual, #{n => N}),
                Total + N;
            (Req, _Total) ->
                error({mixed_requests_not_allowed, {inc_counter, Req}})
        end,
        0,
        BatchReq
    ),
    case CallMode of
        sync ->
            on_query(InstId, {inc_counter, TotalN}, State);
        {async, ReplyFunAndArgs} ->
            on_query_async(InstId, {inc_counter, TotalN}, ReplyFunAndArgs, State)
    end.

batch_get_counter(sync, InstId, State) ->
    on_query(InstId, get_counter, State);
batch_get_counter({async, ReplyFunAndArgs}, InstId, State) ->
    on_query_async(InstId, get_counter, ReplyFunAndArgs, State).

batch_big_payload(sync, InstId, Batch, State) ->
    [Res | _] = lists:map(
        fun(Req = {big_payload, _}) -> on_query(InstId, Req, State) end,
        Batch
    ),
    Res;
batch_big_payload({async, ReplyFunAndArgs}, InstId, Batch, State = #{pid := Pid}) ->
    lists:foreach(
        fun(Req = {big_payload, _}) -> on_query_async(InstId, Req, ReplyFunAndArgs, State) end,
        Batch
    ),
    {ok, Pid}.

batch_individual_reply(sync, InstId, Batch, State) ->
    lists:map(
        fun(Req = {individual_reply, _}) -> on_query(InstId, Req, State) end,
        Batch
    );
batch_individual_reply({async, ReplyFunAndArgs}, InstId, Batch, State) ->
    Pid = spawn(fun() ->
        Results = lists:map(
            fun(Req = {individual_reply, _}) -> on_query(InstId, Req, State) end,
            Batch
        ),
        apply_reply(ReplyFunAndArgs, Results)
    end),
    {ok, Pid}.

on_get_status(_InstId, #{health_check_error := true}) ->
    ?tp(connector_demo_health_check_error, #{}),
    ?status_disconnected;
on_get_status(_InstId, _State = #{health_check_error := {msg, Message}}) ->
    ?tp(connector_demo_health_check_error, #{}),
    {?status_disconnected, Message};
on_get_status(_InstId, #{pid := Pid, health_check_error := {delay, Delay}}) ->
    ?tp(connector_demo_health_check_delay, #{}),
    timer:sleep(Delay),
    case is_process_alive(Pid) of
        true -> ?status_connected;
        false -> ?status_disconnected
    end;
on_get_status(ConnResId, #{health_check_agent := Agent}) ->
    case get_agent_health_check_action(Agent, resource_health_check) of
        {ask, Pid} ->
            Alias = alias([reply]),
            Pid ! {waiting_health_check_result, Alias, resource, ConnResId},
            receive
                {Alias, Result} ->
                    Result
            end;
        Result ->
            Result
    end;
on_get_status(_InstId, #{pid := Pid}) ->
    timer:sleep(300),
    case is_process_alive(Pid) of
        true -> ?status_connected;
        false -> ?status_disconnected
    end.

on_add_channel(ConnResId, ConnSt0, ChanId, ChanCfg) ->
    ConnSt = emqx_utils_maps:deep_put([channels, ChanId], ConnSt0, ChanCfg),
    do_add_channel(ConnResId, ChanId, ChanCfg),
    {ok, ConnSt}.

on_remove_channel(ConnResId, ConnSt0, ChanId) ->
    ConnSt = emqx_utils_maps:deep_remove([channels, ChanId], ConnSt0),
    do_remove_channel(ConnResId, ChanId),
    {ok, ConnSt}.

on_get_channels(ConnResId) ->
    persistent_term:get(?PT_CHAN_KEY(ConnResId), []).

on_get_channel_status(ConnResId, ChanId, #{health_check_agent := Agent}) ->
    case get_agent_health_check_action(Agent, channel_health_check) of
        {ask, Pid} ->
            Alias = alias([reply]),
            Pid ! {waiting_health_check_result, Alias, channel, ConnResId, ChanId},
            receive
                {Alias, Result} ->
                    Result
            end;
        Result ->
            Result
    end;
on_get_channel_status(_ConnResId, ChanId, #{channels := Chans}) ->
    case Chans of
        #{ChanId := #{health_check_delay := Delay}} ->
            ?tp(connector_demo_channel_health_check_delay, #{}),
            timer:sleep(Delay),
            ?status_connected;
        #{ChanId := _ChanCfg} ->
            ?status_connected;
        #{} ->
            ?status_disconnected
    end.

spawn_counter_process(Name, Register) ->
    Pid = spawn_link(?MODULE, counter_loop, []),
    true = maybe_register(Name, Pid, Register),
    Pid.

stop_counter_process(Pid) ->
    true = erlang:is_process_alive(Pid),
    true = erlang:exit(Pid, shutdown),
    receive
        {'EXIT', Pid, shutdown} -> ok
    after 5000 ->
        {error, timeout}
    end.

counter_loop() ->
    counter_loop(#{
        counter => 0,
        status => running,
        incorrect_status_count => 0
    }).

counter_loop(
    #{
        counter := Num,
        status := Status,
        incorrect_status_count := IncorrectCount
    } = State
) ->
    NewState =
        receive
            block ->
                ct:pal("counter recv: ~p", [block]),
                State#{status => blocked};
            {block, ReplyFun} ->
                ct:pal("counter recv: ~p", [block]),
                apply_reply(ReplyFun, ok),
                State#{status => blocked};
            {block_now, ReplyFun} ->
                ct:pal("counter recv: ~p", [block_now]),
                apply_reply(
                    ReplyFun, {error, {resource_error, #{reason => blocked, msg => blocked}}}
                ),
                State#{status => blocked};
            resume ->
                {messages, Msgs} = erlang:process_info(self(), messages),
                ct:pal("counter recv: ~p, buffered msgs: ~p", [resume, length(Msgs)]),
                State#{status => running};
            {resume, ReplyFun} ->
                {messages, Msgs} = erlang:process_info(self(), messages),
                ct:pal("counter recv: ~p, buffered msgs: ~p", [resume, length(Msgs)]),
                apply_reply(ReplyFun, ok),
                State#{status => running};
            {inc, N, ReplyFun} when Status == running ->
                %ct:pal("async counter recv: ~p", [{inc, N}]),
                apply_reply(ReplyFun, ok),
                ?tp(connector_demo_inc_counter_async, #{n => N}),
                State#{counter => Num + N};
            {big_payload, _Payload, ReplyFun} when Status == blocked ->
                apply_reply(ReplyFun, {error, {recoverable_error, blocked}}),
                State;
            {{FromPid, ReqRef}, {inc, N}} when Status == running ->
                %ct:pal("sync counter recv: ~p", [{inc, N}]),
                FromPid ! {ReqRef, ok},
                State#{counter => Num + N};
            {{FromPid, ReqRef}, {inc, _N}} when Status == blocked ->
                FromPid ! {ReqRef, incorrect_status},
                State#{incorrect_status_count := IncorrectCount + 1};
            {{FromPid, ReqRef}, {big_payload, _Payload}} when Status == blocked ->
                FromPid ! {ReqRef, incorrect_status},
                State#{incorrect_status_count := IncorrectCount + 1};
            {{FromPid, ReqRef}, {big_payload, _Payload}} when Status == running ->
                FromPid ! {ReqRef, ok},
                State;
            {get, ReplyFun} ->
                apply_reply(ReplyFun, Num),
                State;
            {{FromPid, ReqRef}, get_incorrect_status_count} ->
                FromPid ! {ReqRef, IncorrectCount},
                State;
            {{FromPid, ReqRef}, get} ->
                FromPid ! {ReqRef, Num},
                State;
            {{FromPid, ReqRef}, {individual_reply, IsSuccess}} ->
                Res =
                    case IsSuccess of
                        true -> ok;
                        false -> {error, {unrecoverable_error, bad_request}}
                    end,
                FromPid ! {ReqRef, Res},
                State;
            {individual_reply, IsSuccess, ReplyFun} ->
                Res =
                    case IsSuccess of
                        true -> ok;
                        false -> {error, {unrecoverable_error, bad_request}}
                    end,
                apply_reply(ReplyFun, Res),
                State;
            {{random_reply, RandNum}, ReplyFun} ->
                %% usually a behaving  connector should reply once and only once for
                %% each (batch) request
                %% but we try to reply random results a random number of times
                %% with 'ok' in the result, the buffer worker should eventually
                %% drain the buffer (and inflights table)
                ReplyCount = 1 + (RandNum rem 3),
                Results = make_random_replies(ReplyCount),
                %% add a delay to trigger inflight full
                lists:foreach(
                    fun(Result) ->
                        timer:sleep(rand:uniform(5)),
                        apply_reply(ReplyFun, Result)
                    end,
                    Results
                ),
                State;
            {{sleep_before_reply, _} = SleepQ, ReplyFun} ->
                apply_reply(ReplyFun, handle_query(async, SleepQ, Status)),
                State;
            {{FromPid, ReqRef}, {sleep_before_reply, _} = SleepQ} ->
                FromPid ! {ReqRef, handle_query(sync, SleepQ, Status)},
                State
        end,
    counter_loop(NewState).

handle_query(Mode, {sleep_before_reply, For} = Query, Status) ->
    ok = timer:sleep(For),
    Result =
        case Status of
            running -> ok;
            blocked -> {error, {recoverable_error, blocked}}
        end,
    ?tp(connector_demo_sleep_handled, #{
        mode => Mode, query => Query, slept => For, result => Result
    }),
    Result.

maybe_register(Name, Pid, true) ->
    ct:pal("---- Register Name: ~p", [Name]),
    ct:pal("---- whereis(): ~p", [whereis(Name)]),
    erlang:register(Name, Pid);
maybe_register(_Name, _Pid, false) ->
    true.

apply_reply({ReplyFun, Args}, Result) when is_function(ReplyFun) ->
    apply(ReplyFun, Args ++ [Result]).

make_random_replies(0) ->
    [];
make_random_replies(N) ->
    [make_random_reply(N) | make_random_replies(N - 1)].

make_random_reply(N) ->
    case rand:uniform(3) of
        1 ->
            {ok, N};
        2 ->
            {error, {recoverable_error, N}};
        3 ->
            {error, {unrecoverable_error, N}}
    end.

do_add_channel(ConnResId, ChanId, ChanCfg) ->
    Chans = persistent_term:get(?PT_CHAN_KEY(ConnResId), []),
    persistent_term:put(?PT_CHAN_KEY(ConnResId), [{ChanId, ChanCfg} | Chans]).

do_remove_channel(ConnResId, ChanId) ->
    Chans = persistent_term:get(?PT_CHAN_KEY(ConnResId), []),
    persistent_term:put(?PT_CHAN_KEY(ConnResId), proplists:delete(ChanId, Chans)).

get_agent_health_check_action(Agent, Key) ->
    emqx_utils_agent:get_and_update(Agent, fun(Old) ->
        case Old of
            #{Key := [Action]} ->
                {Action, Old};
            #{Key := [Action | Actions]} ->
                {Action, Old#{Key := Actions}};
            #{Key := Action} when not is_list(Action) ->
                {Action, Old}
        end
    end).
