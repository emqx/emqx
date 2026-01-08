%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggreg_delivery_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-behaviour(emqx_connector_aggreg_delivery).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_connector_aggregator,
            emqx_schema_registry
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

init_transfer_state_and_container_opts(_Buffer, Opts) ->
    ?tp(test_delivery_init, #{buffer => _Buffer, opts => Opts}),
    #{
        container := ContainerOpts,
        upload_options := #{agent := Agent}
    } = Opts,
    {ok, #{agent => Agent, opts => Opts}, ContainerOpts}.

process_append(_IOData, #{agent := Agent} = State) ->
    ?tp(test_delivery_append, #{data => _IOData, state => State}),
    case consult_agent(Agent, append) of
        continue ->
            State;
        {raise, {Kind, Reason}} ->
            erlang:raise(Kind, Reason, [])
    end.

process_write(#{agent := Agent} = State) ->
    ?tp(test_delivery_write, #{state => State}),
    case consult_agent(Agent, write) of
        continue ->
            {ok, State};
        {raise, {Kind, Reason}} ->
            erlang:raise(Kind, Reason, []);
        {return, Value} ->
            Value
    end.

process_complete(#{agent := Agent} = _State) ->
    ?tp(test_delivery_complete, #{state => _State}),
    case consult_agent(Agent, write) of
        continue ->
            {ok, done};
        {raise, {Kind, Reason}} ->
            erlang:raise(Kind, Reason, []);
        {return, Value} ->
            Value
    end.

process_terminate(_State) ->
    ?tp(test_delivery_terminate, #{state => _State}),
    ok.

process_format_status(State) ->
    ?tp(test_delivery_format_status, #{state => State}),
    State.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).

delivery_finished_callback(Result, TestPid, Agent) ->
    ct:pal("delivery finished: ~p", [Result]),
    _ = TestPid ! {delivery_finished, Result},
    case consult_agent(Agent, callback) of
        continue ->
            ok;
        {raise, {Kind, Reason}} ->
            erlang:raise(Kind, Reason, [])
    end.

consult_agent(Agent, Step) ->
    case emqx_utils_agent:get(Agent) of
        continue ->
            continue;
        {ask, Fn} ->
            Fn(Step)
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Verifies that we call the `delivery_finished_callback` when delivery finishes one way or
another.
""".
t_delivery_finished_callback(TCConfig) ->
    {ok, Agent} = emqx_utils_agent:start_link(continue),
    AggregId = ?FUNCTION_NAME,
    MaxRecords = 3,
    AggregOpts = #{
        max_records => MaxRecords,
        time_interval => 120_000,
        work_dir => emqx_cth_suite:work_dir(TCConfig),
        delivery_finished_callback => {fun ?MODULE:delivery_finished_callback/3, [self(), Agent]}
    },
    ContainerOpts = #{type => noop},
    DeliveryOpts = #{
        callback_module => ?MODULE,
        container => ContainerOpts,
        upload_options => #{agent => Agent}
    },
    ?check_trace(
        #{timetrap => 3_000},
        begin
            {ok, Sup} = emqx_connector_aggreg_upload_sup:start_link(
                AggregId, AggregOpts, DeliveryOpts
            ),
            on_exit(fun() -> gen_server:stop(Sup) end),
            Timestamp = now_ms(),
            Records = lists:duplicate(MaxRecords, #{}),
            %% Success: should call callback with `ok`.
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := test_delivery_complete}
                )
            ),
            ?assertReceive({delivery_finished, ok}),
            %% Skipped: should call callback with `{skipped, Reason}`.
            emqx_utils_agent:set(
                Agent,
                {ask, fun
                    (append) ->
                        exit({shutdown, {skipped, some_reason}});
                    (_) ->
                        continue
                end}
            ),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := "aggregated_buffer_delivery_skipped"}
                )
            ),
            ?assertReceive({delivery_finished, {skipped, some_reason}}),
            %% Failure: should call callback with error term.
            emqx_utils_agent:set(
                Agent,
                {ask, fun
                    (write) ->
                        {return, {error, boom}};
                    (_) ->
                        continue
                end}
            ),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := "aggregated_buffer_delivery_failed"}
                )
            ),
            ?assertReceive({delivery_finished, {error, {upload_failed, boom}}}),
            %% Callback crashes: shouldn't affect aggregator process.
            AggregPid = emqx_connector_aggregator:where(AggregId),
            MRef = monitor(process, AggregPid),
            emqx_utils_agent:set(
                Agent,
                {ask, fun
                    (callback) ->
                        {raise, {throw, oops}};
                    (_) ->
                        continue
                end}
            ),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := "aggregated_delivery_finish_callback_exception"}
                )
            ),
            ?assertNotReceive({'DOWN', MRef, _, _, _}),
            ok
        end,
        []
    ),
    ok.
