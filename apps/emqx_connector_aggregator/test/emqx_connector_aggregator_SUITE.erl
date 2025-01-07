%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggregator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_connector_aggregator],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

init_transfer_state(_Buffer, Opts) ->
    #{opts => Opts}.

process_append(_IOData, State) ->
    State.

process_write(State) ->
    {ok, State}.

process_complete(_State) ->
    {ok, done}.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Verifies that a new delivery is triggered when we reach the configured maximum number
%% of records.
t_trigger_max_records(Config) ->
    ?check_trace(
        #{timetrap => 3_000},
        begin
            AggregId = ?FUNCTION_NAME,
            MaxRecords = 3,
            AggregOpts = #{
                max_records => MaxRecords,
                time_interval => 120_000,
                work_dir => emqx_cth_suite:work_dir(Config)
            },
            ContainerOpts = #{
                type => csv,
                column_order => []
            },
            DeliveryOpts = #{
                callback_module => ?MODULE,
                container => ContainerOpts,
                upload_options => #{}
            },
            {ok, _Sup} = emqx_connector_aggreg_upload_sup:start_link(
                AggregId, AggregOpts, DeliveryOpts
            ),
            Timestamp = now_ms(),
            Records = lists:duplicate(MaxRecords, #{}),
            %% Should immediately trigger a delivery process to be kicked off.
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := connector_aggreg_delivery_completed}
                )
            ),
            ok
        end,
        []
    ),
    ok.
