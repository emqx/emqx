%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggreg_delivery_parquet_SUITE).

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
    #{container := ContainerOpts} = Opts,
    {ok, #{opts => Opts}, ContainerOpts}.

process_append(_IOData, State) ->
    ?tp(test_delivery_append, #{data => _IOData, state => State}),
    State.

process_write(State) ->
    ?tp(test_delivery_write, #{state => State}),
    {ok, State}.

process_complete(_State) ->
    ?tp(test_delivery_complete, #{state => _State}),
    {ok, done}.

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

add_serde(Name, Params) ->
    on_exit(fun() -> emqx_schema_registry:delete_schema(Name) end),
    ok = emqx_schema_registry:add_schema(Name, Params),
    ok.

add_sample_serde1(Name) ->
    add_serde(Name, #{
        type => avro,
        source => emqx_utils_json:encode(
            emqx_connector_aggregator_test_helpers:sample_avro_schema1()
        )
    }).

%% This is unsupported when `write_old_list_structure = true`.
schema_with_unsupported_column_type() ->
    #{
        <<"name">> => <<"root">>,
        <<"type">> => <<"record">>,
        <<"fields">> =>
            [
                #{
                    <<"field-id">> => 1,
                    <<"name">> => <<"x">>,
                    <<"type">> => [
                        <<"null">>,
                        #{
                            <<"type">> => <<"array">>,
                            <<"element-id">> => 2,
                            <<"items">> => [<<"null">>, <<"string">>]
                        }
                    ]
                }
            ]
    }.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Verifies that we throw errors containing reason when the referenced schema in a Parquet
container, during runtime, when starting a delivery process.

- Schema not found
- Wrong type (not Avro)
- Unsupported column type

""".
t_parquet_schema_ref_problems_runtime(TCConfig) ->
    ?check_trace(
        #{timetrap => 3_000},
        begin
            AggregId = ?FUNCTION_NAME,
            MaxRecords = 3,
            AggregOpts = #{
                max_records => MaxRecords,
                time_interval => 120_000,
                work_dir => emqx_cth_suite:work_dir(TCConfig)
            },
            SerdeName = <<"my_avro_serde">>,
            ContainerOpts = #{
                type => parquet,
                schema => #{
                    type => avro_ref,
                    name => SerdeName
                },
                %% Needed for the "unsupported column type" case
                write_old_list_structure => true
            },
            DeliveryOpts = #{
                callback_module => ?MODULE,
                container => ContainerOpts,
                upload_options => #{}
            },
            {ok, Sup} = emqx_connector_aggreg_upload_sup:start_link(
                AggregId, AggregOpts, DeliveryOpts
            ),
            on_exit(fun() -> gen_server:stop(Sup) end),
            Timestamp = now_ms(),
            Records0 = lists:duplicate(MaxRecords, #{}),
            %% Schema reference is currently missing.
            ?assertMatch(
                {ok,
                    {ok, #{reason := {container_opts_error, #{reason := parquet_schema_not_found}}}}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records0),
                    #{?snk_kind := "connector_aggreg_delivery_init_failure"}
                )
            ),
            %% We now create the schema, but with the wrong type
            add_serde(SerdeName, #{type => json, source => <<"{}">>}),
            ?assertMatch(
                {ok,
                    {ok, #{
                        reason :=
                            {container_opts_error, #{
                                reason := parquet_schema_wrong_type,
                                expected_type := avro,
                                schema_name := SerdeName,
                                schema_type := json
                            }}
                    }}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records0),
                    #{?snk_kind := "connector_aggreg_delivery_init_failure"}
                )
            ),
            %% Update schema to correct type, but with unsupported column type
            add_serde(SerdeName, #{
                type => avro,
                source => emqx_utils_json:encode(
                    schema_with_unsupported_column_type()
                )
            }),
            ?assertMatch(
                {ok,
                    {ok, #{
                        reason :=
                            {container_opts_error, #{
                                reason := unsupported_type,
                                type := _,
                                %% "Null array elements are not allowed when
                                %% `write_old_list_structure=true`"
                                hint := _
                            }}
                    }}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records0),
                    #{?snk_kind := "connector_aggreg_delivery_init_failure"}
                )
            ),
            %% Finally, use a valid schema and see it work.
            add_sample_serde1(SerdeName),
            Records1 = lists:duplicate(MaxRecords, #{
                <<"clientid">> => <<"cid">>,
                <<"qos">> => 1
            }),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records1),
                    #{?snk_kind := test_delivery_complete}
                )
            ),
            ok
        end,
        []
    ),
    ok.
