%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggregator_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

%% API
-export([truncate_at/2]).
-export([aggregation_container_config_parquet_inline/1]).
-export([sample_avro_schema1/0]).

%%------------------------------------------------------------------------------
%% File utilities
%%------------------------------------------------------------------------------

truncate_at(Filename, Pos) ->
    {ok, FD} = file:open(Filename, [read, write, binary]),
    {ok, Pos} = file:position(FD, Pos),
    ok = file:truncate(FD),
    ok = file:close(FD).

%%------------------------------------------------------------------------------
%% Sample schemas
%%------------------------------------------------------------------------------

aggregation_container_config_parquet_inline(Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => <<"parquet">>,
            <<"schema">> => #{
                <<"type">> => <<"avro_inline">>,
                <<"def">> => emqx_utils_json:encode(sample_avro_schema1())
            }
        },
        Overrides
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

sample_avro_schema1() ->
    #{
        <<"name">> => <<"root">>,
        <<"type">> => <<"record">>,
        <<"fields">> =>
            [
                #{
                    <<"field-id">> => 1,
                    <<"name">> => <<"clientid">>,
                    <<"type">> => <<"string">>
                },
                #{
                    <<"field-id">> => 2,
                    <<"name">> => <<"qos">>,
                    <<"type">> => <<"int">>
                },
                #{
                    <<"field-id">> => 3,
                    <<"name">> => <<"payload">>,
                    <<"default">> => null,
                    <<"type">> => [<<"null">>, <<"string">>]
                },
                #{
                    <<"field-id">> => 4,
                    <<"name">> => <<"publish_received_at">>,
                    <<"default">> => null,
                    <<"type">> => [
                        <<"null">>,
                        #{
                            <<"type">> => <<"long">>,
                            <<"adjust-to-utc">> => false,
                            <<"logicalType">> => <<"timestamp-micros">>
                        }
                    ]
                }
            ]
    }.
