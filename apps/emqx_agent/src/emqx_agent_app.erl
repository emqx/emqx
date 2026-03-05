%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_agent_sup:start_link(),
    ok = emqx_agent_skill_clickhouse:init(),
    ok = emqx_agent_skill_clickhouse:create(clickhouse_context()),
    {ok, Sup}.

stop(_State) ->
    Ctx = clickhouse_context(),
    ok = emqx_agent_skill_clickhouse:destroy(maps:get(skill_id, Ctx)),
    ok = emqx_agent_skill_clickhouse:deinit().

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

clickhouse_context() ->
    #{
        skill_id => <<"ch-default">>,
        desc =>
            <<"Query historical telemetry for a device and metric ",
                "(templated and tenant-scoped).">>,
        input => #{
            <<"site">> => #{<<"type">> => <<"string">>},
            <<"device_id">> => #{<<"type">> => <<"string">>},
            <<"metric">> => #{<<"type">> => <<"string">>},
            <<"window_min">> => #{
                <<"type">> => <<"integer">>, <<"minimum">> => 1, <<"maximum">> => 1440
            },
            <<"downsample_sec">> => #{
                <<"type">> => <<"integer">>, <<"minimum">> => 1, <<"maximum">> => 3600
            }
        },
        query =>
            ~b"""
            SELECT
                toStartOfInterval(ts, INTERVAL ${downsample_sec} SECOND) AS t,
                avg(value)  AS avg,
                min(value)  AS min,
                max(value)  AS max,
                count()     AS cnt
            FROM historical_telemetry
            WHERE
                site = ${site}
                AND device_id = ${device_id}
                AND metric = ${metric}
                AND ts >= now() - INTERVAL ${window_min} MINUTE
            GROUP BY t
            ORDER BY t ASC
            WITH TOTALS
            """,
        output => #{
            <<"rows">> => #{
                <<"type">> => <<"array">>,
                <<"items">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{
                        <<"t">> => #{<<"type">> => <<"string">>, <<"format">> => <<"date-time">>},
                        <<"avg">> => #{<<"type">> => <<"number">>},
                        <<"min">> => #{<<"type">> => <<"number">>},
                        <<"max">> => #{<<"type">> => <<"number">>},
                        <<"cnt">> => #{<<"type">> => <<"integer">>}
                    },
                    <<"required">> => [<<"t">>, <<"avg">>, <<"min">>, <<"max">>, <<"cnt">>]
                }
            }
        }
    }.
