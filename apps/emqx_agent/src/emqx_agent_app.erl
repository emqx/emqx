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
    ok = emqx_agent_skill_kv:init(),
    ok = emqx_agent_skill_kv:create(assets_context()),
    ok = emqx_agent_skill_http:init(),
    ok = emqx_agent_skill_http:create(weather_context()),
    ok = emqx_agent_skill_publish:init(),
    ok = emqx_agent_session:init_hook(),
    ok = emqx_agent_pipeline_mgr:init_hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_agent_skill_clickhouse:destroy(maps:get(skill_id, clickhouse_context())),
    ok = emqx_agent_skill_clickhouse:deinit(),
    ok = emqx_agent_skill_kv:destroy(maps:get(skill_id, assets_context())),
    ok = emqx_agent_skill_kv:deinit(),
    ok = emqx_agent_skill_http:destroy(maps:get(skill_id, weather_context())),
    ok = emqx_agent_skill_http:deinit(),
    ok = emqx_agent_skill_publish:deinit(),
    ok = emqx_agent_session:deinit_hook(),
    ok = emqx_agent_pipeline_mgr:deinit_hook().

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

weather_context() ->
    #{
        skill_id => <<"weather-default">>,
        desc => <<"Fetch current weather conditions for a city.">>,
        method => get,
        url => <<"https://api.open-meteo.com/v1/forecast">>,
        headers => #{},
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"latitude">> => #{<<"type">> => <<"number">>},
                <<"longitude">> => #{<<"type">> => <<"number">>},
                <<"current">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"latitude">>, <<"longitude">>]
        },
        output_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"current">> => #{<<"type">> => <<"object">>}
            },
            <<"required">> => [<<"current">>]
        }
    }.

assets_context() ->
    #{
        skill_id => <<"assets">>,
        desc => <<"IoT device assets">>,
        allow_put => true,
        data_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"asset_id">> => #{<<"type">> => <<"string">>},
                <<"criticality">> => #{
                    <<"type">> => <<"string">>, <<"enum">> => [<<"low">>, <<"medium">>, <<"high">>]
                },
                <<"sla">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"asset_id">>, <<"criticality">>, <<"sla">>]
        }
    }.

clickhouse_context() ->
    #{
        skill_id => <<"ch-default">>,
        desc =>
            <<"Query historical telemetry for a device and metric ",
                "(templated and tenant-scoped).">>,
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
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
            <<"required">> => [
                <<"site">>, <<"device_id">>, <<"metric">>, <<"window_min">>, <<"downsample_sec">>
            ]
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
        output_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"rows">> => #{
                    <<"type">> => <<"array">>,
                    <<"items">> => #{
                        <<"type">> => <<"object">>,
                        <<"properties">> => #{
                            <<"t">> => #{
                                <<"type">> => <<"string">>, <<"format">> => <<"date-time">>
                            },
                            <<"avg">> => #{<<"type">> => <<"number">>},
                            <<"min">> => #{<<"type">> => <<"number">>},
                            <<"max">> => #{<<"type">> => <<"number">>},
                            <<"cnt">> => #{<<"type">> => <<"integer">>}
                        },
                        <<"required">> => [<<"t">>, <<"avg">>, <<"min">>, <<"max">>, <<"cnt">>]
                    }
                }
            },
            <<"required">> => [<<"rows">>]
        }
    }.
