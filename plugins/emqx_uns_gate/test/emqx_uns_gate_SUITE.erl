%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    case whereis(emqx_uns_gate_store) of
        undefined -> ok;
        Pid -> exit(Pid, shutdown)
    end,
    Config.

init_per_testcase(_Case, Config) ->
    case whereis(emqx_uns_gate_store) of
        undefined ->
            {ok, Pid} = emqx_uns_gate_store:start_link(),
            true = unlink(Pid),
            ok;
        _ ->
            ok
    end,
    ok = emqx_uns_gate_store:reset(),
    ok = emqx_uns_gate_config:update(#{
        <<"enabled">> => true,
        <<"on_mismatch">> => <<"deny">>,
        <<"allow_intermediate_publish">> => false,
        <<"exempt_topics">> => [<<"$SYS/#">>]
    }),
    Config.

t_topic_validation(_Config) ->
    {ok, _} = emqx_uns_gate_store:put_model(sample_model(), true),
    ?assertEqual(
        allow,
        emqx_uns_gate_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
        )
    ),
    ?assertEqual(
        {deny, topic_invalid},
        emqx_uns_gate_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/BadLine/LineControl">>
        )
    ),
    ?assertEqual(
        {deny, not_endpoint},
        emqx_uns_gate_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1">>
        )
    ),
    ?assertEqual(
        {deny, payload_invalid},
        emqx_uns_gate_store:validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\"}">>
        )
    ).

t_plugin_api_get_put_post(_Config) ->
    {ok, 200, _, #{id := <<"model-v1">>, active := true}} = emqx_uns_gate_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => true, <<"model">> => sample_model()}}
    ),
    Model2 = (sample_model())#{<<"id">> => <<"model-v2">>, <<"name">> => <<"UNS Model V2">>},
    {ok, 200, _, #{id := <<"model-v2">>, active := false}} = emqx_uns_gate_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => false, <<"model">> => Model2}}
    ),
    {ok, 200, _, #{data := Data}} = emqx_uns_gate_api:handle(get, [<<"models">>], #{}),
    ?assert(length(Data) >= 2),
    {ok, 200, _, #{id := <<"model-v2">>, active := true}} = emqx_uns_gate_api:handle(
        post, [<<"models">>, <<"model-v2">>, <<"activate">>], #{}
    ),
    {ok, 200, _, #{id := <<"model-v2">>, active := true}} = emqx_uns_gate_api:handle(
        get, [<<"model">>], #{}
    ),
    {ok, 200, _, #{result := #{valid := true}}} = emqx_uns_gate_api:handle(
        post,
        [<<"validate">>, <<"topic">>],
        #{
            body => #{
                <<"topic">> => <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
            }
        }
    ).

sample_model() ->
    #{
        <<"id">> => <<"model-v1">>,
        <<"name">> => <<"UNS Model V1">>,
        <<"variable_types">> => #{
            <<"site_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"area_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"process_cell_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"line_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^Line[0-9]{1,4}$">>
            }
        },
        <<"payload_types">> => #{
            <<"line_control">> => #{
                <<"type">> => <<"object">>,
                <<"required">> => [<<"Status">>, <<"Mode">>],
                <<"properties">> => #{
                    <<"Status">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"running">>, <<"stopped">>]
                    },
                    <<"Mode">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"auto">>, <<"manual">>]
                    }
                },
                <<"additionalProperties">> => false
            }
        },
        <<"tree">> => #{
            <<"default">> => #{
                <<"_type">> => <<"namespace">>,
                <<"children">> => #{
                    <<"{site_id}">> => #{
                        <<"_type">> => <<"variable">>,
                        <<"_var_type">> => <<"site_id">>,
                        <<"children">> => #{
                            <<"{area_id}">> => #{
                                <<"_type">> => <<"variable">>,
                                <<"_var_type">> => <<"area_id">>,
                                <<"children">> => #{
                                    <<"Furnaces">> => #{
                                        <<"_type">> => <<"namespace">>,
                                        <<"children">> => #{
                                            <<"{process_cell_id}">> => #{
                                                <<"_type">> => <<"variable">>,
                                                <<"_var_type">> => <<"process_cell_id">>,
                                                <<"children">> => #{
                                                    <<"Lines">> => #{
                                                        <<"_type">> => <<"namespace">>,
                                                        <<"children">> => #{
                                                            <<"{line_id}">> => #{
                                                                <<"_type">> => <<"variable">>,
                                                                <<"_var_type">> => <<"line_id">>,
                                                                <<"children">> => #{
                                                                    <<"LineControl">> => #{
                                                                        <<"_type">> =>
                                                                            <<"endpoint">>,
                                                                        <<"_payload">> =>
                                                                            <<"line_control">>
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }.
