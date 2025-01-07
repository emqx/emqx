%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_util, [uri/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    <<"durable_sessions">> => #{
                        <<"enable">> => true,
                        <<"renew_streams_interval">> => "100ms"
                    },
                    <<"durable_storage">> => #{
                        <<"messages">> => #{
                            <<"backend">> => <<"builtin_raft">>,
                            <<"n_shards">> => 4
                        },
                        <<"queues">> => #{
                            <<"backend">> => <<"builtin_raft">>,
                            <<"n_shards">> => 4
                        }
                    }
                }
            }},
            emqx,
            {emqx_ds_shared_sub, #{
                config => #{
                    <<"durable_queues">> => #{
                        <<"enable">> => true
                    }
                }
            }},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    _ = emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config),
    ok = snabbkaffe:stop(),
    ok = emqx_ds_shared_sub_registry:purge(),
    ok = destroy_queues(),
    ok.
%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_basic_crud(_Config) ->
    ?assertMatch(
        {ok, #{<<"data">> := []}},
        api_get(["durable_queues"])
    ),

    Resp1 = api(post, ["durable_queues"], #{
        <<"group">> => <<"g1">>,
        <<"topic">> => <<"#">>,
        <<"start_time">> => 42
    }),
    ?assertMatch(
        {ok, 201, #{
            <<"id">> := _QueueID,
            <<"created_at">> := _,
            <<"group">> := <<"g1">>,
            <<"topic">> := <<"#">>,
            <<"start_time">> := 42
        }},
        Resp1
    ),

    ?assertMatch(
        {error, {_, 404, _}},
        api_get(["durable_queues", "non-existent-queue"])
    ),

    {ok, 201, #{<<"id">> := QueueID1}} = Resp1,
    ?assertMatch(
        {ok, #{
            <<"id">> := QueueID1,
            <<"group">> := <<"g1">>,
            <<"topic">> := <<"#">>
        }},
        api_get(["durable_queues", QueueID1])
    ),

    Resp2 = api(post, ["durable_queues"], #{
        <<"group">> => <<"g1">>,
        <<"topic">> => <<"another/topic/filter/+">>,
        <<"start_time">> => 0
    }),
    ?assertMatch(
        {ok, 201, #{
            <<"id">> := _QueueID,
            <<"group">> := <<"g1">>,
            <<"topic">> := <<"another/topic/filter/+">>,
            <<"start_time">> := 0
        }},
        Resp2
    ),

    {ok, 201, #{<<"id">> := QueueID2}} = Resp2,
    Resp3 = api_get(["durable_queues"]),
    ?assertMatch(
        {ok, #{<<"data">> := [#{<<"id">> := _}, #{<<"id">> := _}]}},
        Resp3
    ),
    ?assertMatch(
        [#{<<"id">> := QueueID1}, #{<<"id">> := QueueID2}],
        begin
            {ok, #{<<"data">> := Queues}} = Resp3,
            lists:sort(emqx_utils_maps:key_comparer(<<"id">>), Queues)
        end
    ),

    ?assertMatch(
        {ok, 200, <<"Queue deleted">>},
        api(delete, ["durable_queues", QueueID1], #{})
    ),
    ?assertMatch(
        {ok, 404, #{<<"code">> := <<"NOT_FOUND">>}},
        api(delete, ["durable_queues", QueueID1], #{})
    ),

    ?assertMatch(
        {ok, #{<<"data">> := [#{<<"id">> := QueueID2}]}},
        api_get(["durable_queues"])
    ).

t_list_queues(_Config) ->
    {ok, 201, #{<<"id">> := QID1}} = api(post, ["durable_queues"], #{
        <<"group">> => <<"glq1">>,
        <<"topic">> => <<"#">>,
        <<"start_time">> => 42
    }),
    {ok, 201, #{<<"id">> := QID2}} = api(post, ["durable_queues"], #{
        <<"group">> => <<"glq2">>,
        <<"topic">> => <<"specific/topic">>,
        <<"start_time">> => 0
    }),
    {ok, 201, #{<<"id">> := QID3}} = api(post, ["durable_queues"], #{
        <<"group">> => <<"glq3">>,
        <<"topic">> => <<"1/2/3/+">>,
        <<"start_time">> => emqx_message:timestamp_now()
    }),
    {ok, 201, #{<<"id">> := QID4}} = api(post, ["durable_queues"], #{
        <<"group">> => <<"glq4">>,
        <<"topic">> => <<"4/5/6/#">>,
        <<"start_time">> => emqx_message:timestamp_now()
    }),

    {ok, Resp} = api_get(["durable_queues"]),
    ?assertMatch(
        #{
            <<"data">> := [#{}, #{}, #{}, #{}],
            <<"meta">> := #{<<"hasnext">> := false}
        },
        Resp
    ),

    {ok, Resp1} = api_get(["durable_queues"], #{limit => <<"1">>}),
    ?assertMatch(
        #{
            <<"data">> := [#{}],
            <<"meta">> := #{<<"hasnext">> := true, <<"cursor">> := _}
        },
        Resp1
    ),

    {ok, Resp2} = api_get(["durable_queues"], #{
        limit => <<"1">>,
        cursor => emqx_utils_maps:deep_get([<<"meta">>, <<"cursor">>], Resp1)
    }),
    ?assertMatch(
        #{
            <<"data">> := [#{}],
            <<"meta">> := #{<<"hasnext">> := true, <<"cursor">> := _}
        },
        Resp2
    ),

    {ok, Resp3} = api_get(["durable_queues"], #{
        limit => <<"2">>,
        cursor => emqx_utils_maps:deep_get([<<"meta">>, <<"cursor">>], Resp2)
    }),
    ?assertMatch(
        #{
            <<"data">> := [#{}, #{}],
            <<"meta">> := #{<<"hasnext">> := false}
        },
        Resp3
    ),

    Data = maps:get(<<"data">>, Resp),
    ?assertEqual(
        [QID1, QID2, QID3, QID4],
        lists:sort([ID || #{<<"id">> := ID} <- Data]),
        Resp
    ),

    Data1 = maps:get(<<"data">>, Resp1),
    Data2 = maps:get(<<"data">>, Resp2),
    Data3 = maps:get(<<"data">>, Resp3),
    ?assertEqual(
        [QID1, QID2, QID3, QID4],
        lists:sort([ID || D <- [Data1, Data2, Data3], #{<<"id">> := ID} <- D]),
        [Resp1, Resp2, Resp3]
    ).

t_duplicate_queue(_Config) ->
    ?assertMatch(
        {ok, 201, #{
            <<"id">> := _QueueID,
            <<"group">> := <<"g1">>,
            <<"topic">> := <<"#">>,
            <<"start_time">> := 42
        }},
        api(post, ["durable_queues"], #{
            <<"group">> => <<"g1">>,
            <<"topic">> => <<"#">>,
            <<"start_time">> => 42
        })
    ),

    ?assertMatch(
        {ok, 409, #{<<"code">> := <<"CONFLICT">>}},
        api(post, ["durable_queues"], #{
            <<"group">> => <<"g1">>,
            <<"topic">> => <<"#">>,
            <<"start_time">> => 0
        })
    ).

t_404_when_disable('init', Config) ->
    {ok, _} = emqx_conf:update([durable_queues], #{<<"enable">> => false}, #{}),
    Config;
t_404_when_disable('end', _Config) ->
    {ok, _} = emqx_conf:update([durable_queues], #{<<"enable">> => true}, #{}).

t_404_when_disable(_Config) ->
    ?assertMatch(
        {ok, 404, #{}},
        api(post, ["durable_queues"], #{
            <<"group">> => <<"disabled">>,
            <<"topic">> => <<"#">>
        })
    ).

%%--------------------------------------------------------------------

destroy_queues() ->
    case api_get(["durable_queues"], #{limit => <<"100">>}) of
        {ok, #{<<"data">> := Queues}} ->
            lists:foreach(fun destroy_queue/1, Queues);
        Error ->
            Error
    end.

destroy_queue(#{<<"id">> := QueueID}) ->
    {ok, 200, _Deleted} = api(delete, ["durable_queues", QueueID], #{}).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    api_response(emqx_mgmt_api_test_util:request_api(get, uri(Path))).

api_get(Path, Query) ->
    api_response(emqx_mgmt_api_test_util:request_api(get, uri(Path), Query, [])).

api_response({ok, ResponseBody}) ->
    {ok, jiffy:decode(iolist_to_binary(ResponseBody), [return_maps])};
api_response({error, _} = Error) ->
    Error.

api(Method, Path, Data) ->
    case emqx_mgmt_api_test_util:request(Method, uri(Path), Data) of
        {ok, Code, ResponseBody} ->
            Res =
                case emqx_utils_json:safe_decode(ResponseBody, [return_maps]) of
                    {ok, Decoded} -> Decoded;
                    {error, _} -> ResponseBody
                end,
            {ok, Code, Res};
        {error, _} = Error ->
            Error
    end.
