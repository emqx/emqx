%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request_api/2,
        request/3,
        uri/1
    ]
).

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
                            <<"backend">> => <<"builtin_raft">>
                        },
                        <<"queues">> => #{
                            <<"backend">> => <<"builtin_raft">>
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

init_per_testcase(_TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_ds_shared_sub_registry:purge(),
    ok.
%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_basic_crud(_Config) ->
    ?assertMatch(
        {ok, []},
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
    ?assertMatch(
        {ok, [#{<<"id">> := QueueID1}, #{<<"id">> := QueueID2}]},
        api_get(["durable_queues"])
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
        {ok, [#{<<"id">> := QueueID2}]},
        api_get(["durable_queues"])
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

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    case request_api(get, uri(Path)) of
        {ok, ResponseBody} ->
            {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
        {error, _} = Error ->
            Error
    end.

api(Method, Path, Data) ->
    case request(Method, uri(Path), Data) of
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
