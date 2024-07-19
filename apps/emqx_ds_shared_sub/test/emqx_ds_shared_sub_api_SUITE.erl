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
            {emqx, #{
                config => #{
                    <<"durable_sessions">> => #{
                        <<"enable">> => true,
                        <<"renew_streams_interval">> => "100ms"
                    },
                    <<"durable_storage">> => #{
                        <<"messages">> => #{
                            <<"backend">> => <<"builtin_raft">>
                        }
                    }
                }
            }},
            emqx_ds_shared_sub,
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
    ok = terminate_leaders(),
    ok.
%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_basic_crud(_Config) ->
    ?assertMatch(
        {ok, []},
        api_get(["durable_queues"])
    ),

    ?assertMatch(
        {ok, 200, #{
            <<"id">> := <<"q1">>
        }},
        api(put, ["durable_queues", "q1"], #{})
    ),

    ?assertMatch(
        {error, {_, 404, _}},
        api_get(["durable_queues", "q2"])
    ),

    ?assertMatch(
        {ok, 200, #{
            <<"id">> := <<"q2">>
        }},
        api(put, ["durable_queues", "q2"], #{})
    ),

    ?assertMatch(
        {ok, #{
            <<"id">> := <<"q2">>
        }},
        api_get(["durable_queues", "q2"])
    ),

    ?assertMatch(
        {ok, [#{<<"id">> := <<"q2">>}, #{<<"id">> := <<"q1">>}]},
        api_get(["durable_queues"])
    ),

    ?assertMatch(
        {ok, 200, <<"Queue deleted">>},
        api(delete, ["durable_queues", "q2"], #{})
    ),

    ?assertMatch(
        {ok, [#{<<"id">> := <<"q1">>}]},
        api_get(["durable_queues"])
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

terminate_leaders() ->
    ok = supervisor:terminate_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    {ok, _} = supervisor:restart_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    ok.
