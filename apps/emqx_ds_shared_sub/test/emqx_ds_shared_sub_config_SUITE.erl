%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/asserts.hrl").

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
            {emqx_ds_shared_sub, #{
                config => #{
                    <<"durable_queues">> => #{
                        <<"enable">> => true,
                        <<"session_find_leader_timeout">> => "1200ms"
                    }
                }
            }},
            emqx
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

t_update_config(_Config) ->
    ?assertEqual(
        1200,
        emqx_ds_shared_sub_config:get(session_find_leader_timeout)
    ),

    {ok, _} = emqx_conf:update([durable_queues], #{session_find_leader_timeout => 2000}, #{}),
    ?assertEqual(
        2000,
        emqx_ds_shared_sub_config:get(session_find_leader_timeout)
    ).
