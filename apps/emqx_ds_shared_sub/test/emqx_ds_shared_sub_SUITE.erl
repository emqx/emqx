%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

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
                            <<"backend">> => <<"builtin">>
                        }
                    }
                }
            }},
            emqx_ds_shared_sub
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

t_lease_initial(_Config) ->
    ConnRegular = emqtt_connect_sub(<<"client_regular">>),
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    %% Need to pre-reate some streams in "topic/#".
    %% Leader is dummy by far and won't update streams after the first lease to the agent.
    %% So there should be some streams already when the agent connects.
    {ok, _, _} = emqtt:subscribe(ConnRegular, <<"topic/1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic/1">>, <<"hello1">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 5000),

    ok = emqtt:disconnect(ConnRegular),

    ConnShared = emqtt_connect_sub(<<"client_shared">>),
    {ok, _, _} = emqtt:subscribe(ConnShared, <<"$share/gr1/topic/#">>, 1),

    {ok, _} = emqtt:publish(ConnPub, <<"topic/1">>, <<"hello2">>, 1),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

emqtt_connect_sub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {client_id, ClientId},
        {clean_start, true},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 7200}}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_connect_pub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {client_id, ClientId},
        {clean_start, true},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),
    C.
