%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_mgmt_api_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CLIENTID, <<"api_clientid">>).
-define(USERNAME, <<"api_username">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "durable_sessions {\n"
                "    enable = true\n"
                "    renew_streams_interval = 10ms\n"
                "}"},
            {emqx_ds_shared_sub, #{
                config => #{
                    <<"durable_queues">> => #{
                        <<"enable">> => true,
                        <<"session_find_leader_timeout_ms">> => "1200ms"
                    }
                }
            }},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TC, Config) ->
    ClientConfig = #{
        username => ?USERNAME,
        clientid => ?CLIENTID,
        proto_ver => v5,
        clean_start => true,
        properties => #{'Session-Expiry-Interval' => 300}
    },

    {ok, Client} = emqtt:start_link(ClientConfig),
    {ok, _} = emqtt:connect(Client),
    [{client_config, ClientConfig}, {client, Client} | Config].

end_per_testcase(_TC, Config) ->
    Client = proplists:get_value(client, Config),
    emqtt:disconnect(Client).

t_list_with_shared_sub(_Config) ->
    Client = proplists:get_value(client, _Config),
    RealTopic = <<"t/+">>,
    Topic = <<"$share/g1/", RealTopic/binary>>,

    {ok, _, _} = emqtt:subscribe(Client, Topic),
    {ok, _, _} = emqtt:subscribe(Client, RealTopic),

    QS0 = [
        {"clientid", ?CLIENTID},
        {"match_topic", "t/#"}
    ],
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    ?assertMatch(
        #{<<"data">> := [#{<<"clientid">> := ?CLIENTID}, #{<<"clientid">> := ?CLIENTID}]},
        request_json(get, QS0, Headers)
    ),

    QS1 = [
        {"clientid", ?CLIENTID},
        {"share_group", "g1"}
    ],

    ?assertMatch(
        #{<<"data">> := [#{<<"clientid">> := ?CLIENTID, <<"topic">> := <<"$share/g1/t/+">>}]},
        request_json(get, QS1, Headers)
    ).

request_json(Method, Query, Headers) when is_list(Query) ->
    Qs = uri_string:compose_query(Query),
    {ok, MatchRes} = emqx_mgmt_api_test_util:request_api(Method, path(), Qs, Headers),
    emqx_utils_json:decode(MatchRes, [return_maps]).

path() ->
    emqx_mgmt_api_test_util:api_path(["subscriptions"]).
