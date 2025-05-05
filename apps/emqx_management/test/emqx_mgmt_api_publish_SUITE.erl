%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPIC1, <<"api_topic1">>).
-define(TOPIC2, <<"api_topic2">>).

-define(OFFLINE_TOPIC, <<"offline_api_topic">>).

all() ->
    [
        {group, with_ds},
        {group, without_ds}
    ].

groups() ->
    Tcs = emqx_common_test_helpers:all(?MODULE),
    [
        {with_ds, Tcs},
        {without_ds, Tcs}
    ].

init_per_group(without_ds, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config];
init_per_group(with_ds, Config) ->
    DurableSessionsOpts = #{
        <<"enable">> => true,
        <<"heartbeat_interval">> => <<"100ms">>,
        <<"session_gc_interval">> => <<"2s">>
    },
    Opts = #{durable_sessions_opts => DurableSessionsOpts},
    ExtraApps = [emqx_management, emqx_mgmt_api_test_util:emqx_dashboard()],
    emqx_common_test_helpers:start_apps_ds(Config, ExtraApps, Opts).

end_per_group(with_ds, Config) ->
    ok = emqx_common_test_helpers:stop_apps_ds(Config),
    ok;
end_per_group(_, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

t_publish_api({init, Config}) ->
    {ok, Client} = emqtt:start_link(
        #{
            username => <<"api_username">>,
            clientid => <<"api_clientid">>,
            proto_ver => v5
        }
    ),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, ?TOPIC1, ?QOS_1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, ?TOPIC2, ?QOS_1),
    [{client, Client} | Config];
t_publish_api({'end', Config}) ->
    Client = ?config(client, Config),
    emqtt:stop(Client),
    ok;
t_publish_api(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    UserProperties = #{<<"foo">> => <<"bar">>},
    Properties =
        #{
            <<"payload_format_indicator">> => 0,
            <<"message_expiry_interval">> => 1000,
            <<"response_topic">> => ?TOPIC2,
            <<"correlation_data">> => <<"some_correlation_id">>,
            <<"user_properties">> => UserProperties,
            <<"content_type">> => <<"application/json">>
        },
    Body = #{topic => ?TOPIC1, payload => Payload, properties => Properties},
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseMap = decode_json(Response),
    ?assertEqual([<<"id">>], lists:sort(maps:keys(ResponseMap))),
    {ok, Message} = receive_assert(?TOPIC1, 0, Payload),
    RecvProperties = maps:get(properties, Message),
    UserPropertiesList = maps:to_list(UserProperties),
    #{
        'Payload-Format-Indicator' := 0,
        'Message-Expiry-Interval' := RecvMessageExpiry,
        'Correlation-Data' := <<"some_correlation_id">>,
        'User-Property' := UserPropertiesList,
        'Content-Type' := <<"application/json">>
    } = RecvProperties,
    ?assert(RecvMessageExpiry =< 1000),
    %% note: without props this time
    Body2 = #{topic => ?TOPIC2, payload => Payload},
    {ok, Response2} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body2),
    ResponseMap2 = decode_json(Response2),
    ?assertEqual([<<"id">>], lists:sort(maps:keys(ResponseMap2))),
    ?assertEqual(ok, element(1, receive_assert(?TOPIC2, 0, Payload))).

t_publish_no_subscriber({init, Config}) ->
    Config;
t_publish_no_subscriber({'end', _Config}) ->
    ok;
t_publish_no_subscriber(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = #{topic => ?TOPIC1, payload => Payload},
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseMap = decode_json(Response),
    ?assertEqual([<<"message">>, <<"reason_code">>], lists:sort(maps:keys(ResponseMap))),
    ?assertMatch(#{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}, ResponseMap),
    ok.

t_publish_bad_topic({init, Config}) ->
    Config;
t_publish_bad_topic({'end', _Config}) ->
    ok;
t_publish_bad_topic(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = #{topic => <<"not/a+/valid/topic">>, payload => Payload},
    ?assertMatch(
        {error, {_, 400, _}}, emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body)
    ).

t_publish_bad_base64({init, Config}) ->
    Config;
t_publish_bad_base64({'end', _Config}) ->
    ok;
t_publish_bad_base64(_) ->
    %% not a valid base64
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = #{
        topic => <<"not/a+/valid/topic">>, payload => Payload, payload_encoding => <<"base64">>
    },
    ?assertMatch(
        {error, {_, 400, _}}, emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body)
    ).

t_publish_too_large({init, Config}) ->
    MaxPacketSize = 100,
    meck:new(emqx_config, [no_link, passthrough, no_history]),
    meck:expect(emqx_config, get, fun
        ([mqtt, max_packet_size]) ->
            MaxPacketSize;
        (Other) ->
            meck:passthrough(Other)
    end),
    [{max_packet_size, MaxPacketSize} | Config];
t_publish_too_large({'end', _Config}) ->
    meck:unload(emqx_config),
    ok;
t_publish_too_large(Config) ->
    MaxPacketSize = proplists:get_value(max_packet_size, Config),
    Payload = lists:duplicate(MaxPacketSize, $0),
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = #{topic => <<"random/topic">>, payload => Payload},
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            Auth,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 400, _}, Summary),
    ?assertMatch(
        #{
            <<"reason_code">> := ?RC_QUOTA_EXCEEDED,
            <<"message">> := <<"packet_too_large">>
        },
        decode_json(ResponseBody)
    ),
    ok.

t_publish_bad_topic_bulk({init, Config}) ->
    Config;
t_publish_bad_topic_bulk({'end', _Config}) ->
    ok;
t_publish_bad_topic_bulk(_Config) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [
        #{topic => <<"not/a+/valid/topic">>, payload => Payload},
        #{topic => <<"good/topic">>, payload => Payload}
    ],
    ?assertMatch(
        {error, {_, 400, _}}, emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body)
    ).

t_publish_bulk_api({init, Config}) ->
    {ok, Client} = emqtt:start_link(#{
        username => <<"api_username">>, clientid => <<"api_clientid">>
    }),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    [{client, Client} | Config];
t_publish_bulk_api({'end', Config}) ->
    Client = ?config(client, Config),
    emqtt:stop(Client),
    ok;
t_publish_bulk_api(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [
        #{
            topic => ?TOPIC1,
            payload => Payload,
            payload_encoding => plain
        },
        #{
            topic => ?TOPIC2,
            payload => base64:encode(Payload),
            payload_encoding => base64
        }
    ],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseList = decode_json(Response),
    ?assertEqual(2, erlang:length(ResponseList)),
    lists:foreach(
        fun(ResponseMap) ->
            ?assertMatch(
                [<<"id">>], lists:sort(maps:keys(ResponseMap))
            )
        end,
        ResponseList
    ),
    ?assertEqual(ok, element(1, receive_assert(?TOPIC1, 0, Payload))),
    ?assertEqual(ok, element(1, receive_assert(?TOPIC2, 0, Payload))).

t_publish_no_subscriber_bulk({init, Config}) ->
    Config;
t_publish_no_subscriber_bulk({'end', _Config}) ->
    ok;
t_publish_no_subscriber_bulk(_) ->
    {ok, Client} = emqtt:start_link(#{
        username => <<"api_username">>, clientid => <<"api_clientid">>
    }),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"no/subscrbier/topic">>, payload => Payload}
    ],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseList = decode_json(Response),
    ?assertMatch(
        [
            #{<<"id">> := _},
            #{<<"id">> := _},
            #{<<"message">> := <<"no_matching_subscribers">>}
        ],
        ResponseList
    ),
    ?assertEqual(ok, element(1, receive_assert(?TOPIC1, 0, Payload))),
    ?assertEqual(ok, element(1, receive_assert(?TOPIC2, 0, Payload))),
    emqtt:stop(Client).

t_publish_bulk_dispatch_one_message_invalid_topic({init, Config}) ->
    Config;
t_publish_bulk_dispatch_one_message_invalid_topic({'end', _Config}) ->
    ok;
t_publish_bulk_dispatch_one_message_invalid_topic(Config) when is_list(Config) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"bad/#/topic">>, payload => Payload}
    ],
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            Auth,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 400, _}, Summary),
    ?assertMatch(
        #{<<"reason_code">> := ?RC_TOPIC_NAME_INVALID},
        decode_json(ResponseBody)
    ).

t_publish_bulk_dispatch_failure({init, Config}) ->
    meck:new(emqx, [no_link, passthrough, no_history]),
    meck:expect(emqx, is_running, fun() -> false end),
    {ok, Client} = emqtt:start_link(#{
        username => <<"api_username">>, clientid => <<"api_clientid">>
    }),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    [{client, Client} | Config];
t_publish_bulk_dispatch_failure({'end', Config}) ->
    meck:unload(emqx),
    Client = ?config(client, Config),
    emqtt:stop(Client),
    ok;
t_publish_bulk_dispatch_failure(Config) when is_list(Config) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"no/subscrbier/topic">>, payload => Payload}
    ],
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            Auth,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 503, _}, Summary),
    ?assertMatch(
        [
            #{<<"reason_code">> := ?RC_IMPLEMENTATION_SPECIFIC_ERROR},
            #{<<"reason_code">> := ?RC_IMPLEMENTATION_SPECIFIC_ERROR},
            #{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}
        ],
        decode_json(ResponseBody)
    ).

t_publish_offline_api({init, Config}) ->
    {ok, Client} = emqtt:start_link(
        #{
            username => <<"api_username">>,
            clientid => <<"api_clientid">>,
            proto_ver => v5,
            clean_start => false,
            properties => #{'Session-Expiry-Interval' => 60}
        }
    ),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, ?OFFLINE_TOPIC, ?QOS_1),
    _ = emqtt:stop(Client),
    Config;
t_publish_offline_api({'end', _Config}) ->
    ok;
t_publish_offline_api(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    UserProperties = #{<<"foo">> => <<"bar">>},
    Properties =
        #{
            <<"payload_format_indicator">> => 0,
            <<"message_expiry_interval">> => 1000,
            <<"correlation_data">> => <<"some_correlation_id">>,
            <<"user_properties">> => UserProperties,
            <<"content_type">> => <<"application/json">>
        },
    Body = #{topic => ?OFFLINE_TOPIC, payload => Payload, properties => Properties},
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseMap = decode_json(Response),
    ?assertEqual([<<"id">>], lists:sort(maps:keys(ResponseMap))).

%% Checks that we return HTTP response status code 200 for delayed messages, even if there
%% are no subscribers at the time of publishing.
t_delayed_message_always_200({init, Config}) ->
    Config;
t_delayed_message_always_200({'end', _Config}) ->
    ok;
t_delayed_message_always_200(Config) when is_list(Config) ->
    Request = #{
        <<"topic">> => <<"$delayed/1/t">>,
        <<"payload">> => <<"delayed">>
    },
    ?assertMatch(
        {200, #{<<"id">> := _}},
        emqx_mgmt_api_test_util:simple_request(
            post,
            emqx_mgmt_api_test_util:api_path(["publish"]),
            Request
        )
    ),
    ok.

receive_assert(Topic, Qos, Payload) ->
    receive
        {publish, Message} ->
            ReceiveTopic = maps:get(topic, Message),
            ReceiveQos = maps:get(qos, Message),
            ReceivePayload = maps:get(payload, Message),
            ?assertEqual(Topic, ReceiveTopic),
            ?assertEqual(Qos, ReceiveQos),
            ?assertEqual(Payload, ReceivePayload),
            {ok, Message}
    after 5000 ->
        {error, timeout}
    end.

decode_json(In) ->
    emqx_utils_json:decode(In).
