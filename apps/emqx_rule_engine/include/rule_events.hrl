%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-define(COLUMNS(EVENT), [Key || {Key, _ExampleVal} <- ?EG_COLUMNS(EVENT)]).

-define(EG_COLUMNS(EVENT),
        case EVENT of
        'message.publish' ->
                [ {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"flags">>, #{}}
                , {<<"headers">>, undefined}
                , {<<"publish_received_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'message.delivered' ->
                [ {<<"event">>, 'message.delivered'}
                , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
                , {<<"from_clientid">>, <<"c_emqx_1">>}
                , {<<"from_username">>, <<"u_emqx_1">>}
                , {<<"clientid">>, <<"c_emqx_2">>}
                , {<<"username">>, <<"u_emqx_2">>}
                , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"flags">>, #{}}
                , {<<"publish_received_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'message.acked' ->
                [ {<<"event">>, 'message.acked'}
                , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
                , {<<"from_clientid">>, <<"c_emqx_1">>}
                , {<<"from_username">>, <<"u_emqx_1">>}
                , {<<"clientid">>, <<"c_emqx_2">>}
                , {<<"username">>, <<"u_emqx_2">>}
                , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"flags">>, #{}}
                , {<<"publish_received_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'message.dropped' ->
                [ {<<"event">>, 'message.dropped'}
                , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
                , {<<"reason">>, no_subscribers}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"flags">>, #{}}
                , {<<"publish_received_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'client.connected' ->
                [ {<<"event">>, 'client.connected'}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"mountpoint">>, undefined}
                , {<<"peername">>, <<"192.168.0.10:56431">>}
                , {<<"sockname">>, <<"0.0.0.0:1883">>}
                , {<<"proto_name">>, <<"MQTT">>}
                , {<<"proto_ver">>, 5}
                , {<<"keepalive">>, 60}
                , {<<"clean_start">>, true}
                , {<<"expiry_interval">>, 3600}
                , {<<"is_bridge">>, false}
                , {<<"connected_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'client.disconnected' ->
                [ {<<"event">>, 'client.disconnected'}
                , {<<"reason">>, normal}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"peername">>, <<"192.168.0.10:56431">>}
                , {<<"sockname">>, <<"0.0.0.0:1883">>}
                , {<<"disconnected_at">>, erlang:system_time(millisecond)}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'session.subscribed' ->
                [ {<<"event">>, 'session.subscribed'}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        'session.unsubscribed' ->
                [ {<<"event">>, 'session.unsubscribed'}
                , {<<"clientid">>, <<"c_emqx">>}
                , {<<"username">>, <<"u_emqx">>}
                , {<<"peerhost">>, <<"192.168.0.10">>}
                , {<<"topic">>, <<"t/a">>}
                , {<<"qos">>, 1}
                , {<<"timestamp">>, erlang:system_time(millisecond)}
                , {<<"node">>, node()}
                ];
        RuleType ->
                error({unknown_rule_type, RuleType})
        end).

-define(TEST_COLUMNS_MESSGE,
        [ {<<"clientid">>, <<"c_emqx">>}
        , {<<"username">>, <<"u_emqx">>}
        , {<<"topic">>, <<"t/a">>}
        , {<<"qos">>, 1}
        , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
        ]).

-define(TEST_COLUMNS_MESSGE_DELIVERED_ACKED,
        [ {<<"from_clientid">>, <<"c_emqx_1">>}
        , {<<"from_username">>, <<"u_emqx_1">>}
        , {<<"clientid">>, <<"c_emqx_2">>}
        , {<<"username">>, <<"u_emqx_2">>}
        , {<<"topic">>, <<"t/a">>}
        , {<<"qos">>, 1}
        , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
        ]).

-define(TEST_COLUMNS(EVENT),
        case EVENT of
        'message.publish' -> ?TEST_COLUMNS_MESSGE;
        'message.dropped' -> ?TEST_COLUMNS_MESSGE;
        'message.delivered' -> ?TEST_COLUMNS_MESSGE_DELIVERED_ACKED;
        'message.acked' -> ?TEST_COLUMNS_MESSGE_DELIVERED_ACKED;
        'client.connected' ->
            [ {<<"clientid">>, <<"c_emqx">>}
            , {<<"username">>, <<"u_emqx">>}
            , {<<"peername">>, <<"127.0.0.1:52918">>}
            ];
        'client.disconnected' ->
            [ {<<"clientid">>, <<"c_emqx">>}
            , {<<"username">>, <<"u_emqx">>}
            , {<<"reason">>, <<"normal">>}
            ];
        'session.subscribed' ->
            [ {<<"clientid">>, <<"c_emqx">>}
            , {<<"username">>, <<"u_emqx">>}
            , {<<"topic">>, <<"t/a">>}
            , {<<"qos">>, 1}
            ];
        'session.unsubscribed' ->
            [ {<<"clientid">>, <<"c_emqx">>}
            , {<<"username">>, <<"u_emqx">>}
            , {<<"topic">>, <<"t/a">>}
            , {<<"qos">>, 1}
            ];
        RuleType ->
            error({unknown_rule_type, RuleType})
        end).

-define(EVENT_INFO_MESSAGE_PUBLISH,
        #{ event => '$events/message_publish',
           title => #{en => <<"message publish">>, zh => <<"消息发布"/utf8>>},
           description => #{en => <<"message publish">>, zh => <<"消息发布"/utf8>>},
           test_columns => ?TEST_COLUMNS('message.publish'),
           columns => ?COLUMNS('message.publish'),
           sql_example => <<"SELECT payload.msg as msg FROM \"t/#\" WHERE msg = 'hello'">>
        }).

-define(EVENT_INFO_MESSAGE_DELIVER,
        #{ event => '$events/message_delivered',
           title => #{en => <<"message delivered">>, zh => <<"消息投递"/utf8>>},
           description => #{en => <<"message delivered">>, zh => <<"消息投递"/utf8>>},
           test_columns => ?TEST_COLUMNS('message.delivered'),
           columns => ?COLUMNS('message.delivered'),
           sql_example => <<"SELECT * FROM \"$events/message_delivered\" WHERE topic =~ 't/#'">>
        }).

-define(EVENT_INFO_MESSAGE_ACKED,
        #{ event => '$events/message_acked',
           title => #{en => <<"message acked">>, zh => <<"消息应答"/utf8>>},
           description => #{en => <<"message acked">>, zh => <<"消息应答"/utf8>>},
           test_columns => ?TEST_COLUMNS('message.acked'),
           columns => ?COLUMNS('message.acked'),
           sql_example => <<"SELECT * FROM \"$events/message_acked\" WHERE topic =~ 't/#'">>
        }).

-define(EVENT_INFO_MESSAGE_DROPPED,
        #{ event => '$events/message_dropped',
           title => #{en => <<"message dropped">>, zh => <<"消息丢弃"/utf8>>},
           description => #{en => <<"message dropped">>, zh => <<"消息丢弃"/utf8>>},
           test_columns => ?TEST_COLUMNS('message.dropped'),
           columns => ?COLUMNS('message.dropped'),
           sql_example => <<"SELECT * FROM \"$events/message_dropped\" WHERE topic =~ 't/#'">>
        }).

-define(EVENT_INFO_CLIENT_CONNECTED,
        #{ event => '$events/client_connected',
           title => #{en => <<"client connected">>, zh => <<"连接建立"/utf8>>},
           description => #{en => <<"client connected">>, zh => <<"连接建立"/utf8>>},
           test_columns => ?TEST_COLUMNS('client.connected'),
           columns => ?COLUMNS('client.connected'),
           sql_example => <<"SELECT * FROM \"$events/client_connected\"">>
        }).

-define(EVENT_INFO_CLIENT_DISCONNECTED,
        #{ event => '$events/client_disconnected',
           title => #{en => <<"client disconnected">>, zh => <<"连接断开"/utf8>>},
           description => #{en => <<"client disconnected">>, zh => <<"连接断开"/utf8>>},
           test_columns => ?TEST_COLUMNS('client.disconnected'),
           columns => ?COLUMNS('client.disconnected'),
           sql_example => <<"SELECT * FROM \"$events/client_disconnected\"">>
        }).

-define(EVENT_INFO_SESSION_SUBSCRIBED,
        #{ event => '$events/session_subscribed',
           title => #{en => <<"session subscribed">>, zh => <<"会话订阅完成"/utf8>>},
           description => #{en => <<"session subscribed">>, zh => <<"会话订阅完成"/utf8>>},
           test_columns => ?TEST_COLUMNS('session.subscribed'),
           columns => ?COLUMNS('session.subscribed'),
           sql_example => <<"SELECT * FROM \"$events/session_subscribed\" WHERE topic =~ 't/#'">>
        }).

-define(EVENT_INFO_SESSION_UNSUBSCRIBED,
        #{ event => '$events/session_unsubscribed',
           title => #{en => <<"session unsubscribed">>, zh => <<"会话取消订阅完成"/utf8>>},
           description => #{en => <<"session unsubscribed">>, zh => <<"会话取消订阅完成"/utf8>>},
           test_columns => ?TEST_COLUMNS('session.unsubscribed'),
           columns => ?COLUMNS('session.unsubscribed'),
           sql_example => <<"SELECT * FROM \"$events/session_unsubscribed\" WHERE topic =~ 't/#'">>
        }).

-define(EVENT_INFO,
        [ ?EVENT_INFO_MESSAGE_PUBLISH
        , ?EVENT_INFO_MESSAGE_DELIVER
        , ?EVENT_INFO_MESSAGE_ACKED
        , ?EVENT_INFO_MESSAGE_DROPPED
        , ?EVENT_INFO_CLIENT_CONNECTED
        , ?EVENT_INFO_CLIENT_DISCONNECTED
        , ?EVENT_INFO_SESSION_SUBSCRIBED
        , ?EVENT_INFO_SESSION_UNSUBSCRIBED
        ]).

-define(EG_ENVS(EVENT_TOPIC),
        case EVENT_TOPIC of
        <<"$events/", _/binary>> ->
            EventName = emqx_rule_events:event_name(EVENT_TOPIC),
            emqx_rule_maps:atom_key_map(maps:from_list(?EG_COLUMNS(EventName)));
        _PublishTopic ->
            #{id => emqx_guid:to_hexstr(emqx_guid:gen()),
              clientid => <<"c_emqx">>,
              username => <<"u_emqx">>,
              payload => <<"{\"id\": 1, \"name\": \"ha\"}">>,
              peerhost => <<"127.0.0.1">>,
              topic => <<"t/a">>,
              qos => 1,
              flags => #{sys => true, event => true},
              publish_received_at => emqx_rule_utils:now_ms(),
              timestamp => emqx_rule_utils:now_ms(),
              node => node()
              }
        end).
