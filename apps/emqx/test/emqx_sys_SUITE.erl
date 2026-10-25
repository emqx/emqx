%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    %% Forward every `$SYS` message that reaches the publish hooks, the way the rule engine sees them.
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, [self()]}, 0),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_config:erase(rule_engine),
    ok.

on_message_publish(Msg, Pid) ->
    emqx_message:is_sys(Msg) andalso (Pid ! {sys_publish, emqx_message:topic(Msg)}),
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Without a subscriber and with the rule engine ignoring `$SYS` messages, the client events are not published.
t_client_event_skipped_without_subscriber(_Config) ->
    ClientId = connect_and_disconnect(),
    ?assertEqual([], client_event_topics(ClientId, 500)).

%% A subscriber on the event topic receives the connected event; the disconnected event still has no subscriber.
t_client_event_published_to_subscriber(_Config) ->
    Filter = <<"$SYS/brokers/+/clients/+/connected">>,
    ok = emqx:subscribe(Filter),
    try
        ClientId = connect_and_disconnect(),
        Expected = systop(<<"clients/", ClientId/binary, "/connected">>),
        receive
            {deliver, Filter, #message{topic = Topic}} -> ?assertEqual(Expected, Topic)
        after 1000 -> ct:fail("connected event not delivered")
        end,
        ?assertEqual([Expected], client_event_topics(ClientId, 500))
    after
        ok = emqx:unsubscribe(Filter)
    end.

%% With `rule_engine.ignore_sys_message = false` the client events are published for the rule engine,
%% also with no subscriber.
t_client_event_published_for_rule_engine(_Config) ->
    %% The rule engine application is not started here, so its config root does not exist yet.
    ok = emqx_config:force_put([rule_engine, ignore_sys_message], false),
    ClientId = connect_and_disconnect(),
    ?assertEqual(
        [
            systop(<<"clients/", ClientId/binary, "/connected">>),
            systop(<<"clients/", ClientId/binary, "/disconnected">>)
        ],
        client_event_topics(ClientId, 500)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

connect_and_disconnect() ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    ok = emqtt:disconnect(Client),
    ClientId.

%% The `$SYS` topics that carry `ClientId`, in the order the publish hooks saw them.
client_event_topics(ClientId, Timeout) ->
    receive
        {sys_publish, Topic} ->
            case binary:match(Topic, ClientId) of
                nomatch -> client_event_topics(ClientId, Timeout);
                _ -> [Topic | client_event_topics(ClientId, Timeout)]
            end
    after Timeout ->
        []
    end.

systop(Suffix) ->
    emqx_topic:systop(Suffix).
