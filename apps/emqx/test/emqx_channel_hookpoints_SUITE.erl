%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_channel_hookpoints_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_delayed_puback({init, Config}) ->
    emqx_hooks:put('message.puback', {?MODULE, on_message_puback, []}, ?HP_LOWEST),
    Config;
t_delayed_puback({'end', _Config}) ->
    emqx_hooks:del('message.puback', {?MODULE, on_message_puback});
t_delayed_puback(_Config) ->
    {ok, ConnPid} = emqtt:start_link([{clientid, <<"clientid">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, #{reason_code := ?RC_UNSPECIFIED_ERROR}} = emqtt:publish(
        ConnPid, <<"topic">>, <<"hello">>, 1
    ),
    emqtt:disconnect(ConnPid).

t_deliver_via_handle_info({init, Config}) ->
    emqx_hooks:put('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    emqx_hooks:put('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST),
    Config;
t_deliver_via_handle_info({'end', _Config}) ->
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info});
t_deliver_via_handle_info(_Config) ->
    {ok, ConnPid} = emqtt:start_link([{clientid, <<"clientid">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, _, _} = emqtt:subscribe(ConnPid, <<"$test/topic">>, 1),
    receive
        {publish, #{payload := <<"test message">>, topic := <<"topic">>}} ->
            ok;
        Message ->
            ct:fail("unexpected message: ~p", [Message])
    after 1000 ->
        ct:fail("no publish received")
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

on_message_puback(PacketId, _Msg, PubRes, _RC) ->
    erlang:send(self(), {puback, PacketId, PubRes, ?RC_UNSPECIFIED_ERROR}),
    {stop, undefined}.

on_session_subscribed(#{clientid := _ClientId}, <<"$test/", Topic/binary>>, _SubOpts) ->
    _ = erlang:send_after(0, self(), {test_message, Topic}),
    ok;
on_session_subscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

on_client_handle_info(#{clientid := ClientId}, {test_message, Topic}, #{deliver := Delivers} = Acc) ->
    Msg = emqx_message:make(ClientId, Topic, <<"test message">>),
    Deliver = {deliver, Topic, Msg},
    _ = erlang:send_after(1000, self(), {test_message, Topic}),
    {ok, Acc#{deliver => [Deliver | Delivers]}};
on_client_handle_info(_ClientInfo, _Info, Acc) ->
    {ok, Acc}.
