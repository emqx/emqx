%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_channel_SUITE).

-include("emqx_nats.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-record(channel, {
    ctx,
    conninfo,
    clientinfo,
    session,
    clientinfo_override,
    conn_state,
    subscriptions,
    timers,
    transaction
}).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    Conf = #{
        [gateway, nats, server_id] => <<"emqx_nats_gateway">>,
        [gateway, nats, server_name] => <<"emqx_nats_gateway">>,
        [gateway, nats, protocol, max_payload_size] => 1024,
        [gateway, nats, default_heartbeat_interval] => 2000,
        [gateway, nats, heartbeat_wait_timeout] => 1000
    },
    ok = meck:new(emqx_conf, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_conf, get, fun(Path) -> maps:get(Path, Conf) end),
    ok = meck:expect(emqx_conf, get, fun(Path, Default) -> maps:get(Path, Conf, Default) end),
    ok = meck:new(emqx_hooks, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_hooks, run, fun(_Name, _Args) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Name, _Args, Acc) -> Acc end),
    ok = meck:new(emqx_gateway_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_metrics, inc, fun(_GwName, _Name) -> ok end),
    ok = meck:expect(emqx_gateway_metrics, inc, fun(_GwName, _Name, _N) -> ok end),
    Config.

end_per_suite(_Config) ->
    meck:unload([emqx_conf, emqx_hooks, emqx_gateway_metrics]),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

base_ctx() ->
    #{gwname => nats, cm => self()}.

base_conninfo() ->
    #{
        peername => {{127, 0, 0, 1}, 12345},
        sockname => {{127, 0, 0, 1}, 4222},
        conn_mod => emqx_gateway_conn,
        conn_params => #{},
        keepalive => 1000
    }.

base_options() ->
    #{
        ctx => base_ctx(),
        listener => {nats, tcp, default},
        enable_authn => true,
        clientinfo_override => #{}
    }.

make_channel(OptOverrides, ConnOverrides) ->
    ConnInfo = maps:merge(base_conninfo(), ConnOverrides),
    Options = maps:merge(base_options(), OptOverrides),
    emqx_nats_channel:init(ConnInfo, Options).

make_channel_without_listener(OptOverrides, ConnOverrides) ->
    ConnInfo = maps:merge(base_conninfo(), ConnOverrides),
    Options = maps:merge(maps:remove(listener, base_options()), OptOverrides),
    emqx_nats_channel:init(ConnInfo, Options).

sub(SId, Subject, MountedTopic, MaxMsgs) ->
    #{
        sid => SId,
        subject => Subject,
        mounted_topic => MountedTopic,
        max_msgs => MaxMsgs,
        sub_opts => #{}
    }.

connect_frame(Params) ->
    #nats_frame{operation = ?OP_CONNECT, message = Params}.

sub_frame(SId, Subject) ->
    #nats_frame{operation = ?OP_SUB, message = #{sid => SId, subject => Subject}}.

unsub_frame(SId, MaxMsgs) ->
    #nats_frame{operation = ?OP_UNSUB, message = #{sid => SId, max_msgs => MaxMsgs}}.

pub_frame(Subject, Payload, ReplyTo) ->
    #nats_frame{
        operation = ?OP_PUB,
        message = #{subject => Subject, payload => Payload, reply_to => ReplyTo}
    }.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_init_no_listener_and_no_auth(_Config) ->
    Channel = make_channel_without_listener(#{enable_authn => false}, #{}),
    ?assertEqual(anonymous, emqx_nats_channel:info(conn_state, Channel)),
    ClientInfo = emqx_nats_channel:info(clientinfo, Channel),
    ?assertEqual(undefined, maps:get(listener, ClientInfo)).

t_init_with_peercert_sets_dn_cn(_Config) ->
    ok = meck:new(esockd_peercert, [passthrough, no_history]),
    ok = meck:expect(esockd_peercert, subject, fun(_) -> <<"DN">> end),
    ok = meck:expect(esockd_peercert, common_name, fun(_) -> <<"CN">> end),
    try
        Channel = make_channel(#{}, #{peercert => dummy_cert}),
        ClientInfo = emqx_nats_channel:info(clientinfo, Channel),
        ?assertEqual(<<"DN">>, maps:get(dn, ClientInfo)),
        ?assertEqual(<<"CN">>, maps:get(cn, ClientInfo))
    after
        meck:unload(esockd_peercert)
    end.

t_info_frame_tls_options_wss(_Config) ->
    Channel = make_channel(#{listener => {nats, wss, default}}, #{}),
    {ok, [{outgoing, #nats_frame{operation = ?OP_INFO, message = Msg}}], _} =
        emqx_nats_channel:handle_info(after_init, Channel),
    ?assertEqual(true, maps:get(tls_required, Msg)),
    ?assertEqual(true, maps:get(tls_handshake_first, Msg)).

t_set_conn_state(_Config) ->
    Channel = make_channel(#{}, #{}),
    Channel1 = emqx_nats_channel:set_conn_state(disconnected, Channel),
    ?assertEqual(disconnected, emqx_nats_channel:info(conn_state, Channel1)).

t_connect_pipeline_hook_error_with_placeholder_undefined(_Config) ->
    ok = meck:new(emqx_placeholder, [passthrough, no_history]),
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_placeholder, proc_tmpl, fun(_, _, _) -> [undefined] end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Name, _Args, _Acc) -> {error, <<"hook_failed">>} end),
    ok = meck:expect(emqx_gateway_ctx, authenticate, fun(_Ctx, Info) -> {ok, Info} end),
    try
        Channel = make_channel_without_listener(#{}, #{}),
        Frame = connect_frame(#{}),
        ?assertMatch(
            {shutdown, <<"hook_failed">>, _Frame, _Chan},
            emqx_nats_channel:handle_in(Frame, Channel)
        )
    after
        ok = meck:expect(emqx_hooks, run_fold, fun(_Name, _Args, Acc) -> Acc end),
        meck:unload([emqx_placeholder, emqx_gateway_ctx])
    end.

t_schedule_connection_expire_timer(_Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history]),
    ok = meck:expect(emqx_gateway_ctx, authenticate, fun(_Ctx, Info) -> {ok, Info} end),
    ok = meck:expect(emqx_gateway_ctx, connection_expire_interval, fun(_Ctx, _Info) -> 1000 end),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun(_Ctx, _CleanStart, _ClientInfo, _ConnInfo, _SessFun) ->
            {ok, #{session => #{}}}
        end
    ),
    ok = meck:expect(emqx_hooks, run, fun(_Name, _Args) -> ok end),
    try
        Channel = make_channel(#{}, #{}),
        Frame = connect_frame(#{}),
        {ok, _Replies, _Channel1} = emqx_nats_channel:handle_in(Frame, Channel)
    after
        meck:unload(emqx_gateway_ctx)
    end.

t_process_connect_open_session_error(_Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_ctx, authenticate, fun(_Ctx, Info) -> {ok, Info} end),
    ok = meck:expect(
        emqx_gateway_ctx,
        connection_expire_interval,
        fun(_Ctx, _ClientInfo) -> undefined end
    ),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun(_Ctx, _CleanStart, _ClientInfo, _ConnInfo, _SessFun) ->
            {error, <<"open_failed">>}
        end
    ),
    ok = meck:expect(emqx_hooks, run, fun(_Name, _Args) -> ok end),
    try
        Channel = make_channel(#{}, #{}),
        Frame = connect_frame(#{}),
        ?assertMatch(
            {shutdown, failed_to_open_session, _Frame, _Chan},
            emqx_nats_channel:handle_in(Frame, Channel)
        )
    after
        meck:unload(emqx_gateway_ctx)
    end.

t_double_connect_check_no_responders_paths(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Channel = Channel0#channel{conn_state = connected},
    ErrFrame = connect_frame(#{<<"no_responders">> => true, <<"headers">> => false}),
    ?assertMatch(
        {ok, {outgoing, #nats_frame{operation = ?OP_ERR}}, _},
        emqx_nats_channel:handle_in(ErrFrame, Channel)
    ),
    OkFrame = connect_frame(#{<<"no_responders">> => false, <<"headers">> => true}),
    {ok, Replies, _} = emqx_nats_channel:handle_in(OkFrame, Channel),
    ?assert(lists:member({event, updated}, Replies)),
    ?assert(
        lists:any(
            fun
                ({outgoing, #nats_frame{operation = ?OP_OK}}) -> true;
                (_) -> false
            end,
            Replies
        )
    ).

t_subscribe_hook_blocked(_Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_ctx, authorize, fun(_, _, _, _) -> allow end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Name, _Args, _Acc) -> [] end),
    ok = meck:new(emqx_utils, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_utils,
        pipeline,
        fun(_Funs, _Packet, Channel) -> {ok, {<<"1">>, <<"foo">>}, Channel} end
    ),
    try
        Channel0 = make_channel(#{}, #{}),
        Channel = Channel0#channel{conn_state = connected},
        Frame = sub_frame(<<"1">>, <<"foo">>),
        ?assertMatch(
            {ok, {outgoing, #nats_frame{operation = ?OP_ERR}}, _},
            emqx_nats_channel:handle_in(Frame, Channel)
        )
    after
        ok = meck:expect(emqx_hooks, run_fold, fun(_Name, _Args, Acc) -> Acc end),
        meck:unload([emqx_gateway_ctx, emqx_utils])
    end.

t_subscribe_duplicate_sid(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Subs = [sub(<<"1">>, <<"foo">>, <<"foo">>, infinity)],
    Channel = Channel0#channel{conn_state = connected, subscriptions = Subs},
    Frame = sub_frame(<<"1">>, <<"bar">>),
    ?assertMatch(
        {ok, {outgoing, #nats_frame{operation = ?OP_ERR}}, _},
        emqx_nats_channel:handle_in(Frame, Channel)
    ).

t_subscribe_duplicate_topic(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Subs = [sub(<<"1">>, <<"foo">>, <<"foo">>, infinity)],
    Channel = Channel0#channel{conn_state = connected, subscriptions = Subs},
    Frame = sub_frame(<<"2">>, <<"foo">>),
    ?assertMatch(
        {ok, {outgoing, #nats_frame{operation = ?OP_ERR}}, _},
        emqx_nats_channel:handle_in(Frame, Channel)
    ).

t_unsubscribe_unknown_sid(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Channel = Channel0#channel{conn_state = connected, subscriptions = []},
    Frame = unsub_frame(<<"missing">>, 0),
    ?assertMatch(
        {ok, [{outgoing, #nats_frame{operation = ?OP_OK}}], _},
        emqx_nats_channel:handle_in(Frame, Channel)
    ).

t_handle_in_ok_and_unexpected(_Config) ->
    Channel = make_channel(#{}, #{}),
    ?assertMatch({ok, _}, emqx_nats_channel:handle_in(?PACKET(?OP_OK), Channel)),
    ?assertMatch({ok, _}, emqx_nats_channel:handle_in(unexpected, Channel)).

t_handle_in_pong_timer_noop(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    TRef = make_ref(),
    Channel = Channel0#channel{
        conn_state = connected,
        timers = #{keepalive_send_timer => TRef}
    },
    ?assertMatch({ok, _}, emqx_nats_channel:handle_in(?PACKET(?OP_PONG), Channel)).

t_handle_frame_error_idle(_Config) ->
    Channel = make_channel(#{}, #{}),
    Channel1 = Channel#channel{conn_state = idle},
    ?assertMatch({shutdown, badarg, _}, emqx_nats_channel:handle_frame_error(badarg, Channel1)).

t_handle_call_discard(_Config) ->
    Channel = make_channel(#{}, #{}),
    ?assertMatch(
        {shutdown, discarded, ok, #nats_frame{operation = ?OP_ERR}, _},
        emqx_nats_channel:handle_call(discard, self(), Channel)
    ).

t_handle_cast_unexpected(_Config) ->
    Channel = make_channel(#{}, #{}),
    ?assertMatch({ok, _}, emqx_nats_channel:handle_cast(unexpected_cast, Channel)).

t_handle_info_sock_closed_connecting(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Channel = Channel0#channel{conn_state = connecting},
    ?assertMatch(
        {shutdown, closed, _},
        emqx_nats_channel:handle_info({sock_closed, closed}, Channel)
    ).

t_handle_info_sock_closed_disconnected(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Channel = Channel0#channel{conn_state = disconnected},
    ?assertMatch(
        {ok, _},
        emqx_nats_channel:handle_info({sock_closed, closed}, Channel)
    ).

t_handle_info_clean_authz_cache(_Config) ->
    ok = meck:new(emqx_authz_cache, [passthrough, no_history]),
    ok = meck:expect(emqx_authz_cache, empty_authz_cache, fun() -> ok end),
    try
        Channel = make_channel(#{}, #{}),
        ?assertMatch({ok, _}, emqx_nats_channel:handle_info(clean_authz_cache, Channel))
    after
        meck:unload(emqx_authz_cache)
    end.

t_handle_info_unexpected(_Config) ->
    Channel = make_channel(#{}, #{}),
    ?assertMatch({ok, _}, emqx_nats_channel:handle_info(unexpected_info, Channel)).

t_handle_deliver_max_msgs_zero(_Config) ->
    ok = meck:new(emqx_broker, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_broker, unsubscribe, fun(_Topic) -> ok end),
    try
        Channel0 = make_channel(#{}, #{}),
        Subs = [sub(<<"1">>, <<"foo">>, <<"foo">>, 0)],
        Channel = Channel0#channel{subscriptions = Subs},
        Msg = emqx_message:make(<<"cid">>, 0, <<"foo">>, <<"payload">>),
        {ok, [{outgoing, Frames}, {event, updated}], _} =
            emqx_nats_channel:handle_deliver([{deliver, <<"foo">>, Msg}], Channel),
        ?assertEqual([], Frames)
    after
        meck:unload(emqx_broker)
    end.

t_handle_deliver_missing_subid(_Config) ->
    Channel = make_channel(#{}, #{}),
    Msg = emqx_message:make(<<"cid">>, 0, <<"missing">>, <<"payload">>),
    {ok, [{outgoing, Frames}, {event, updated}], _} =
        emqx_nats_channel:handle_deliver([{deliver, <<"missing">>, Msg}], Channel),
    ?assertEqual([], Frames).

t_handle_deliver_reduce_nonmatching(_Config) ->
    ok = meck:new(emqx_broker, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_broker, unsubscribe, fun(_Topic) -> ok end),
    try
        Channel0 = make_channel(#{}, #{}),
        Subs = [
            sub(<<"1">>, <<"foo">>, <<"foo">>, 1),
            sub(<<"2">>, <<"bar">>, <<"bar">>, 5)
        ],
        Channel = Channel0#channel{subscriptions = Subs},
        Msg = emqx_message:make(<<"cid">>, 0, <<"foo">>, <<"payload">>),
        {ok, [{outgoing, Frames}, {event, updated}], _} =
            emqx_nats_channel:handle_deliver([{deliver, <<"foo">>, Msg}], Channel),
        ?assertMatch([#nats_frame{operation = ?OP_MSG} | _], Frames)
    after
        meck:unload(emqx_broker)
    end.

t_handle_deliver_duplicate_sid_unsubscribe(_Config) ->
    ok = meck:new(emqx_broker, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_broker, unsubscribe, fun(_Topic) -> ok end),
    try
        Channel0 = make_channel(#{}, #{}),
        Subs = [
            sub(<<"1">>, <<"foo">>, <<"foo">>, 1),
            sub(<<"1">>, <<"foo">>, <<"foo">>, 1)
        ],
        Channel = Channel0#channel{subscriptions = Subs},
        Msg = emqx_message:make(<<"cid">>, 0, <<"foo">>, <<"payload">>),
        {ok, [{outgoing, _Frames}, {event, updated}], _} =
            emqx_nats_channel:handle_deliver([{deliver, <<"foo">>, Msg}], Channel)
    after
        meck:unload(emqx_broker)
    end.

t_unsub_update_sub_max_msgs(_Config) ->
    Channel0 = make_channel(#{}, #{}),
    Subs = [
        sub(<<"1">>, <<"foo">>, <<"foo">>, infinity),
        sub(<<"2">>, <<"bar">>, <<"bar">>, infinity)
    ],
    Channel = Channel0#channel{subscriptions = Subs, conn_state = connected},
    Frame = unsub_frame(<<"1">>, 2),
    {ok, [{outgoing, #nats_frame{operation = ?OP_OK}}], Channel1} =
        emqx_nats_channel:handle_in(Frame, Channel),
    ?assertEqual(
        true,
        lists:any(
            fun
                (#{sid := <<"2">>}) -> true;
                (_) -> false
            end,
            Channel1#channel.subscriptions
        )
    ),
    [Sub1] = [Sub || Sub = #{sid := <<"1">>} <- Channel1#channel.subscriptions],
    ?assertEqual(2, maps:get(max_msgs, Sub1)).

t_handle_timeout_connection_expire(_Config) ->
    Channel = make_channel(#{}, #{}),
    ?assertMatch(
        {shutdown, expired, _Frame, _},
        emqx_nats_channel:handle_timeout(make_ref(), connection_expire, Channel)
    ).

t_handle_in_pub_no_responders_no_match(_Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history]),
    ok = meck:expect(emqx_gateway_ctx, authorize, fun(_, _, _, _) -> allow end),
    ok = meck:new(emqx_broker, [passthrough, no_history]),
    ok = meck:expect(emqx_broker, publish, fun(_Msg) -> [] end),
    try
        Channel0 = make_channel(#{}, #{}),
        ConnInfo = Channel0#channel.conninfo,
        Channel = Channel0#channel{
            conn_state = connected,
            conninfo = ConnInfo#{conn_params => #{<<"no_responders">> => true}},
            subscriptions = [sub(<<"1">>, <<"foo">>, <<"foo">>, 1)]
        },
        Frame = pub_frame(<<"foo">>, <<"payload">>, <<"reply.subject">>),
        {ok, _Replies, _} = emqx_nats_channel:handle_in(Frame, Channel)
    after
        meck:unload([emqx_gateway_ctx, emqx_broker])
    end.
