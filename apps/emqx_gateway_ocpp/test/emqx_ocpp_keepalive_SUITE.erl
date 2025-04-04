%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ocpp_keepalive_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_check(_) ->
    Keepalive = emqx_ocpp_keepalive:init(60),
    ?assertEqual(60, emqx_ocpp_keepalive:info(interval, Keepalive)),
    ?assertEqual(0, emqx_ocpp_keepalive:info(statval, Keepalive)),
    ?assertEqual(0, emqx_ocpp_keepalive:info(repeat, Keepalive)),
    ?assertEqual(1, emqx_ocpp_keepalive:info(max_repeat, Keepalive)),
    Info = emqx_ocpp_keepalive:info(Keepalive),
    ?assertEqual(
        #{
            interval => 60,
            statval => 0,
            repeat => 0,
            max_repeat => 1
        },
        Info
    ),
    {ok, Keepalive1} = emqx_ocpp_keepalive:check(1, Keepalive),
    ?assertEqual(1, emqx_ocpp_keepalive:info(statval, Keepalive1)),
    ?assertEqual(0, emqx_ocpp_keepalive:info(repeat, Keepalive1)),
    {ok, Keepalive2} = emqx_ocpp_keepalive:check(1, Keepalive1),
    ?assertEqual(1, emqx_ocpp_keepalive:info(statval, Keepalive2)),
    ?assertEqual(1, emqx_ocpp_keepalive:info(repeat, Keepalive2)),
    ?assertEqual({error, timeout}, emqx_ocpp_keepalive:check(1, Keepalive2)).

t_check_max_repeat(_) ->
    Keepalive = emqx_ocpp_keepalive:init(60, 2),
    {ok, Keepalive1} = emqx_ocpp_keepalive:check(1, Keepalive),
    ?assertEqual(1, emqx_ocpp_keepalive:info(statval, Keepalive1)),
    ?assertEqual(0, emqx_ocpp_keepalive:info(repeat, Keepalive1)),
    {ok, Keepalive2} = emqx_ocpp_keepalive:check(1, Keepalive1),
    ?assertEqual(1, emqx_ocpp_keepalive:info(statval, Keepalive2)),
    ?assertEqual(1, emqx_ocpp_keepalive:info(repeat, Keepalive2)),
    {ok, Keepalive3} = emqx_ocpp_keepalive:check(1, Keepalive2),
    ?assertEqual(1, emqx_ocpp_keepalive:info(statval, Keepalive3)),
    ?assertEqual(2, emqx_ocpp_keepalive:info(repeat, Keepalive3)),
    ?assertEqual({error, timeout}, emqx_ocpp_keepalive:check(1, Keepalive3)).
