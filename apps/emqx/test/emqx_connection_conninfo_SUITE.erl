%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_conninfo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        {group, gen_tcp},
        {group, socket}
    ].

groups() ->
    [
        {gen_tcp, [], [t_inconsistent_chan_info]},
        {socket, [], [t_inconsistent_chan_info]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_utils:format("""
                listeners.tcp.default {
                   tcp_backend = ~p
                   tcp_options {
                       recbuf = 10
                       buffer = 10
                       active_n = 1
                   }
                   messages_rate = "1/1s"
                   bytes_rate = "1KB/1s"
                }
            """, [Group])}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [{apps, Apps} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

t_inconsistent_chan_info(_Config) ->
    {ok, C} = emqtt:start_link([{clientid, emqx_guid:to_hexstr(emqx_guid:gen())}]),
    {ok, _} = emqtt:connect(C),
    ConnMod = emqx_cth_broker:connection_info(connmod, C),
    ok = emqtt:disconnect(C),

    ClientIds = [
        ClientId
     || {ClientId, _ChanPid, _ConnState, _ConnInfo, _ClientInfo} <- emqx_utils_stream:consume(
            emqx_cm:all_channels_stream([ConnMod])
        )
    ],

    ?assertNot(lists:member(undefined, ClientIds)).
