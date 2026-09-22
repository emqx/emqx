%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_config_tests).

-include_lib("eunit/include/eunit.hrl").

%% Link configuration for `mk_emqtt_options/1'.
link_conf(Server) ->
    #{
        name => <<"remote">>,
        clientid => <<"linkclientid">>,
        server => Server,
        ssl => #{enable => false},
        retry_interval => 15_000,
        max_inflight => 32
    }.

%% `mk_emqtt_options/1' reads the local cluster name from the config (the default of
%% `maps:get/3' is evaluated even when the link configuration carries a `clientid').
with_cluster_name(Tests) ->
    {
        setup,
        fun() -> emqx_config:put([cluster, name], emqxcl) end,
        fun(_) -> emqx_config:erase(cluster) end,
        Tests
    }.

mk_emqtt_options_test_() ->
    with_cluster_name([
        %% The message forwarding health check pings the peer, which keeps the socket's
        %% send counter moving and would make `emqtt' skip its own keepalive PINGREQ
        %% (`should_ping/1'), losing the dead connection detection it drives.
        ?_assertMatch(
            #{force_ping := true},
            emqx_cluster_link_config:mk_emqtt_options(link_conf(<<"h1">>))
        )
    ]).
