%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_config_tests).

-include_lib("eunit/include/eunit.hrl").

link_conf(Server) ->
    #{
        name => <<"remote">>,
        clientid => <<"linkclientid">>,
        server => Server,
        ssl => #{enable => false},
        retry_interval => 15_000,
        max_inflight => 32
    }.

%% `mk_emqtt_options/1` reads the local cluster name from the config.
with_cluster_name(Tests) ->
    {
        setup,
        fun() -> emqx_config:put([cluster, name], emqxcl) end,
        fun(_) -> emqx_config:erase(cluster) end,
        Tests
    }.

mk_emqtt_options_test_() ->
    with_cluster_name([
        ?_assertMatch(
            #{hosts := [{"h1", 1883}]},
            emqx_cluster_link_config:mk_emqtt_options(link_conf(<<"h1">>))
        ),
        ?_assertMatch(
            #{hosts := [{"h1", 1883}, {"h2", 1884}, {"h3", 1885}]},
            emqx_cluster_link_config:mk_emqtt_options(link_conf(<<"h1:1883,h2:1884,h3:1885">>))
        )
    ]).

prefer_host_test_() ->
    with_cluster_name(fun() ->
        Opts = emqx_cluster_link_config:mk_emqtt_options(link_conf(<<"h1:1883,h2:1884,h3:1885">>)),
        Preferred = fun(N) ->
            #{hosts := Hosts} = emqx_cluster_link_config:prefer_host(N, Opts),
            Hosts
        end,
        [
            ?_assertEqual([{"h1", 1883}, {"h2", 1884}, {"h3", 1885}], Preferred(1)),
            ?_assertEqual([{"h2", 1884}, {"h3", 1885}, {"h1", 1883}], Preferred(2)),
            ?_assertEqual([{"h3", 1885}, {"h1", 1883}, {"h2", 1884}], Preferred(3)),
            ?_assertEqual([{"h1", 1883}, {"h2", 1884}, {"h3", 1885}], Preferred(4))
        ]
    end).
