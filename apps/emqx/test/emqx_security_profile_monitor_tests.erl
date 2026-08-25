%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_security_profile_monitor_tests).

-include_lib("eunit/include/eunit.hrl").

%% Peers without a cached profile are unknown and never count as legacy.
classify_peers_test() ->
    Peers = [n4, n1, n3, n2, n5],
    Profiles = #{n1 => legacy, n2 => legacy, n4 => hardened, n_gone => legacy},
    ?assertEqual(
        #{
            legacy_nodes => [n1, n2],
            hardened_nodes => [n4],
            unknown_nodes => [n3, n5]
        },
        emqx_security_profile_monitor:classify_peers(Peers, Profiles)
    ).

classify_no_peers_test() ->
    ?assertEqual(
        #{legacy_nodes => [], hardened_nodes => [], unknown_nodes => []},
        emqx_security_profile_monitor:classify_peers([], #{})
    ).
