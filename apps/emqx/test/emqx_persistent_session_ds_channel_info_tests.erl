%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_channel_info_tests).

-include_lib("eunit/include/eunit.hrl").

%% The record keeps `recv_cnt` and `send_cnt` for compatibility, with the
%% values of the packet counters.
cnt_from_pkt_test() ->
    Bin = emqx_persistent_session_ds_channel_info:encode(#{
        stats => [{recv_cnt, 999}, {recv_pkt, 5}, {send_cnt, 999}, {send_pkt, 7}],
        chan_info => #{
            conninfo => #{proto_ver => 5, connected_at => 1},
            clientinfo => #{protocol => mqtt, listener => 'tcp:default'}
        },
        disconnected_at => 2,
        last_connected_to => node()
    }),
    {_, _, Stats} = emqx_persistent_session_ds_channel_info:enrich(Bin, #{
        id => <<"c1">>,
        subscriptions => #{},
        streams => #{},
        metadata => #{},
        awaiting_rel => #{}
    }),
    ?assertEqual(5, proplists:get_value(recv_cnt, Stats)),
    ?assertEqual(5, proplists:get_value(recv_pkt, Stats)),
    ?assertEqual(7, proplists:get_value(send_cnt, Stats)),
    ?assertEqual(7, proplists:get_value(send_pkt, Stats)).
