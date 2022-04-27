-module(emqx_rule_events_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_mod_hook_fun(_) ->
    Funcs = emqx_rule_events:module_info(exports),
    [
        ?assert(lists:keymember(emqx_rule_events:hook_fun(Event), 1, Funcs))
     || Event <- [
            'client.connected',
            'client.disconnected',
            'session.subscribed',
            'session.unsubscribed',
            'message.acked',
            'message.dropped',
            'message.delivered'
        ]
    ].

t_printable_maps(_) ->
    Headers = #{
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 9980},
        sockname => {{127, 0, 0, 1}, 1883}
    },
    ?assertMatch(
        #{
            peerhost := <<"127.0.0.1">>,
            peername := <<"127.0.0.1:9980">>,
            sockname := <<"127.0.0.1:1883">>
        },
        emqx_rule_events:printable_maps(Headers)
    ).
