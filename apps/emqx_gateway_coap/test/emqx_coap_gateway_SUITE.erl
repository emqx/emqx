%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_gateway_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_api_namespace(_) ->
    ?assertEqual("gateway_coap", emqx_coap_api:namespace()),
    ok.

t_schema_and_gateway_paths(_) ->
    {ok, _} = application:ensure_all_started(esockd),
    _ = emqx_coap_schema:namespace(),
    _ = emqx_coap_schema:desc(other),
    Port = emqx_common_test_helpers:select_free_port(udp),
    {ok, Sock} = gen_udp:open(Port, [binary, {active, false}]),
    BadConfig = #{
        connection_required => false,
        listeners => #{udp => #{default => #{bind => {{127, 0, 0, 1}, Port}}}}
    },
    try
        ?assertThrow(
            {badconf, _},
            emqx_gateway_coap:on_gateway_load(
                #{name => coap, config => BadConfig},
                #{}
            )
        ),
        {error, _} =
            emqx_gateway_coap:on_gateway_update(
                BadConfig,
                #{name => coap, config => #{connection_required => false, listeners => #{}}},
                #{ctx => coap_ctx()}
            )
    after
        gen_udp:close(Sock)
    end,
    ok.

coap_ctx() ->
    #{
        gwname => coap,
        cm => self(),
        metrics_tab => emqx_gateway_metrics:tabname(coap)
    }.
