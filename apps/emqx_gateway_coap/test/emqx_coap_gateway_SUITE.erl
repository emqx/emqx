%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_gateway_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_api_namespace(_) ->
    ?assertEqual("gateway_coap", emqx_coap_api:namespace()),
    ok.

t_schema_and_gateway_paths(_) ->
    _ = emqx_coap_schema:namespace(),
    _ = emqx_coap_schema:desc(other),
    ok = meck:new(emqx_gateway_utils, [passthrough, no_history, no_link]),
    ok = meck:new(emqx_gateway_utils_conf, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_gateway_utils_conf,
        to_rt_listener_configs,
        fun(_, _, _, _) -> [#{original_listener_config => #{}}] end
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        start_listeners,
        fun(_) -> {error, {bad_listener, #{original_listener_config => #{}}}} end
    ),
    ?assertThrow(
        {badconf, _},
        emqx_gateway_coap:on_gateway_load(
            #{name => coap, config => #{connection_required => false}},
            #{}
        )
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        update_gateway_listeners,
        fun(_, _, _) -> {error, update_failed} end
    ),
    {error, update_failed} =
        emqx_gateway_coap:on_gateway_update(
            #{connection_required => false},
            #{name => coap, config => #{connection_required => false}},
            #{ctx => #{gwname => coap, cm => self()}}
        ),
    meck:unload(emqx_gateway_utils_conf),
    meck:unload(emqx_gateway_utils),
    ok.
