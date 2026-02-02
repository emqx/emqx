%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_nats_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        t_on_gateway_load_badconf,
        t_on_gateway_update_error,
        t_on_gateway_unload_stop_listeners
    ].

t_on_gateway_load_badconf(_Config) ->
    ok = meck:new(emqx_gateway_utils_conf, [passthrough, no_history]),
    ok = meck:new(emqx_gateway_utils, [passthrough, no_history]),
    ok = meck:expect(
        emqx_gateway_utils_conf,
        to_rt_listener_configs,
        fun(_GwName, _Config, _ModCfg, _Ctx) -> [dummy_listener] end
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        start_listeners,
        fun(_Listeners) ->
            {error, {bad_listener, #{original_listener_config => #{bad => config}}}}
        end
    ),
    Gateway = #{name => nats, config => #{}},
    Ctx = #{},
    try
        ?assertThrow({badconf, _}, emqx_gateway_nats:on_gateway_load(Gateway, Ctx))
    after
        meck:unload([emqx_gateway_utils_conf, emqx_gateway_utils])
    end.

t_on_gateway_update_error(_Config) ->
    ok = meck:new(emqx_gateway_utils_conf, [passthrough, no_history]),
    ok = meck:new(emqx_gateway_utils, [passthrough, no_history]),
    ok = meck:expect(
        emqx_gateway_utils_conf,
        to_rt_listener_configs,
        fun(_GwName, Config, _ModCfg, _Ctx) -> Config end
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        update_gateway_listeners,
        fun(_GwName, _OldListeners, _NewListeners) ->
            {error, update_failed}
        end
    ),
    GwState = #{ctx => #{}},
    Gateway = #{name => nats, config => old_config},
    try
        ?assertEqual(
            {error, update_failed},
            emqx_gateway_nats:on_gateway_update(new_config, Gateway, GwState)
        )
    after
        meck:unload([emqx_gateway_utils_conf, emqx_gateway_utils])
    end.

t_on_gateway_unload_stop_listeners(_Config) ->
    ok = meck:new(emqx_gateway_utils_conf, [passthrough, no_history]),
    ok = meck:new(emqx_gateway_utils, [passthrough, no_history]),
    ok = meck:expect(
        emqx_gateway_utils_conf,
        to_rt_listener_ids,
        fun(_GwName, _Config) -> [listener_id] end
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        stop_listeners,
        fun(Ids) -> {ok, Ids} end
    ),
    Gateway = #{name => nats, config => #{}},
    try
        ?assertEqual({ok, [listener_id]}, emqx_gateway_nats:on_gateway_unload(Gateway, #{}))
    after
        meck:unload([emqx_gateway_utils_conf, emqx_gateway_utils])
    end.
