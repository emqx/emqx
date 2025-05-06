%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_expire_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

t_disonnect_by_auth_info(_) ->
    _ = process_flag(trap_exit, true),

    _ = meck:new(emqx_access_control, [passthrough, no_history]),
    _ = meck:expect(emqx_access_control, authenticate, fun(_) ->
        {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
    end),

    {ok, C} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    receive
        {disconnected, ?RC_NOT_AUTHORIZED, #{}} -> ok
    after 5000 ->
        ct:fail("Client should be disconnected by timeout")
    end.
