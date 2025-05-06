%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_boot_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = application:load(emqx),
    Config.

end_per_suite(_) ->
    ok = application:unload(emqx).

t_is_enabled(_) ->
    try
        ok = application:set_env(emqx, boot_modules, all),
        ?assert(emqx_boot:is_enabled(broker)),
        ?assert(emqx_boot:is_enabled(listeners)),
        ok = application:set_env(emqx, boot_modules, [broker]),
        ?assert(emqx_boot:is_enabled(broker)),
        ?assertNot(emqx_boot:is_enabled(listeners)),
        ok = application:set_env(emqx, boot_modules, [broker, listeners]),
        ?assert(emqx_boot:is_enabled(broker)),
        ?assert(emqx_boot:is_enabled(listeners))
    after
        application:set_env(emqx, boot_modules, all)
    end.
