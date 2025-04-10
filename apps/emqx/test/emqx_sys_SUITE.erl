%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx),
    ok = emqx_logger:set_log_level(emergency),
    Config.

end_per_suite(_Config) ->
    application:unload(emqx),
    ok = emqx_logger:set_log_level(error),
    ok.
