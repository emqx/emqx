%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ocpp_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    Conf.

end_per_suite(_Conf) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_load_unload(_) ->
    ok.

t_get_env(_) ->
    ok.
