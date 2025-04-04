%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_locker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

t_start_link(_) ->
    emqx_cm_locker:start_link().

t_trans(_) ->
    ok = emqx_cm_locker:trans(undefined, fun(_) -> ok end),
    ok = emqx_cm_locker:trans(<<"clientid">>, fun(_) -> ok end).

t_lock_unlock(_) ->
    {true, _} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _} = emqx_cm_locker:unlock(<<"clientid">>),
    {true, _} = emqx_cm_locker:unlock(<<"clientid">>).
