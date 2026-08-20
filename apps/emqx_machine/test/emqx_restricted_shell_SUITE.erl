%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_restricted_shell_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

t_local_allowed(_Config) ->
    LocalProhibited = [halt, q],
    State = undefined,
    lists:foreach(
        fun(LocalFunc) ->
            ?assertEqual({false, State}, emqx_restricted_shell:local_allowed(LocalFunc, [], State))
        end,
        LocalProhibited
    ),
    LocalAllowed = [ls, pwd],
    lists:foreach(
        fun(LocalFunc) ->
            ?assertEqual({true, State}, emqx_restricted_shell:local_allowed(LocalFunc, [], State))
        end,
        LocalAllowed
    ),
    ok.

t_non_local_allowed(_Config) ->
    RemoteProhibited = [{erlang, halt}, {c, q}, {init, stop}, {init, restart}, {init, reboot}],
    State = undefined,
    lists:foreach(
        fun(RemoteFunc) ->
            ?assertEqual(
                {false, State}, emqx_restricted_shell:non_local_allowed(RemoteFunc, [], State)
            )
        end,
        RemoteProhibited
    ),
    RemoteAllowed = [{erlang, date}, {erlang, system_time}],
    lists:foreach(
        fun(RemoteFunc) ->
            ?assertEqual({true, State}, emqx_restricted_shell:local_allowed(RemoteFunc, [], State))
        end,
        RemoteAllowed
    ),
    ok.

t_lock(_Config) ->
    State = undefined,
    emqx_restricted_shell:lock(),
    ?assertEqual({false, State}, emqx_restricted_shell:local_allowed(q, [], State)),
    ?assertEqual({true, State}, emqx_restricted_shell:local_allowed(ls, [], State)),
    ?assertEqual({false, State}, emqx_restricted_shell:non_local_allowed({init, stop}, [], State)),
    ?assertEqual(
        {true, State}, emqx_restricted_shell:non_local_allowed({inet, getifaddrs}, [], State)
    ),
    emqx_restricted_shell:unlock(),
    ?assertEqual({true, State}, emqx_restricted_shell:local_allowed(q, [], State)),
    ?assertEqual({true, State}, emqx_restricted_shell:local_allowed(ls, [], State)),
    ?assertEqual({true, State}, emqx_restricted_shell:non_local_allowed({init, stop}, [], State)),
    ?assertEqual(
        {true, State}, emqx_restricted_shell:non_local_allowed({inet, getifaddrs}, [], State)
    ),
    emqx_restricted_shell:lock(),
    ok.
