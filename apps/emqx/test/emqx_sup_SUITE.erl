%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

t_child(_) ->
    ?assertMatch({error, _}, emqx_sup:start_child(undef, worker)),
    ?assertMatch({error, not_found}, emqx_sup:stop_child(undef)),
    ?assertMatch({error, _}, emqx_sup:start_child(emqx_broker_sup, supervisor)),
    ?assertEqual(ok, emqx_sup:stop_child(emqx_broker_sup)),
    ?assertMatch({ok, _}, emqx_sup:start_child(emqx_broker_sup, supervisor)).
