%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_metrics_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(GWNAME, mqttsn).
-define(METRIC, 'ct.test.metrics_name').
-define(CONF_DEFAULT, <<"gateway {}">>).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_gateway, ?CONF_DEFAULT}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Conf)}
    ),
    [{apps, Apps} | Conf].

end_per_suite(Conf) ->
    ok = emqx_cth_suite:stop(?config(apps, Conf)),
    ok.

init_per_testcase(_TestCase, Conf) ->
    %% A previous gateway suite in the same CT run can leave the mqttsn
    %% gateway metrics ETS table populated with real broker stats
    %% (bytes.received, client.connect, etc.). emqx_gateway_metrics:start_link/1
    %% is idempotent on the table -- it does not clear stale rows. Wipe the
    %% table here so each test sees a clean slate.
    clear_metrics_tab(),
    {ok, Pid} = emqx_gateway_metrics:start_link(?GWNAME),
    [{metrics, Pid} | Conf].

end_per_testcase(_TestCase, Conf) ->
    Pid = proplists:get_value(metrics, Conf),
    gen_server:stop(Pid),
    %% Symmetric cleanup -- don't let our test data leak into a subsequent
    %% suite the same way the cascade just leaked into us.
    clear_metrics_tab(),
    Conf.

clear_metrics_tab() ->
    Tab = emqx_gateway_metrics:tabname(?GWNAME),
    case ets:info(Tab, name) of
        undefined -> ok;
        _ -> true = ets:delete_all_objects(Tab)
    end.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_inc_dec(_) ->
    ok = emqx_gateway_metrics:inc(?GWNAME, ?METRIC),
    ok = emqx_gateway_metrics:inc(?GWNAME, ?METRIC),

    ?assertEqual(
        [{?METRIC, 2}],
        emqx_gateway_metrics:lookup(?GWNAME)
    ),

    ok = emqx_gateway_metrics:dec(?GWNAME, ?METRIC),
    ok = emqx_gateway_metrics:dec(?GWNAME, ?METRIC),

    ?assertEqual(
        [{?METRIC, 0}],
        emqx_gateway_metrics:lookup(?GWNAME)
    ).

t_handle_unexpected_msg(Conf) ->
    Pid = proplists:get_value(metrics, Conf),
    _ = Pid ! unexpected_info,
    ok = gen_server:cast(Pid, unexpected_cast),
    ok = gen_server:call(Pid, unexpected_call),
    ok.
