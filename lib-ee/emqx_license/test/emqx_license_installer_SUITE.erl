%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_installer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config);
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_update(_Config) ->
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    Pid0 = spawn_link(fun() ->
                        receive
                            exit -> ok
                        end
                    end),
                    register(installer_test, Pid0),

                    {ok, _} = emqx_license_installer:start_link(
                        installer_test,
                        ?MODULE,
                        10,
                        fun() -> ok end
                    ),

                    {ok, _} = ?block_until(
                        #{?snk_kind := emqx_license_installer_nochange},
                        100
                    ),

                    Pid0 ! exit,

                    {ok, _} = ?block_until(
                        #{?snk_kind := emqx_license_installer_noproc},
                        100
                    ),

                    Pid1 = spawn_link(fun() -> timer:sleep(100) end),
                    register(installer_test, Pid1)
                end,
                #{?snk_kind := emqx_license_installer_called},
                1000
            )
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_license_installer_called, Trace))
        end
    ).

t_unknown_calls(_Config) ->
    ok = gen_server:cast(emqx_license_installer, some_cast),
    some_msg = erlang:send(emqx_license_installer, some_msg),
    ?assertEqual(unknown, gen_server:call(emqx_license_installer, some_request)).
