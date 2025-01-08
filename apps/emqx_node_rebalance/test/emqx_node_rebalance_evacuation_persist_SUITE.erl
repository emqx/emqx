%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_evacuation_persist_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance_evacuation_persist:clear(),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation_persist:clear().

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_save_read(_Config) ->
    DefaultOpts = #{
        server_reference => <<"default_ref">>,
        conn_evict_rate => 2001,
        sess_evict_rate => 2002,
        wait_takeover => 2003
    },

    Opts0 = #{
        server_reference => <<"ref">>,
        conn_evict_rate => 1001,
        sess_evict_rate => 1002,
        wait_takeover => 1003
    },
    ok = emqx_node_rebalance_evacuation_persist:save(Opts0),

    {ok, ReadOpts0} = emqx_node_rebalance_evacuation_persist:read(DefaultOpts),
    ?assertEqual(Opts0, ReadOpts0),

    Opts1 = Opts0#{server_reference => undefined},
    ok = emqx_node_rebalance_evacuation_persist:save(Opts1),

    {ok, ReadOpts1} = emqx_node_rebalance_evacuation_persist:read(DefaultOpts),
    ?assertEqual(Opts1, ReadOpts1).

t_read_default(_Config) ->
    ok = write_evacuation_file(<<"{}">>),

    DefaultOpts = #{
        server_reference => <<"ref">>,
        conn_evict_rate => 1001,
        sess_evict_rate => 1002,
        wait_takeover => 1003
    },

    {ok, ReadOpts} = emqx_node_rebalance_evacuation_persist:read(DefaultOpts),
    ?assertEqual(DefaultOpts, ReadOpts).

t_read_bad_data(_Config) ->
    ok = write_evacuation_file(<<"{bad json">>),

    DefaultOpts = #{
        server_reference => <<"ref">>,
        conn_evict_rate => 1001,
        sess_evict_rate => 1002,
        wait_takeover => 1003
    },

    {ok, ReadOpts} = emqx_node_rebalance_evacuation_persist:read(DefaultOpts),
    ?assertEqual(DefaultOpts, ReadOpts).

t_clear(_Config) ->
    ok = write_evacuation_file(<<"{}">>),

    ?assertMatch(
        {ok, _},
        emqx_node_rebalance_evacuation_persist:read(#{})
    ),

    ok = emqx_node_rebalance_evacuation_persist:clear(),

    ?assertEqual(
        none,
        emqx_node_rebalance_evacuation_persist:read(#{})
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

write_evacuation_file(Json) ->
    ok = filelib:ensure_dir(emqx_node_rebalance_evacuation_persist:evacuation_filepath()),
    ok = file:write_file(
        emqx_node_rebalance_evacuation_persist:evacuation_filepath(),
        Json
    ).
