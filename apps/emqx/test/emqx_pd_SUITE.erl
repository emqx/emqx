%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_pd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_update_counter(_) ->
    ?assertEqual(undefined, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(1, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(2, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(3, emqx_pd:get_counter(bytes)),
    ?assertEqual(3, emqx_pd:reset_counter(bytes)),
    ?assertEqual(0, emqx_pd:get_counter(bytes)).
