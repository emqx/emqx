%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_listeners_tests).

-include_lib("eunit/include/eunit.hrl").

clamp_active_n_test_() ->
    [
        ?_assertEqual(1, emqx_listeners:clamp_active_n(0)),
        ?_assertEqual(1, emqx_listeners:clamp_active_n(-10)),
        ?_assertEqual(1, emqx_listeners:clamp_active_n(1)),
        ?_assertEqual(10, emqx_listeners:clamp_active_n(10)),
        ?_assertEqual(1000, emqx_listeners:clamp_active_n(1000)),
        ?_assertEqual(1000, emqx_listeners:clamp_active_n(1001)),
        ?_assertEqual(1000, emqx_listeners:clamp_active_n(100000))
    ].
