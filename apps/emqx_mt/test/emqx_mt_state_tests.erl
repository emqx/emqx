%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_state_tests).

-include_lib("eunit/include/eunit.hrl").

%% The `emqx_mt' application is not running in these tests, so its tables do not
%% exist.  Callers from other applications (e.g. the Prometheus collector) must not
%% crash in this situation.

fold_known_nss_no_table_test() ->
    ?assertEqual(acc, emqx_mt_state:fold_known_nss(fun(_Ns, _Acc) -> other end, acc)).

is_known_ns_no_table_test() ->
    ?assertNot(emqx_mt_state:is_known_ns(<<"some_namespace">>)).
