%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_oracle_tests).

-include_lib("eunit/include/eunit.hrl").

sql_has_parse_side_effect_test_() ->
    [
        ?_assert(emqx_oracle:sql_has_parse_side_effect(<<"CREATE TABLE t(id NUMBER)">>)),
        ?_assert(emqx_oracle:sql_has_parse_side_effect(<<"  /* c */ DROP TABLE t">>)),
        ?_assert(emqx_oracle:sql_has_parse_side_effect(<<"-- c\nROLLBACK">>)),
        ?_assertNot(emqx_oracle:sql_has_parse_side_effect(<<"INSERT INTO t(id) VALUES (:1)">>)),
        ?_assertNot(emqx_oracle:sql_has_parse_side_effect(<<" /* c */ SELECT * FROM t">>))
    ].
