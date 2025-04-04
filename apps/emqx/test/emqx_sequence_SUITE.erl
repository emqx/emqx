%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sequence_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-import(
    emqx_sequence,
    [
        nextval/2,
        currval/2,
        reclaim/2
    ]
).

all() -> emqx_common_test_helpers:all(?MODULE).

% t_currval(_) ->
%     error('TODO').

% t_delete(_) ->
%     error('TODO').

% t_create(_) ->
%     error('TODO').

% t_reclaim(_) ->
%     error('TODO').

% t_nextval(_) ->
%     error('TODO').

t_generate(_) ->
    ok = emqx_sequence:create(seqtab),
    ?assertEqual(0, currval(seqtab, key)),
    ?assertEqual(1, nextval(seqtab, key)),
    ?assertEqual(1, currval(seqtab, key)),
    ?assertEqual(2, nextval(seqtab, key)),
    ?assertEqual(2, currval(seqtab, key)),
    ?assertEqual(3, nextval(seqtab, key)),
    ?assertEqual(2, reclaim(seqtab, key)),
    ?assertEqual(1, reclaim(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(1, nextval(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(false, ets:member(seqtab, key)),
    ?assert(emqx_sequence:delete(seqtab)),
    ?assertNot(emqx_sequence:delete(seqtab)).
