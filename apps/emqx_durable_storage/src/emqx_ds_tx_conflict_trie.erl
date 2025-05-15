%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a data structure for storing
%% information about recent updates. It is used to reject transactions
%% that may potentially read dirty data.
-module(emqx_ds_tx_conflict_trie).

%% API:
-export([topic_filter_to_conflict_domain/1, new/2, push/3, rotate/1, is_dirty/3, print/1]).

-export_type([t/0, conflict_domain/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(conflict_tree, {
    rotate_every :: pos_integer() | infinity,
    min_serial,
    old_max_serial,
    max_serial,
    old_trie = gb_trees:empty(),
    trie = gb_trees:empty()
}).

-opaque t() :: #conflict_tree{}.

-type conflict_domain() :: [binary() | '#'].

%%================================================================================
%% API functions
%%================================================================================

-spec print(t()) -> map().
print(#conflict_tree{min_serial = Min, max_serial = Max, old_trie = Old, trie = Current}) ->
    #{
        range => {Min, Max},
        old => gb_trees:to_list(Old),
        current => gb_trees:to_list(Current)
    }.

%% @doc Translate topic filter to conflict domain. This function
%% replaces all topic levels following a wildcard (+ or #) with #
-spec topic_filter_to_conflict_domain(emqx_ds:topic_filter()) -> conflict_domain().
topic_filter_to_conflict_domain(TF) ->
    tf2cd(TF, []).

%% @doc Create a new conflict tracking trie.
%%
%% @param MinSerial Transaction serial at the time of creation.
%%
%% @param RotateEvery Automatically rotate the trie when the tracked
%% serial interval reaches this value. Atom `infinity' disables
%% automatic rotation.
-spec new(emqx_ds_optimistic_tx:serial(), pos_integer() | infinity) -> t().
new(MinSerial, RotateEvery) when
    RotateEvery > 0;
    RotateEvery =:= infinity
->
    #conflict_tree{
        rotate_every = RotateEvery,
        min_serial = MinSerial,
        max_serial = MinSerial,
        old_max_serial = MinSerial
    }.

%% @doc Add a new conflict domain to the trie.
-spec push(conflict_domain(), emqx_ds_optimistic_tx:serial(), t()) -> t().
push(
    CD,
    Serial,
    S = #conflict_tree{min_serial = MinS, max_serial = MaxS}
) when Serial > MinS, Serial >= MaxS ->
    #conflict_tree{rotate_every = RotateEvery, old_max_serial = OldMax, trie = Trie} = S,
    case Serial - OldMax >= RotateEvery of
        true ->
            push(CD, Serial, rotate(S));
        false ->
            WildcardPrefix = wildcard_prefix(CD),
            S#conflict_tree{
                max_serial = Serial,
                trie = do_push(WildcardPrefix, CD, Serial, Trie)
            }
    end.

%% @doc Return `false' if there are no entries with serial **greater**
%% than `Serial' in the tracked conflict range. Otherwise, return
%% `true'.
-spec is_dirty(conflict_domain(), emqx_ds_optimistic_tx:serial(), t()) -> boolean().
is_dirty(
    _CD,
    Serial,
    #conflict_tree{min_serial = MinS}
) when Serial < MinS ->
    %% Serial is out of the tracked conflict range:
    true;
is_dirty(
    CD,
    Serial,
    #conflict_tree{trie = Trie, old_trie = OldTrie}
) ->
    check_dirty(CD, Serial, Trie) orelse
        check_dirty(CD, Serial, OldTrie).

%% @doc This function is used to reduce size of the trie by removing old
%% conflicts.
%%
%% It moves `current' trie to `old', empties the current trie, and
%% shrinks the tracked interval.
-spec rotate(t()) -> t().
rotate(S = #conflict_tree{old_max_serial = OldMax, max_serial = Max, trie = Trie}) ->
    S#conflict_tree{
        min_serial = OldMax,
        old_max_serial = Max,
        max_serial = Max,
        old_trie = Trie,
        trie = gb_trees:empty()
    }.

%%================================================================================
%% Internal functions
%%================================================================================

tf2cd([], Acc) ->
    lists:reverse(Acc);
tf2cd(['#' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd(['+' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd([Const | Rest], Acc) ->
    tf2cd(Rest, [Const | Acc]).

do_push(false, CD, Serial, Trie) ->
    gb_trees:enter(CD, Serial, Trie);
do_push(WildcardPrefix, CD, Serial, Trie) ->
    gb_trees:enter(CD, Serial, remove_keys_with_prefix(WildcardPrefix, Trie)).

check_dirty(CD, Serial, Trie) ->
    WCPrefixes = wc_prefixes(CD),
    maybe
        %% 1. Verify the exact domain:
        false ?= compare_domain_serial(CD, Serial, Trie),
        %% 2. Verify its wildcard prefixes:
        false ?=
            lists:search(
                fun(WCPrefix) ->
                    compare_domain_serial(WCPrefix, Serial, Trie)
                end,
                WCPrefixes
            ),
        %% 3. If CD is itself a wildcard we need to check all
        %% subdomains in the trie:
        case wildcard_prefix(CD) of
            false ->
                false;
            Prefix ->
                Fun = fun(_Key, KeySerial, Acc) ->
                    Acc orelse (KeySerial > Serial)
                end,
                fold_keys_with_prefix(Fun, Prefix, Trie, false)
        end
    else
        _ -> true
    end.

compare_domain_serial(CD, Serial, Trie) ->
    case gb_trees:lookup(CD, Trie) of
        {value, Val} when Val > Serial ->
            true;
        _ ->
            false
    end.

remove_keys_with_prefix(Prefix, Trie) ->
    Fun = fun(Key, _Val, Acc) ->
        gb_trees:delete(Key, Acc)
    end,
    fold_keys_with_prefix(Fun, Prefix, Trie, Trie).

fold_keys_with_prefix(Fun, Prefix, Trie, Acc) ->
    It = gb_trees:iterator_from(Prefix, Trie),
    do_fold_keys_with_prefix(Fun, Prefix, It, Acc).

do_fold_keys_with_prefix(Fun, Prefix, It0, Acc0) ->
    case gb_trees:next(It0) of
        none ->
            Acc0;
        {Key, Val, It} ->
            case lists:prefix(Prefix, Key) of
                true ->
                    Acc = Fun(Key, Val, Acc0),
                    do_fold_keys_with_prefix(Fun, Prefix, It, Acc);
                false ->
                    Acc0
            end
    end.

wildcard_prefix(L) ->
    wc_prefix(L, []).

wc_prefix([], _Acc) ->
    false;
wc_prefix(['#'], Acc) ->
    lists:reverse(Acc);
wc_prefix([A | L], Acc) ->
    wc_prefix(L, [A | Acc]).

wc_prefixes(CD) ->
    wc_prefixes(CD, [], []).

wc_prefixes(Suffix, PrefixAcc, Acc) ->
    Prefix = lists:reverse(['#' | PrefixAcc]),
    case Suffix of
        ['#'] ->
            Acc;
        [] ->
            [Prefix | Acc];
        [Token | Rest] ->
            wc_prefixes(Rest, [Token | PrefixAcc], [Prefix | Acc])
    end.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

tf2cd_test() ->
    ?assertEqual([], topic_filter_to_conflict_domain([])),
    ?assertEqual(
        [<<1>>, <<2>>],
        topic_filter_to_conflict_domain([<<1>>, <<2>>])
    ),
    ?assertEqual(
        [<<1>>, <<2>>, '#'],
        topic_filter_to_conflict_domain([<<1>>, <<2>>, '+', <<3>>])
    ),
    ?assertEqual(
        [<<1>>, <<2>>, '#'],
        topic_filter_to_conflict_domain([<<1>>, <<2>>, '#'])
    ).

-define(dirty(CD, SERIAL, TRIE),
    ?assertMatch(
        true,
        is_dirty(CD, SERIAL, TRIE)
    )
).

-define(clean(CD, SERIAL, TRIE),
    ?assertMatch(
        false,
        is_dirty(CD, SERIAL, TRIE)
    )
).

%% Test is_dirty on an empty trie:
is_dirty0_test() ->
    Trie = mk_test_trie(0, []),
    ?clean([], 1, Trie),
    ?clean([], 1, rotate(Trie)),

    ?clean(['#'], 1, Trie),
    ?clean(['#'], 1, rotate(Trie)),

    ?clean([<<1>>, <<2>>, '#'], 1, Trie),
    ?clean([<<1>>, <<2>>, '#'], 1, rotate(Trie)).

%% Test is_dirty on a trie without wildcards:
is_dirty1_test() ->
    D1 = {[<<1>>], 2},
    D2 = {[<<1>>, <<2>>], 3},
    D3 = {[<<2>>], 4},
    D4 = {[], 5},
    Trie = mk_test_trie(0, [D1, D2, D3, D4]),
    %% Non-existent domains:
    ?clean([<<3>>], 1, Trie),
    ?clean([<<3>>], 1, rotate(Trie)),
    %% Existing domains:
    ?clean([<<1>>], 2, Trie),
    ?dirty([<<1>>], 1, Trie),
    ?dirty([<<1>>], 1, rotate(Trie)),

    ?dirty([], 1, Trie),
    ?dirty([], 4, rotate(Trie)),
    ?clean([], 5, Trie),
    ?clean([], 5, rotate(Trie)),
    %% Existing wildcard domains:
    ?dirty([<<1>>, '#'], 1, Trie),
    ?dirty([<<1>>, '#'], 1, rotate(Trie)),
    ?dirty([<<1>>, '#'], 2, Trie),
    ?dirty([<<1>>, '#'], 2, rotate(Trie)),

    ?clean([<<1>>, '#'], 3, Trie),
    ?clean([<<1>>, '#'], 3, rotate(Trie)),
    %% All domains:
    ?dirty(['#'], 1, Trie),
    ?dirty(['#'], 1, rotate(Trie)),
    ?dirty(['#'], 4, Trie),
    ?dirty(['#'], 4, rotate(Trie)),
    ?clean(['#'], 5, Trie),
    ?clean(['#'], 5, rotate(Trie)).

%% Test is_dirty on a trie with wildcards:
is_dirty2_test() ->
    D1 = {[<<1>>], 2},
    D2 = {[<<1>>, <<2>>, '#'], 3},
    D3 = {[<<2>>, '#'], 4},
    Trie = mk_test_trie(0, [D1, D2, D3]),
    %% Non-existent domains:
    ?clean([<<3>>], 1, Trie),
    ?clean([<<3>>], 1, rotate(Trie)),
    %% Existing domains:
    ?clean([<<1>>], 2, Trie),
    ?clean([<<1>>], 2, rotate(Trie)),
    ?dirty([<<1>>, <<2>>], 2, Trie),
    ?dirty([<<1>>, <<2>>], 2, rotate(Trie)),

    ?clean([<<1>>, <<2>>], 3, Trie),
    ?dirty([<<1>>, <<2>>, <<3>>], 2, Trie),
    ?clean([<<1>>, <<2>>, <<3>>], 3, Trie),
    %% Existing wildcard domains:
    ?dirty([<<1>>, <<2>>, '#'], 1, Trie),
    ?dirty([<<1>>, <<2>>, '#'], 1, rotate(Trie)),

    ?dirty([<<1>>, <<2>>, '#'], 2, Trie),
    ?clean([<<1>>, <<2>>, '#'], 3, Trie),
    ?clean([<<1>>, <<2>>, '#'], 3, rotate(Trie)),

    ?dirty([<<1>>, <<2>>, <<3>>, '#'], 2, Trie),
    ?clean([<<1>>, <<2>>, <<3>>, '#'], 3, Trie),

    %% All domains:
    ?dirty(['#'], 1, Trie),
    ?dirty(['#'], 1, rotate(Trie)),
    ?dirty(['#'], 3, Trie),
    ?dirty(['#'], 3, rotate(Trie)),
    ?clean(['#'], 4, Trie),
    ?clean(['#'], 4, rotate(Trie)).

mk_test_trie(Min, L) ->
    lists:foldl(
        fun({Dom, Serial}, Acc) ->
            push(Dom, Serial, Acc)
        end,
        new(Min, infinity),
        L
    ).

wc_prefixes_test() ->
    ?assertEqual(
        [['#']],
        wc_prefixes([])
    ),
    ?assertEqual(
        [],
        wc_prefixes(['#'])
    ),
    ?assertEqual(
        [
            [<<>>, '#'],
            ['#']
        ],
        wc_prefixes([<<>>])
    ),
    ?assertEqual(
        [
            ['#']
        ],
        wc_prefixes([<<>>, '#'])
    ),
    ?assertEqual(
        [
            [<<1>>, <<2>>, <<3>>, '#'],
            [<<1>>, <<2>>, '#'],
            [<<1>>, '#'],
            ['#']
        ],
        wc_prefixes([<<1>>, <<2>>, <<3>>])
    ),
    ?assertEqual(
        [
            [<<1>>, <<2>>, '#'],
            [<<1>>, '#'],
            ['#']
        ],
        wc_prefixes([<<1>>, <<2>>, <<3>>, '#'])
    ).

push_test() ->
    T0 = new(0, infinity),
    T1 = push([<<1>>], 1, T0),
    T2 = push([<<1>>, <<1>>], 1, T1),
    T3 = push([<<1>>, <<2>>], 1, T2),
    T4 = push([<<1>>, <<3>>, '#'], 1, T3),
    T5 = push([<<2>>], 2, T4),
    T6 = push([<<2>>, <<1>>], 2, T5),
    T7 = push([<<2>>, <<2>>], 2, T6),
    T8 = push([<<2>>, <<3>>], 2, T7),
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<1>>, <<1>>], 1},
            {[<<1>>, <<2>>], 1},
            {[<<1>>, <<3>>, '#'], 1},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T8#conflict_tree.trie)
    ),
    %% Overwrite some key:
    T9 = push([<<1>>, <<3>>, '#'], 3, T8),
    T10 = push([<<1>>, <<2>>], 3, T9),
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<1>>, <<1>>], 1},
            {[<<1>>, <<2>>], 3},
            {[<<1>>, <<3>>, '#'], 3},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T10#conflict_tree.trie)
    ),
    %% Insert wildcard:
    T11 = push([<<1>>, '#'], 4, T10),
    ?assertEqual(
        [
            {[<<1>>, '#'], 4},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T11#conflict_tree.trie)
    ),
    ok.

rotate_test() ->
    T0 = new(0, 3),
    T1 = push([<<1>>], 1, T0),
    T2 = push([<<2>>], 2, T1),
    ?assertEqual(
        [],
        gb_trees:to_list(T2#conflict_tree.old_trie)
    ),
    %% Push an item with serial that should rotate the tree:
    T3 = push([<<3>>], 3, T2),
    %% Old items were moved to the old trie:
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<2>>], 2}
        ],
        gb_trees:to_list(T3#conflict_tree.old_trie)
    ),
    %% New item has been added to the current trie:
    ?assertEqual(
        [
            {[<<3>>], 3}
        ],
        gb_trees:to_list(T3#conflict_tree.trie)
    ),
    ?assertEqual(0, T3#conflict_tree.min_serial),
    ?assertEqual(2, T3#conflict_tree.old_max_serial),
    ?assertEqual(3, T3#conflict_tree.max_serial),
    %% Push an item to the same "generation":
    T4 = push([<<4>>], 4, T3),
    %% Push an item that will rotate the tree again:
    T5 = push([<<5>>], 6, T4),
    ?assertEqual(
        [
            {[<<3>>], 3},
            {[<<4>>], 4}
        ],
        gb_trees:to_list(T5#conflict_tree.old_trie)
    ),
    ?assertEqual(
        [
            {[<<5>>], 6}
        ],
        gb_trees:to_list(T5#conflict_tree.trie)
    ),
    ?assertEqual(2, T5#conflict_tree.min_serial),
    ?assertEqual(4, T5#conflict_tree.old_max_serial),
    ?assertEqual(6, T5#conflict_tree.max_serial),
    ok.

-endif.
