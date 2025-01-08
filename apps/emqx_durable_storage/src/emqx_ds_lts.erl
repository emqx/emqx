%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_ds_lts).

%% API:
-export([
    trie_create/1, trie_create/0,
    destroy/1,
    trie_dump/2,
    trie_restore/2,
    trie_update/2,
    trie_copy_learned_paths/2,
    topic_key/3,
    match_topics/2,
    lookup_topic_key/2,
    reverse_lookup/2,
    info/2,
    info/1,

    updated_topics/2,

    threshold_fun/1,

    compress_topic/3,
    decompress_topic/2
]).

%% Debug:
-export([trie_next/3, trie_insert/3, dump_to_dot/2]).

-export_type([
    options/0,
    level/0,
    static_key/0,
    varying/0,
    trie/0,
    dump/0,
    msg_storage_key/0,
    learned_structure/0,
    threshold_spec/0,
    threshold_fun/0
]).

-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-elvis([{elvis_style, variable_naming_convention, disable}]).
-elvis([{elvis_style, dont_repeat_yourself, disable}]).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% End Of Topic
-define(EOT, []).
-define(PLUS, '+').

-type level() :: binary() | ''.

-type edge() :: level() | ?EOT | ?PLUS.

%% Fixed size binary or integer, depending on the options:
-type static_key() :: non_neg_integer() | binary().

%% Trie roots:
-define(PREFIX, prefix).
-define(PREFIX_SPECIAL, special).
%% Special prefix root for reverse lookups:
-define(rlookup, rlookup).
-define(rlookup(STATIC), {?rlookup, STATIC}).

-type state() :: static_key() | ?PREFIX | ?PREFIX_SPECIAL.

-type varying() :: [level() | ?PLUS].

-type msg_storage_key() :: {static_key(), varying()}.

-type threshold_spec() ::
    %% Simple spec that maps level (depth) to a threshold.
    %% For example, `{simple, {inf, 20}}` means that 0th level has infinite
    %% threshold while all other levels' threshold is 20.
    {simple, tuple()}.

-type threshold_fun() :: fun((non_neg_integer()) -> non_neg_integer()).

-type persist_callback() :: fun((_Key, _Val) -> ok).

-type learned_structure() :: [level() | ?PLUS, ...].

-type options() ::
    #{
        persist_callback => persist_callback(),
        %% If set, static key is an integer that fits in a given nubmer of bits:
        static_key_bits => pos_integer(),
        %% If set, static key is a _binary_ of a given length:
        static_key_bytes => pos_integer(),
        reverse_lookups => boolean()
    }.

-type dump() :: [{_Key, _Val}].

-record(trie, {
    persist :: persist_callback(),
    is_binary_key :: boolean(),
    static_key_size :: pos_integer(),
    trie :: ets:tid(),
    stats :: ets:tid(),
    rlookups = false :: boolean()
}).

-opaque trie() :: #trie{}.

-record(trans, {key, next}).

-type trans() ::
    #trans{
        key :: {state(), edge()},
        next :: state()
    }
    | #trans{
        key :: {?rlookup, static_key()},
        next :: [level() | ?PLUS]
    }.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Create an empty trie
-spec trie_create(options()) -> trie().
trie_create(UserOpts) ->
    Persist = maps:get(
        persist_callback,
        UserOpts,
        fun(_, _) -> ok end
    ),
    Rlookups = maps:get(reverse_lookups, UserOpts, false),
    IsBinaryKey =
        case UserOpts of
            #{static_key_bits := StaticKeySize} ->
                false;
            #{static_key_bytes := StaticKeySize} ->
                true;
            _ ->
                StaticKeySize = 16,
                true
        end,
    Trie = ets:new(trie, [{keypos, #trans.key}, set, public]),
    Stats = ets:new(stats, [{keypos, 1}, set, public]),
    #trie{
        persist = Persist,
        is_binary_key = IsBinaryKey,
        static_key_size = StaticKeySize,
        trie = Trie,
        stats = Stats,
        rlookups = Rlookups
    }.

-spec trie_create() -> trie().
trie_create() ->
    trie_create(#{}).

-spec destroy(trie()) -> ok.
destroy(#trie{trie = Trie, stats = Stats}) ->
    catch ets:delete(Trie),
    catch ets:delete(Stats),
    ok.

%% @doc Restore trie from a dump
-spec trie_restore(options(), dump()) -> trie().
trie_restore(Options, Dump) ->
    trie_update(trie_create(Options), Dump).

%% @doc Update a trie with a dump of operations (used for replication)
-spec trie_update(trie(), dump()) -> trie().
trie_update(Trie, Dump) ->
    lists:foreach(
        fun({{StateFrom, Token}, StateTo}) ->
            trie_insert(Trie, StateFrom, Token, StateTo)
        end,
        Dump
    ),
    Trie.

-spec trie_dump(trie(), _Filter :: all | wildcard) -> dump().
trie_dump(Trie, Filter) ->
    case Filter of
        all ->
            Fun = fun(_) -> true end;
        wildcard ->
            Fun = fun(L) -> lists:member(?PLUS, L) end
    end,
    Paths = lists:filter(
        fun(Path) ->
            Fun(tokens_of_path(Path))
        end,
        paths(Trie)
    ),
    RlookupIdx = lists:filter(
        fun({_, Tokens}) ->
            Fun(Tokens)
        end,
        all_emanating(Trie, ?rlookup)
    ),
    lists:flatten([Paths, RlookupIdx]).

-spec trie_copy_learned_paths(trie(), trie()) -> trie().
trie_copy_learned_paths(OldTrie, NewTrie) ->
    lists:foreach(
        fun({{StateFrom, Token}, StateTo}) ->
            trie_insert(NewTrie, StateFrom, Token, StateTo)
        end,
        trie_dump(OldTrie, wildcard)
    ),
    NewTrie.

%% @doc Lookup the topic key. Create a new one, if not found.
-spec topic_key(trie(), threshold_fun(), [level()]) -> msg_storage_key().
topic_key(Trie, ThresholdFun, [<<"$", _/bytes>> | _] = Tokens) ->
    %% [MQTT-4.7.2-1]
    %% Put any topic starting with `$` into a separate _special_ root.
    %% Using a special root only when the topic and the filter start with $<X>
    %% prevents special topics from matching with + or # pattern, but not with
    %% $<X>/+ or $<X>/# pattern. See also `match_topics/2`.
    do_topic_key(Trie, ThresholdFun, 0, ?PREFIX_SPECIAL, Tokens, [], []);
topic_key(Trie, ThresholdFun, Tokens) ->
    do_topic_key(Trie, ThresholdFun, 0, ?PREFIX, Tokens, [], []).

%% @doc Return an exisiting topic key if it exists.
-spec lookup_topic_key(trie(), [level()]) -> {ok, msg_storage_key()} | undefined.
lookup_topic_key(Trie, [<<"$", _/bytes>> | _] = Tokens) ->
    %% [MQTT-4.7.2-1]
    %% See also `match_topics/2`.
    do_lookup_topic_key(Trie, ?PREFIX_SPECIAL, Tokens, []);
lookup_topic_key(Trie, Tokens) ->
    do_lookup_topic_key(Trie, ?PREFIX, Tokens, []).

%% @doc Return list of keys of topics that match a given topic filter
-spec match_topics(trie(), [level() | '+' | '#']) ->
    [msg_storage_key()].
match_topics(Trie, [<<"$", _/bytes>> | _] = TopicFilter) ->
    %% [MQTT-4.7.2-1]
    %% Any topics starting with `$` should belong to a separate _special_ root.
    %% Using a special root only when the topic and the filter start with $<X>
    %% prevents special topics from matching with + or # pattern, but not with
    %% $<X>/+ or $<X>/# pattern.
    do_match_topics(Trie, ?PREFIX_SPECIAL, [], TopicFilter);
match_topics(Trie, TopicFilter) ->
    do_match_topics(Trie, ?PREFIX, [], TopicFilter).

%% @doc Dump trie to graphviz format for debugging
-spec dump_to_dot(trie(), file:filename()) -> ok.
dump_to_dot(#trie{trie = Trie, stats = Stats}, Filename) ->
    L = ets:tab2list(Trie),
    {Nodes0, Edges} =
        lists:foldl(
            fun(#trans{key = {From, Label}, next = To}, {AccN, AccEdge}) ->
                Edge = {From, To, Label},
                {[From, To] ++ AccN, [Edge | AccEdge]}
            end,
            {[], []},
            L
        ),
    Nodes =
        lists:map(
            fun(Node) ->
                case ets:lookup(Stats, Node) of
                    [{_, NChildren}] -> ok;
                    [] -> NChildren = 0
                end,
                {Node, NChildren}
            end,
            lists:usort(Nodes0)
        ),
    {ok, FD} = file:open(Filename, [write]),
    Print = fun
        (?PREFIX) -> "prefix";
        (Bin) when is_binary(Bin) -> Bin;
        (NodeId) when is_integer(NodeId) -> integer_to_binary(NodeId, 16)
    end,
    io:format(FD, "digraph {~n", []),
    lists:foreach(
        fun({Node, NChildren}) ->
            Id = Print(Node),
            io:format(FD, "  \"~s\" [label=\"~s : ~p\"];~n", [Id, Id, NChildren])
        end,
        Nodes
    ),
    lists:foreach(
        fun({From, To, Label}) ->
            io:format(FD, "  \"~s\" -> \"~s\" [label=\"~s\"];~n", [Print(From), Print(To), Label])
        end,
        Edges
    ),
    io:format(FD, "}~n", []),
    file:close(FD).

-spec reverse_lookup(trie(), static_key()) -> {ok, learned_structure()} | undefined.
reverse_lookup(#trie{rlookups = false}, _) ->
    error({badarg, reverse_lookups_disabled});
reverse_lookup(#trie{trie = Trie}, StaticKey) ->
    case ets:lookup(Trie, ?rlookup(StaticKey)) of
        [#trans{next = Next}] ->
            {ok, Next};
        [] ->
            undefined
    end.

%% @doc Get information about the trie.
%%
%% Note: `reverse_lookups' must be enabled to get the number of
%% topics.
-spec info(trie(), size | topics) -> _.
info(#trie{rlookups = true, stats = Stats}, topics) ->
    case ets:lookup(Stats, ?rlookup) of
        [{_, N}] -> N;
        [] -> 0
    end;
info(#trie{}, topics) ->
    undefined;
info(#trie{trie = T}, size) ->
    ets:info(T, size).

%% @doc Return size of the trie
-spec info(trie()) -> proplists:proplist().
info(Trie) ->
    [
        {size, info(Trie, size)},
        {topics, info(Trie, topics)}
    ].

-spec threshold_fun(threshold_spec()) -> threshold_fun().
threshold_fun({simple, Thresholds}) ->
    S = tuple_size(Thresholds),
    fun(Depth) ->
        element(min(Depth + 1, S), Thresholds)
    end.

%%%%%%%% Topic compression %%%%%%%%%%

%% @doc Given topic structure for the static LTS index (as returned by
%% `reverse_lookup'), compress a topic filter to exclude static
%% levels:
-spec compress_topic(static_key(), learned_structure(), emqx_ds:topic_filter()) ->
    [emqx_ds_lts:level() | '+'].
compress_topic(StaticKey, TopicStructure, TopicFilter) ->
    compress_topic(StaticKey, TopicStructure, TopicFilter, []).

%% @doc Given topic structure and a compressed topic filter, return
%% the original* topic filter.
%%
%% * '#' will be replaced with '+'s
-spec decompress_topic(learned_structure(), [level() | '+']) ->
    emqx_ds:topic_filter().
decompress_topic(TopicStructure, Topic) ->
    decompress_topic(TopicStructure, Topic, []).

-spec updated_topics(trie(), dump()) -> [emqx_ds:topic_filter()].
updated_topics(#trie{rlookups = true}, Dump) ->
    lists:filtermap(
        fun
            ({{?rlookup, _StaticIdx}, UpdatedTF}) ->
                {true, UpdatedTF};
            ({{_StateFrom, _Token}, _StateTo}) ->
                false
        end,
        Dump
    ).

%%================================================================================
%% Internal exports
%%================================================================================

-spec trie_next(trie(), state(), level() | ?EOT) -> {Wildcard, state()} | undefined when
    Wildcard :: boolean().
trie_next(#trie{trie = Trie}, State, ?EOT) ->
    case ets:lookup(Trie, {State, ?EOT}) of
        [#trans{next = Next}] -> {false, Next};
        [] -> undefined
    end;
trie_next(#trie{trie = Trie}, State, Token) ->
    %% NOTE: it's crucial to return the original (non-wildcard) index
    %% for the topic, if found. Otherwise messages from the same topic
    %% will end up in different streams, once the wildcard is learned,
    %% and their replay order will become undefined:
    case ets:lookup(Trie, {State, Token}) of
        [#trans{next = Next}] ->
            {false, Next};
        [] ->
            case ets:lookup(Trie, {State, ?PLUS}) of
                [#trans{next = Next}] -> {true, Next};
                [] -> undefined
            end
    end.

-spec trie_insert(trie(), state(), edge()) -> {Updated, state()} when
    NChildren :: non_neg_integer(),
    Updated :: false | NChildren.
trie_insert(Trie, State, Token) ->
    NextState = get_id_for_key(Trie, State, Token),
    trie_insert(Trie, State, Token, NextState).

%%================================================================================
%% Internal functions
%%================================================================================

-spec trie_insert
    (trie(), state(), edge(), state()) -> {Updated, state()} when
        NChildren :: non_neg_integer(),
        Updated :: false | NChildren;
    (trie(), ?rlookup, static_key(), [level() | '+']) ->
        {false | non_neg_integer(), state()}.
trie_insert(#trie{trie = Trie, stats = Stats, persist = Persist}, State, Token, NewState) ->
    Key = {State, Token},
    Rec = #trans{
        key = Key,
        next = NewState
    },
    case ets_insert_new(Trie, Rec) of
        true ->
            ok = Persist(Key, NewState),
            Inc =
                case Token of
                    ?EOT -> 0;
                    ?PLUS -> 0;
                    _ -> 1
                end,
            NChildren = ets:update_counter(Stats, State, {2, Inc}, {State, 0}),
            {NChildren, NewState};
        false ->
            [#trans{next = NextState}] = ets:lookup(Trie, Key),
            {false, NextState}
    end.

%% @doc Get storage static key
-spec get_id_for_key(trie(), state(), edge()) -> static_key().
get_id_for_key(#trie{is_binary_key = IsBin, static_key_size = Size}, State, Token) ->
    %% Requirements for the return value:
    %%
    %% It should be globally unique for the `{State, Token}` pair. Other
    %% than that, there's no requirements. The return value doesn't even
    %% have to be deterministic, since the states are saved in the trie.
    %% Yet, it helps a lot if it is, so that applying the same sequence
    %% of topics to different tries will result in the same trie state.
    %%
    %% The generated value becomes the ID of the topic in the durable
    %% storage. Its size should be relatively small to reduce the
    %% overhead of storing messages.
    %%
    %% If we want to impress computer science crowd, sorry, I mean to
    %% minimize storage requirements, we can even employ Huffman coding
    %% based on the frequency of messages.
    Hash = crypto:hash(sha256, term_to_binary([State | Token])),
    case IsBin of
        false ->
            %% Note: for backward compatibility with bitstream_lts
            %% layout we allow the key to be an integer. But this also
            %% changes the semantics of `static_key_size` from number
            %% of bytes to bits:
            <<Int:Size, _/bytes>> = Hash,
            Int;
        true ->
            element(1, erlang:split_binary(Hash, Size))
    end.

%% erlfmt-ignore
-spec do_match_topics(trie(), state(), [level() | '+'], [level() | '+' | '#']) ->
          list().
do_match_topics(Trie, State, Varying, []) ->
    case trie_next(Trie, State, ?EOT) of
        {false, Static} -> [{Static, lists:reverse(Varying)}];
        undefined -> []
    end;
do_match_topics(Trie, State, Varying, ['#']) ->
    Emanating = emanating(Trie, State, ?PLUS),
    lists:flatmap(
        fun
            ({?EOT, Static}) ->
                [{Static, lists:reverse(Varying)}];
            ({?PLUS, NextState}) ->
                do_match_topics(Trie, NextState, [?PLUS | Varying], ['#']);
            ({_, NextState}) ->
                do_match_topics(Trie, NextState, Varying, ['#'])
        end,
        Emanating
    );
do_match_topics(Trie, State, Varying, [Level | Rest]) ->
    Emanating = emanating(Trie, State, Level),
    lists:flatmap(
        fun
            ({?EOT, _NextState}) ->
                [];
            ({?PLUS, NextState}) ->
                do_match_topics(Trie, NextState, [Level | Varying], Rest);
            ({_, NextState}) ->
                do_match_topics(Trie, NextState, Varying, Rest)
        end,
        Emanating
    ).

-spec do_lookup_topic_key(trie(), state(), [level()], [level()]) ->
    {ok, msg_storage_key()} | undefined.
do_lookup_topic_key(Trie, State, [], Varying) ->
    case trie_next(Trie, State, ?EOT) of
        {false, Static} ->
            {ok, {Static, lists:reverse(Varying)}};
        undefined ->
            undefined
    end;
do_lookup_topic_key(Trie, State, [Tok | Rest], Varying) ->
    case trie_next(Trie, State, Tok) of
        {true, NextState} ->
            do_lookup_topic_key(Trie, NextState, Rest, [Tok | Varying]);
        {false, NextState} ->
            do_lookup_topic_key(Trie, NextState, Rest, Varying);
        undefined ->
            undefined
    end.

do_topic_key(Trie, _, _, State, [], Tokens, Varying) ->
    %% We reached the end of topic. Assert: Trie node that corresponds
    %% to EOT cannot be a wildcard.
    {Updated, false, Static} = trie_next_(Trie, State, ?EOT),
    _ =
        case Trie#trie.rlookups andalso Updated of
            false ->
                ok;
            _ ->
                trie_insert(Trie, rlookup, Static, lists:reverse(Tokens))
        end,
    {Static, lists:reverse(Varying)};
do_topic_key(Trie, ThresholdFun, Depth, State, [Tok | Rest], Tokens, Varying0) ->
    % TODO: it's not necessary to call it every time.
    Threshold = ThresholdFun(Depth),
    {NChildren, IsWildcard, NextState} = trie_next_(Trie, State, Tok),
    Varying =
        case IsWildcard of
            _ when is_integer(NChildren), NChildren >= Threshold ->
                %% Topic structure learnt!
                %% Number of children for the trie node reached the
                %% threshold, we need to insert wildcard here.
                %% Next new children from next call will resue the WildcardState.
                {_, _WildcardState} = trie_insert(Trie, State, ?PLUS),
                Varying0;
            false ->
                Varying0;
            true ->
                %% This topic level is marked as wildcard in the trie,
                %% we need to add it to the varying part of the key:
                [Tok | Varying0]
        end,
    TokOrWildcard =
        case IsWildcard of
            true -> ?PLUS;
            false -> Tok
        end,
    do_topic_key(Trie, ThresholdFun, Depth + 1, NextState, Rest, [TokOrWildcard | Tokens], Varying).

%% @doc Has side effects! Inserts missing elements.
-spec trie_next_(trie(), state(), binary() | ?EOT) -> {New, IsWildcard, state()} when
    New :: false | non_neg_integer(),
    IsWildcard :: boolean().
trie_next_(Trie, State, Token) ->
    case trie_next(Trie, State, Token) of
        {IsWildcard, NextState} ->
            {false, IsWildcard, NextState};
        undefined ->
            %% No exists, create new static key for return
            {Updated, NextState} = trie_insert(Trie, State, Token),
            {Updated, false, NextState}
    end.

%% @doc Return all edges emanating from a node:
%% erlfmt-ignore
-spec emanating(trie(), state(), edge()) -> [{edge(), state()}].
emanating(#trie{trie = Tab}, State, ?PLUS) ->
    ets:select(
        Tab,
        ets:fun2ms(
            fun(#trans{key = {S, Edge}, next = Next}) when S == State ->
                {Edge, Next}
            end
        )
    );
emanating(#trie{trie = Tab}, State, ?EOT) ->
    case ets:lookup(Tab, {State, ?EOT}) of
        [#trans{next = Next}] -> [{?EOT, Next}];
        [] -> []
    end;
emanating(#trie{trie = Tab}, State, Token) when is_binary(Token); Token =:= '' ->
    [
        {Edge, Next}
     || #trans{key = {_, Edge}, next = Next} <-
            ets:lookup(Tab, {State, ?PLUS}) ++
                ets:lookup(Tab, {State, Token})
    ].

all_emanating(#trie{trie = Tab}, State) ->
    ets:select(
        Tab,
        ets:fun2ms(fun(#trans{key = {S, Edge}, next = Next}) when S == State ->
            {{S, Edge}, Next}
        end)
    ).

paths(#trie{} = T) ->
    Roots = all_emanating(T, ?PREFIX),
    lists:flatmap(
        fun({Segment, Next}) ->
            follow_path(T, Next, [{Segment, Next}])
        end,
        Roots
    ).

follow_path(#trie{} = T, State, Path) ->
    lists:flatmap(
        fun
            ({{_State, ?EOT}, _Next} = Segment) ->
                [lists:reverse([Segment | Path])];
            ({_Edge, Next} = Segment) ->
                follow_path(T, Next, [Segment | Path])
        end,
        all_emanating(T, State)
    ).

tokens_of_path([{{_State, Token}, _Next} | Rest]) ->
    [Token | tokens_of_path(Rest)];
tokens_of_path([]) ->
    [].

%% Wrapper for type checking only:
-compile({inline, ets_insert_new/2}).
-spec ets_insert_new(ets:tid(), trans()) -> boolean().
ets_insert_new(Tid, Trans) ->
    ets:insert_new(Tid, Trans).

compress_topic(_StaticKey, [], [], Acc) ->
    lists:reverse(Acc);
compress_topic(StaticKey, TStructL0, ['#'], Acc) ->
    case TStructL0 of
        [] ->
            lists:reverse(Acc);
        ['+' | TStructL] ->
            compress_topic(StaticKey, TStructL, ['#'], ['+' | Acc]);
        [_ | TStructL] ->
            compress_topic(StaticKey, TStructL, ['#'], Acc)
    end;
compress_topic(StaticKey, ['+' | TStructL], [Level | TopicL], Acc) ->
    compress_topic(StaticKey, TStructL, TopicL, [Level | Acc]);
compress_topic(StaticKey, [Struct | TStructL], [Level | TopicL], Acc) when
    Level =:= '+'; Level =:= Struct
->
    compress_topic(StaticKey, TStructL, TopicL, Acc);
compress_topic(StaticKey, TStructL, TopicL, _Acc) ->
    %% Topic is mismatched with the structure. This should never
    %% happen. LTS got corrupted?
    Err = #{
        msg => 'Topic structure mismatch',
        static_key => StaticKey,
        input => TopicL,
        structure => TStructL
    },
    throw({unrecoverable, Err}).

decompress_topic(['+' | TStructL], [Level | TopicL], Acc) ->
    decompress_topic(TStructL, TopicL, [Level | Acc]);
decompress_topic([StaticLevel | TStructL], TopicL, Acc) ->
    decompress_topic(TStructL, TopicL, [StaticLevel | Acc]);
decompress_topic([], [], Acc) ->
    lists:reverse(Acc).

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

trie_basic_test() ->
    T = trie_create(),
    ?assertMatch(undefined, trie_next(T, ?PREFIX, <<"foo">>)),
    {1, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    ?assertMatch({false, S1}, trie_insert(T, ?PREFIX, <<"foo">>)),
    ?assertMatch({false, S1}, trie_next(T, ?PREFIX, <<"foo">>)),

    ?assertMatch(undefined, trie_next(T, ?PREFIX, <<"bar">>)),
    {2, S2} = trie_insert(T, ?PREFIX, <<"bar">>),
    ?assertMatch({false, S2}, trie_insert(T, ?PREFIX, <<"bar">>)),

    ?assertMatch(undefined, trie_next(T, S1, <<"foo">>)),
    ?assertMatch(undefined, trie_next(T, S1, <<"bar">>)),
    {1, S11} = trie_insert(T, S1, <<"foo">>),
    {2, S12} = trie_insert(T, S1, <<"bar">>),
    ?assertMatch({false, S11}, trie_next(T, S1, <<"foo">>)),
    ?assertMatch({false, S12}, trie_next(T, S1, <<"bar">>)),

    ?assertMatch(undefined, trie_next(T, S11, <<"bar">>)),
    {1, S111} = trie_insert(T, S11, <<"bar">>),
    ?assertMatch({false, S111}, trie_next(T, S11, <<"bar">>)).

lookup_key_test() ->
    T = trie_create(),
    {_, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    {_, S11} = trie_insert(T, S1, <<"foo">>),
    %% Topics don't match until we insert ?EOT:
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"foo">>])
    ),
    {_, S10} = trie_insert(T, S1, ?EOT),
    {_, S110} = trie_insert(T, S11, ?EOT),
    ?assertMatch(
        {ok, {S10, []}},
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        {ok, {S110, []}},
        lookup_topic_key(T, [<<"foo">>, <<"foo">>])
    ),
    %% The rest of keys still don't match:
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>, <<"foo">>])
    ).

wildcard_lookup_test() ->
    T = trie_create(),
    {1, S1} = trie_insert(T, ?PREFIX, <<"foo">>),
    %% Plus doesn't increase the number of children
    {0, S11} = trie_insert(T, S1, ?PLUS),
    {1, S111} = trie_insert(T, S11, <<"foo">>),
    %% ?EOT doesn't increase the number of children
    {0, S1110} = trie_insert(T, S111, ?EOT),
    ?assertMatch(
        {ok, {S1110, [<<"bar">>]}},
        lookup_topic_key(T, [<<"foo">>, <<"bar">>, <<"foo">>])
    ),
    ?assertMatch(
        {ok, {S1110, [<<"quux">>]}},
        lookup_topic_key(T, [<<"foo">>, <<"quux">>, <<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"foo">>, <<"bar">>, <<"bar">>])
    ),
    ?assertMatch(
        undefined,
        lookup_topic_key(T, [<<"bar">>, <<"foo">>, <<"foo">>])
    ),
    {_, S10} = trie_insert(T, S1, ?EOT),
    ?assertMatch(
        {ok, {S10, []}},
        lookup_topic_key(T, [<<"foo">>])
    ).

%% erlfmt-ignore
topic_key_test() ->
    T = trie_create(),
    try
        Threshold = 4,
        ThresholdFun = fun(0) -> 1000;
                          (_) -> Threshold
                       end,
        %% Test that bottom layer threshold is high:
        lists:foreach(
          fun(I) ->
                  {_, []} = test_key(T, ThresholdFun, [I, 99999, 999999, 99999])
          end,
          lists:seq(1, 10)),
        %% Test adding children on the 2nd level:
        lists:foreach(
          fun(I) ->
                  case test_key(T, ThresholdFun, [1, I, 1]) of
                      {_, []} ->
                          ?assert(I < Threshold, {I, '<', Threshold}),
                          ok;
                      {_, [Var]} ->
                          ?assert(I >= Threshold, {I, '>=', Threshold}),
                          ?assertEqual(Var, integer_to_binary(I))
                  end
          end,
          lists:seq(1, 100)),
        %% This doesn't affect 2nd level with a different prefix:
        ?assertMatch({_, []}, test_key(T, ThresholdFun, [2, 1, 1])),
        ?assertMatch({_, []}, test_key(T, ThresholdFun, [2, 10, 1])),
        %% This didn't retroactively change the indexes that were
        %% created prior to reaching the threshold:
        ?assertMatch({_, []}, test_key(T, ThresholdFun, [1, 1, 1])),
        ?assertMatch({_, []}, test_key(T, ThresholdFun, [1, 2, 1])),
        %% Now create another level of +:
        lists:foreach(
          fun(I) ->
                  case test_key(T, ThresholdFun, [1, 42, 1, I, 42]) of
                      {_, [<<"42">>]} when I =< Threshold -> %% TODO: off by 1 error
                          ok;
                      {_, [<<"42">>, Var]} ->
                          ?assertEqual(Var, integer_to_binary(I));
                      Ret ->
                          error({Ret, I})
                  end
          end,
          lists:seq(1, 100))
    after
        dump_to_dot(T, filename:join("_build", atom_to_list(?FUNCTION_NAME) ++ ".dot"))
    end.

%% erlfmt-ignore
topic_match_test() ->
    T = trie_create(),
    try
        Threshold = 2,
        ThresholdFun = fun(0) -> 1000;
                          (_) -> Threshold
                       end,
        {S1, []} = test_key(T, ThresholdFun, [1]),
        {S11, []} = test_key(T, ThresholdFun, [1, 1]),
        {S12, []} = test_key(T, ThresholdFun, [1, 2]),
        {S111, []} = test_key(T, ThresholdFun, [1, 1, 1]),
        {S11e, []} = test_key(T, ThresholdFun, [1, 1, '']),
        %% Match concrete topics:
        assert_match_topics(T, [1], [{S1, []}]),
        assert_match_topics(T, [1, 1], [{S11, []}]),
        assert_match_topics(T, [1, 1, 1], [{S111, []}]),
        %% Match topics with +:
        assert_match_topics(T, [1, '+'], [{S11, []}, {S12, []}]),
        assert_match_topics(T, [1, '+', 1], [{S111, []}]),
        assert_match_topics(T, [1, '+', ''], [{S11e, []}]),
        %% Match topics with #:
        assert_match_topics(T, [1, '#'],
                            [{S1, []},
                             {S11, []}, {S12, []},
                             {S111, []}, {S11e, []}]),
        assert_match_topics(T, [1, 1, '#'],
                            [{S11, []},
                             {S111, []},
                             {S11e, []}]),
        %% Now add learned wildcards:
        {S21, []} = test_key(T, ThresholdFun, [2, 1]),
        {S22, []} = test_key(T, ThresholdFun, [2, 2]),
        {S2_, [<<"3">>]} = test_key(T, ThresholdFun, [2, 3]),
        {S2_11, [<<"3">>]} = test_key(T, ThresholdFun, [2, 3, 1, 1]),
        {S2_12, [<<"4">>]} = test_key(T, ThresholdFun, [2, 4, 1, 2]),
        {S2_1_, [<<"3">>, <<"3">>]} = test_key(T, ThresholdFun, [2, 3, 1, 3]),
        %% %% Check matching:
        assert_match_topics(T, [2, 2],
                            [{S22, []}, {S2_, [<<"2">>]}]),
        assert_match_topics(T, [2, '+'],
                            [{S22, []}, {S21, []}, {S2_, ['+']}]),
        assert_match_topics(T, [2, '#'],
                            [{S21, []}, {S22, []},
                             {S2_, ['+']},
                             {S2_11, ['+']}, {S2_12, ['+']}, {S2_1_, ['+', '+']}]),
        ok
    after
        dump_to_dot(T, filename:join("_build", atom_to_list(?FUNCTION_NAME) ++ ".dot"))
    end.

%% erlfmt-ignore
rlookup_test() ->
    T = trie_create(#{reverse_lookups => true}),
    Threshold = 2,
    ThresholdFun = fun(0) -> 1000;
                      (_) -> Threshold
                   end,
    {S1, []} = test_key(T, ThresholdFun, [1]),
    {S11, []} = test_key(T, ThresholdFun, [1, 1]),
    {S12, []} = test_key(T, ThresholdFun, [1, 2]),
    {S111, []} = test_key(T, ThresholdFun, [1, 1, 1]),
    {S11e, []} = test_key(T, ThresholdFun, [1, 1, '']),
    %% Now add learned wildcards:
    {S21, []} = test_key(T, ThresholdFun, [2, 1]),
    {S22, []} = test_key(T, ThresholdFun, [2, 2]),
    {S2_, [<<"3">>]} = test_key(T, ThresholdFun, [2, 3]),
    {S2_11, [<<"3">>]} = test_key(T, ThresholdFun, [2, 3, 1, 1]),
    {S2_12, [<<"4">>]} = test_key(T, ThresholdFun, [2, 4, 1, 2]),
    {S2_1_, [<<"3">>, <<"3">>]} = test_key(T, ThresholdFun, [2, 3, 1, 3]),
    %% Check reverse matching:
    ?assertEqual({ok, [<<"1">>]}, reverse_lookup(T, S1)),
    ?assertEqual({ok, [<<"1">>, <<"1">>]}, reverse_lookup(T, S11)),
    ?assertEqual({ok, [<<"1">>, <<"2">>]}, reverse_lookup(T, S12)),
    ?assertEqual({ok, [<<"1">>, <<"1">>, <<"1">>]}, reverse_lookup(T, S111)),
    ?assertEqual({ok, [<<"1">>, <<"1">>, '']}, reverse_lookup(T, S11e)),
    ?assertEqual({ok, [<<"2">>, <<"1">>]}, reverse_lookup(T, S21)),
    ?assertEqual({ok, [<<"2">>, <<"2">>]}, reverse_lookup(T, S22)),
    ?assertEqual({ok, [<<"2">>, '+']}, reverse_lookup(T, S2_)),
    ?assertEqual({ok, [<<"2">>, '+', <<"1">>, <<"1">>]}, reverse_lookup(T, S2_11)),
    ?assertEqual({ok, [<<"2">>, '+', <<"1">>, <<"2">>]}, reverse_lookup(T, S2_12)),
    ?assertEqual({ok, [<<"2">>, '+', <<"1">>, '+']}, reverse_lookup(T, S2_1_)),
    %% Dump and restore trie to make sure rlookup still works:
    T1 = trie_restore(#{reverse_lookups => true}, trie_dump(T, all)),
    destroy(T),
    ?assertEqual({ok, [<<"2">>, <<"1">>]}, reverse_lookup(T1, S21)),
    ?assertEqual({ok, [<<"2">>, '+', <<"1">>, '+']}, reverse_lookup(T1, S2_1_)).

updated_topics_test() ->
    %% Trie updates are sent as messages to self:
    T = trie_create(#{
        reverse_lookups => true,
        persist_callback => fun(Key, Val) ->
            self() ! {?FUNCTION_NAME, Key, Val},
            ok
        end
    }),
    %% Dump trie updates from the mailbox:
    Ops = fun F() ->
        receive
            {?FUNCTION_NAME, K, V} -> [{K, V} | F()]
        after 0 -> []
        end
    end,
    Threshold = 2,
    ThresholdFun = fun
        (0) -> 1000;
        (_) -> Threshold
    end,
    %% Singleton topics:
    test_key(T, ThresholdFun, [1]),
    ?assertMatch(
        [[<<"1">>]],
        updated_topics(T, Ops())
    ),
    %% Check that events are only sent once:
    test_key(T, ThresholdFun, [1]),
    ?assertMatch(
        [],
        updated_topics(T, Ops())
    ),
    test_key(T, ThresholdFun, [1, 1]),
    ?assertMatch(
        [[<<"1">>, <<"1">>]],
        updated_topics(T, Ops())
    ),
    test_key(T, ThresholdFun, [1, 2]),
    ?assertMatch(
        [[<<"1">>, <<"2">>]],
        updated_topics(T, Ops())
    ),
    %% Upgrade to wildcard:
    test_key(T, ThresholdFun, [1, 3]),
    ?assertMatch(
        [[<<"1">>, '+']],
        updated_topics(T, Ops())
    ),
    test_key(T, ThresholdFun, [1, 4]),
    ?assertMatch(
        [],
        updated_topics(T, Ops())
    ).

n_topics_test() ->
    Threshold = 3,
    ThresholdFun = fun
        (0) -> 1000;
        (_) -> Threshold
    end,

    T = trie_create(#{reverse_lookups => true}),
    ?assertEqual(0, info(T, topics)),
    {S11, []} = test_key(T, ThresholdFun, [1, 1]),
    {S11, []} = test_key(T, ThresholdFun, [1, 1]),
    ?assertEqual(1, info(T, topics)),

    {S12, []} = test_key(T, ThresholdFun, [1, 2]),
    {S12, []} = test_key(T, ThresholdFun, [1, 2]),
    ?assertEqual(2, info(T, topics)),

    {_S13, []} = test_key(T, ThresholdFun, [1, 3]),
    ?assertEqual(3, info(T, topics)),

    {S1_, [_]} = test_key(T, ThresholdFun, [1, 4]),
    ?assertEqual(4, info(T, topics)),

    {S1_, [_]} = test_key(T, ThresholdFun, [1, 5]),
    {S1_, [_]} = test_key(T, ThresholdFun, [1, 6]),
    {S1_, [_]} = test_key(T, ThresholdFun, [1, 7]),
    ?assertEqual(4, info(T, topics)),

    ?assertMatch(
        [{size, N}, {topics, 4}] when is_integer(N),
        info(T)
    ).

-define(keys_history, topic_key_history).

%% erlfmt-ignore
assert_match_topics(Trie, Filter0, Expected) ->
    Filter = lists:map(fun(I) when is_integer(I) -> integer_to_binary(I);
                          (I) -> I
                       end,
                       Filter0),
    Matched = match_topics(Trie, Filter),
    ?assertMatch( #{missing := [], unexpected := []}
                , #{ missing    => Expected -- Matched
                   , unexpected => Matched -- Expected
                   }
                , Filter
                ).

%% erlfmt-ignore
test_key(Trie, Threshold, Topic0) ->
    Topic = lists:map(fun('') -> '';
                         (I) -> integer_to_binary(I)
                      end,
                      Topic0),
    Ret = topic_key(Trie, Threshold, Topic),
    %% Test idempotency:
    Ret1 = topic_key(Trie, Threshold, Topic),
    ?assertEqual(Ret, Ret1, Topic),
    %% Add new key to the history:
    case get(?keys_history) of
        undefined  -> OldHistory = #{};
        OldHistory -> ok
    end,
    %% Test that the generated keys are always unique for the topic:
    History = maps:update_with(
                Ret,
                fun(Old) ->
                        case Old =:= Topic of
                            true  -> Old;
                            false -> error(#{ '$msg' => "Duplicate key!"
                                            , key => Ret
                                            , old_topic => Old
                                            , new_topic => Topic
                                            })
                        end
                end,
                Topic,
                OldHistory),
    put(?keys_history, History),
    {ok, Ret} = lookup_topic_key(Trie, Topic),
    Ret.

paths_test() ->
    T = trie_create(),
    Threshold = 4,
    ThresholdFun = fun
        (0) -> 1000;
        (_) -> Threshold
    end,
    PathsToInsert =
        [
            [''],
            [1],
            [2, 2],
            [3, 3, 3],
            [2, 3, 4]
        ] ++ [[4, I, 4] || I <- lists:seq(1, Threshold + 2)] ++
            [['', I, ''] || I <- lists:seq(1, Threshold + 2)],
    lists:foreach(
        fun(PathSpec) ->
            test_key(T, ThresholdFun, PathSpec)
        end,
        PathsToInsert
    ),

    %% Test that the paths we've inserted are produced in the output
    Paths = paths(T),
    FormattedPaths = lists:map(fun format_path/1, Paths),
    ExpectedWildcardPaths =
        [
            [4, '+', 4],
            ['', '+', '']
        ],
    ExpectedPaths =
        [
            [''],
            [1],
            [2, 2],
            [3, 3, 3]
        ] ++ [[4, I, 4] || I <- lists:seq(1, Threshold)] ++
            [['', I, ''] || I <- lists:seq(1, Threshold)] ++
            ExpectedWildcardPaths,
    FormatPathSpec =
        fun(PathSpec) ->
            lists:map(
                fun
                    (I) when is_integer(I) -> integer_to_binary(I);
                    (A) -> A
                end,
                PathSpec
            ) ++ [?EOT]
        end,
    lists:foreach(
        fun(PathSpec) ->
            Path = FormatPathSpec(PathSpec),
            ?assert(
                lists:member(Path, FormattedPaths),
                #{
                    paths => FormattedPaths,
                    expected_path => Path
                }
            )
        end,
        ExpectedPaths
    ),

    %% Test filter function for paths containing wildcards
    WildcardPaths = lists:filter(
        fun(Path) ->
            lists:member(?PLUS, tokens_of_path(Path))
        end,
        Paths
    ),
    FormattedWildcardPaths = lists:map(fun format_path/1, WildcardPaths),
    ?assertEqual(
        sets:from_list(lists:map(FormatPathSpec, ExpectedWildcardPaths), [{version, 2}]),
        sets:from_list(FormattedWildcardPaths, [{version, 2}]),
        #{
            expected => ExpectedWildcardPaths,
            wildcards => FormattedWildcardPaths
        }
    ),

    %% Test that we're able to reconstruct the same trie from the paths
    T2 = trie_create(),
    [
        trie_insert(T2, State, Edge, Next)
     || Path <- Paths,
        {{State, Edge}, Next} <- Path
    ],
    #trie{trie = Tab1} = T,
    #trie{trie = Tab2} = T2,
    Dump1 = sets:from_list(ets:tab2list(Tab1), [{version, 2}]),
    Dump2 = sets:from_list(ets:tab2list(Tab2), [{version, 2}]),
    ?assertEqual(Dump1, Dump2).

format_path([{{_State, Edge}, _Next} | Rest]) ->
    [Edge | format_path(Rest)];
format_path([]) ->
    [].

compress_topic_test() ->
    %% Structure without wildcards:
    ?assertEqual([], compress_topic(42, [], [])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, ''], [<<"foo">>, ''])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, ''], [<<"foo">>, '+'])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, ''], ['+', '+'])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, <<"bar">>, ''], ['#'])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, <<"bar">>, ''], [<<"foo">>, <<"bar">>, '#'])),
    ?assertEqual([], compress_topic(42, [<<"foo">>, <<"bar">>, ''], ['+', '#'])),
    ?assertEqual(
        [], compress_topic(42, [<<"foo">>, <<"bar">>, ''], [<<"foo">>, <<"bar">>, '', '#'])
    ),
    %% With wildcards:
    ?assertEqual(
        [<<"1">>], compress_topic(42, [<<"foo">>, '+', <<"bar">>], [<<"foo">>, <<"1">>, <<"bar">>])
    ),
    ?assertEqual(
        [<<"1">>, <<"2">>],
        compress_topic(
            42,
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>],
            [<<"foo">>, <<"1">>, <<"bar">>, <<"2">>, <<"baz">>]
        )
    ),
    ?assertEqual(
        ['+', <<"2">>],
        compress_topic(
            42,
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>],
            [<<"foo">>, '+', <<"bar">>, <<"2">>, <<"baz">>]
        )
    ),
    ?assertEqual(
        ['+', '+'],
        compress_topic(
            42,
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>],
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>]
        )
    ),
    ?assertEqual(
        ['+', '+'],
        compress_topic(
            42,
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>],
            ['#']
        )
    ),
    ?assertEqual(
        ['+', '+'],
        compress_topic(
            42,
            [<<"foo">>, '+', <<"bar">>, '+', <<"baz">>],
            [<<"foo">>, '+', '+', '#']
        )
    ),
    %% Mismatch:
    ?assertException(_, {unrecoverable, _}, compress_topic(42, [<<"foo">>], [<<"bar">>])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, [], [<<"bar">>])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, [<<"foo">>], [])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, ['', ''], ['', '', ''])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, ['', ''], [<<"foo">>, '#'])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, ['', ''], ['+', '+', '+', '#'])),
    ?assertException(_, {unrecoverable, _}, compress_topic(42, ['+'], [<<"bar">>, '+'])),
    ?assertException(
        _, {unrecoverable, _}, compress_topic(42, [<<"foo">>, '+'], [<<"bar">>, <<"baz">>])
    ).

decompress_topic_test() ->
    %% Structure without wildcards:
    ?assertEqual([], decompress_topic([], [])),
    ?assertEqual(
        [<<"foo">>, '', <<"bar">>],
        decompress_topic([<<"foo">>, '', <<"bar">>], [])
    ),
    %% With wildcards:
    ?assertEqual(
        [<<"foo">>, '', <<"bar">>, <<"baz">>],
        decompress_topic([<<"foo">>, '+', <<"bar">>, '+'], ['', <<"baz">>])
    ),
    ?assertEqual(
        [<<"foo">>, '+', <<"bar">>, '+', ''],
        decompress_topic([<<"foo">>, '+', <<"bar">>, '+', ''], ['+', '+'])
    ).

-endif.
