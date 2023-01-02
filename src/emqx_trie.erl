%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trie).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie APIs
-export([ insert/1
        , match/1
        , delete/1
        , put_compaction_flag/1
        , put_default_compaction_flag/0
        ]).

-export([ empty/0
        , lock_tables/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(TRIE, emqx_trie).
-define(PREFIX(Prefix), {Prefix, 0}).
-define(TOPIC(Topic), {Topic, 1}).

-record(?TRIE,
        { key :: ?TOPIC(binary()) | ?PREFIX(binary())
        , count = 0 :: non_neg_integer()
        }).

-define(IS_COMPACT, true).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

put_compaction_flag(Bool) when is_boolean(Bool) ->
    _ = persistent_term:put({?MODULE, compaction}, Bool),
    ok.

put_default_compaction_flag() ->
    ok = put_compaction_flag(?IS_COMPACT).

%% @doc Create or replicate topics table.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true},
                         {write_concurrency, true}
                        ]}],
    ok = ekka_mnesia:create_table(?TRIE, [
                {ram_copies, [node()]},
                {record_name, ?TRIE},
                {attributes, record_info(fields, ?TRIE)},
                {type, ordered_set},
                {storage_properties, StoreProps}]);
mnesia(copy) ->
    %% Copy topics table
    ok = ekka_mnesia:copy_table(?TRIE, ram_copies).

%%--------------------------------------------------------------------
%% Topics APIs
%%--------------------------------------------------------------------

%% @doc Insert a topic filter into the trie.
-spec(insert(emqx_topic:topic()) -> ok).
insert(Topic) when is_binary(Topic) ->
    {TopicKey, PrefixKeys} = make_keys(Topic),
    case mnesia:wread({?TRIE, TopicKey}) of
        [_] -> ok; %% already inserted
        [] -> lists:foreach(fun insert_key/1, [TopicKey | PrefixKeys])
    end.

%% @doc Delete a topic filter from the trie.
-spec(delete(emqx_topic:topic()) -> ok).
delete(Topic) when is_binary(Topic) ->
    {TopicKey, PrefixKeys} = make_keys(Topic),
    case [] =/= mnesia:wread({?TRIE, TopicKey}) of
        true -> lists:foreach(fun delete_key/1, [TopicKey | PrefixKeys]);
        false -> ok
    end.

%% @doc Find trie nodes that matchs the topic name.
-spec(match(emqx_topic:topic()) -> list(emqx_topic:topic())).
match(Topic) when is_binary(Topic) ->
    Words = emqx_topic:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            %% In MQTT spec, clients are not allowed to
            %% publish messages to a wildcard topic.
            %% Here we refuse to match wildcard topic.
            %%
            %% NOTE: this does not imply emqx allows clients
            %% publishing to wildcard topics.
            %% Such clients will get disconnected.
            [];
        false ->
            do_match(Words)
    end.

%% @doc Is the trie empty?
-spec(empty() -> boolean()).
empty() -> ets:first(?TRIE) =:= '$end_of_table'.

-spec lock_tables() -> ok.
lock_tables() ->
    mnesia:write_lock_table(?TRIE).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_keys(Topic) ->
    Words = emqx_topic:words(Topic),
    {?TOPIC(Topic), [?PREFIX(Prefix) || Prefix <- make_prefixes(Words)]}.

compact(Words) ->
    case is_compact() of
        true -> do_compact(Words);
        false -> Words
    end.

%% join split words into compacted segments
%% each segment ends with one wildcard word
%% e.g.
%% a/b/c/+/d/# => [a/b/c/+, d/#]
%% a/+/+/b => [a/+, +, b]
%% a/+/+/+/+/b => [a/+, +, +, +, b]
do_compact(Words) ->
    do_compact(Words, empty, []).

do_compact([], empty, Acc) -> lists:reverse(Acc);
do_compact([], Seg, Acc) -> lists:reverse([Seg | Acc]);
do_compact([Word | Words], Seg, Acc) when Word =:= '+' orelse Word =:= '#' ->
    do_compact(Words, empty, [join(Seg, Word) | Acc]);
do_compact([Word | Words], Seg, Acc) ->
    do_compact(Words, join(Seg, Word), Acc).

join(empty, '+') -> <<"+">>;
join(empty, '#') -> <<"#">>;
join(empty, '') -> <<>>;
join(empty, Word) -> Word;
join(Prefix, Word) -> emqx_topic:join([Prefix, Word]).

make_prefixes(Words) ->
    lists:map(fun emqx_topic:join/1,
              make_prefixes(compact(Words), [], [])).

make_prefixes([_LastWord], _Prefix, Acc) ->
    lists:map(fun lists:reverse/1, Acc);
make_prefixes([H | T], Prefix0, Acc0) ->
    Prefix = [H | Prefix0],
    Acc = [Prefix | Acc0],
    make_prefixes(T, Prefix, Acc).

insert_key(Key) ->
    T = case mnesia:wread({?TRIE, Key}) of
            [#?TRIE{count = C} = T1] ->
                T1#?TRIE{count = C + 1};
             [] ->
                #?TRIE{key = Key, count = 1}
         end,
    ok = mnesia:write(T).

delete_key(Key) ->
    case mnesia:wread({?TRIE, Key}) of
        [#?TRIE{count = C} = T] when C > 1 ->
            ok = mnesia:write(T#?TRIE{count = C - 1});
        [_] ->
            ok = mnesia:delete(?TRIE, Key, write);
        [] ->
            ok
    end.

%% micro-optimization: no need to lookup when topic is not wildcard
%% because we only insert wildcards to emqx_trie
lookup_topic(_Topic, false) -> [];
lookup_topic(Topic, true) -> lookup_topic(Topic).

lookup_topic(Topic) when is_binary(Topic) ->
    case ets:lookup(?TRIE, ?TOPIC(Topic)) of
        [#?TRIE{count = C}] -> [Topic || C > 0];
        [] -> []
    end.

has_prefix(empty) -> true; %% this is the virtual tree root
has_prefix(Prefix) ->
    case ets:lookup(?TRIE, ?PREFIX(Prefix)) of
        [#?TRIE{count = C}] -> C > 0;
        [] -> false
    end.

do_match([<<"$", _/binary>> = Prefix | Words]) ->
    %% For topics having dollar sign prefix,
    %% we do not match root level + or #,
    %% fast forward to the next level.
    case Words =:= [] of
        true -> lookup_topic(Prefix);
        false -> []
    end ++ do_match(Words, Prefix);
do_match(Words) ->
    do_match(Words, empty).

do_match(Words, Prefix) ->
    case is_compact() of
        true -> match_compact(Words, Prefix, false, []);
        false -> match_no_compact(Words, Prefix, false, [])
    end.

match_no_compact([], Topic, IsWildcard, Acc) ->
    'match_#'(Topic) ++ %% try match foo/+/# or foo/bar/#
    lookup_topic(Topic, IsWildcard) ++ %% e.g. foo/+
    Acc;
match_no_compact([Word | Words], Prefix, IsWildcard, Acc0) ->
    case has_prefix(Prefix) of
        true ->
            Acc1 = 'match_#'(Prefix) ++ Acc0,
            Acc = match_no_compact(Words, join(Prefix, '+'), true, Acc1),
            match_no_compact(Words, join(Prefix, Word), IsWildcard, Acc);
        false ->
            %% non-compact paths in database
            %% if there is no prefix matches the current topic prefix
            %% we can simpliy return from here
            %% e.g. a/b/c/+ results in
            %%  - a
            %%  - a/b
            %%  - a/b/c
            %%  - a/b/c/+
            %% if the input topic is to match 'a/x/y',
            %% then at the second level, we lookup prefix a/x,
            %% no such prefix to be found, meaning there is no point
            %% searching for 'a/x/y', 'a/x/+' or 'a/x/#'
            Acc0
    end.

match_compact([], Topic, IsWildcard, Acc) ->
    'match_#'(Topic) ++ %% try match foo/bar/#
    lookup_topic(Topic, IsWildcard) ++ %% try match foo/bar
    Acc;
match_compact([Word | Words], Prefix, IsWildcard, Acc0) ->
    Acc1 = 'match_#'(Prefix) ++ Acc0,
    Acc = match_compact(Words, join(Prefix, Word), IsWildcard, Acc1),
    WildcardPrefix = join(Prefix, '+'),
    %% go deeper to match current_prefix/+ only when:
    %% 1. current word is the last
    %% OR
    %% 2. there is a prefix = 'current_prefix/+'
    case Words =:= [] orelse has_prefix(WildcardPrefix) of
        true -> match_compact(Words, WildcardPrefix, true, Acc);
        false -> Acc
    end.

'match_#'(Prefix) ->
    MlTopic = join(Prefix, '#'),
    lookup_topic(MlTopic).

is_compact() ->
    case persistent_term:get({?MODULE, compaction}, undefined) of
        undefined ->
            Default = ?IS_COMPACT,
            FromEnv = emqx:get_env(trie_compaction, Default),
            _ = put_compaction_flag(FromEnv),
            true = is_boolean(FromEnv),
            FromEnv;
        Value when is_boolean(Value) ->
            Value
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_keys_test_() ->
    [{"no compact", fun() -> with_compact_flag(false, fun make_keys_no_compact/0) end},
     {"compact", fun() -> with_compact_flag(true, fun make_keys_compact/0) end}
    ].

make_keys_no_compact() ->
    ?assertEqual({?TOPIC(<<"#">>), []}, make_keys(<<"#">>)),
    ?assertEqual({?TOPIC(<<"a/+">>),
                  [?PREFIX(<<"a">>)]}, make_keys(<<"a/+">>)),
    ?assertEqual({?TOPIC(<<"+">>), []}, make_keys(<<"+">>)).

make_keys_compact() ->
    ?assertEqual({?TOPIC(<<"#">>), []}, make_keys(<<"#">>)),
    ?assertEqual({?TOPIC(<<"a/+">>), []}, make_keys(<<"a/+">>)),
    ?assertEqual({?TOPIC(<<"+">>), []}, make_keys(<<"+">>)),
    ?assertEqual({?TOPIC(<<"a/+/c">>),
                  [?PREFIX(<<"a/+">>)]}, make_keys(<<"a/+/c">>)).

words(T) -> emqx_topic:words(T).

make_prefixes_t(Topic) -> make_prefixes(words(Topic)).

with_compact_flag(IsCmopact, F) ->
    put_compaction_flag(IsCmopact),
    try F()
    after put_default_compaction_flag()
    end.

make_prefixes_test_() ->
    [{"no compact", fun() -> with_compact_flag(false, fun make_prefixes_no_compact/0) end},
     {"compact", fun() -> with_compact_flag(true, fun make_prefixes_compact/0) end}
    ].

make_prefixes_no_compact() ->
    ?assertEqual([<<"a/b">>, <<"a">>], make_prefixes_t(<<"a/b/+">>)),
    ?assertEqual([<<"a/b/+/c">>, <<"a/b/+">>, <<"a/b">>, <<"a">>],
                 make_prefixes_t(<<"a/b/+/c/#">>)).

make_prefixes_compact() ->
    ?assertEqual([], make_prefixes_t(<<"a/b/+">>)),
    ?assertEqual([<<"a/b/+">>], make_prefixes_t(<<"a/b/+/c/#">>)).

do_compact_test() ->
    ?assertEqual([<<"/+">>], do_compact(words(<<"/+">>))),
    ?assertEqual([<<"/#">>], do_compact(words(<<"/#">>))),
    ?assertEqual([<<"a/b/+">>, <<"c">>], do_compact(words(<<"a/b/+/c">>))),
    ?assertEqual([<<"a/+">>, <<"+">>, <<"b">>], do_compact(words(<<"a/+/+/b">>))),
    ?assertEqual([<<"a/+">>, <<"+">>, <<"+">>, <<"+">>, <<"b">>],
                 do_compact(words(<<"a/+/+/+/+/b">>))),
    ok.

clear_tables() -> mnesia:clear_table(?TRIE).

-endif. % TEST
