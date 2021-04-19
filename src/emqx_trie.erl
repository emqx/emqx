%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        ]).

-export([empty/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-record(emqx_topic,
        { path :: binary()
        , prefix_count = 0 :: non_neg_integer()
        , topic_count = 0 :: non_neg_integer()
        }).

-define(TOPICS_TAB, emqx_topic).
-define(TPATH, emqx_topic).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

%% @doc Create or replicate topics table.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true},
                         {write_concurrency, true}]}],
    ok = ekka_mnesia:create_table(?TOPICS_TAB, [
                {ram_copies, [node()]},
                {record_name, ?TPATH},
                {attributes, record_info(fields, ?TPATH)},
                {storage_properties, StoreProps}]);
mnesia(copy) ->
    %% Copy topics table
    ok = ekka_mnesia:copy_table(?TOPICS_TAB, ram_copies).

%%--------------------------------------------------------------------
%% Topics APIs
%%--------------------------------------------------------------------

%% @doc Insert a topic filter into the trie.
-spec(insert(emqx_topic:topic()) -> ok).
insert(Topic) when is_binary(Topic) ->
    Paths = make_paths(Topic),
    lists:foreach(fun insert_tpath/1, Paths).

%% @doc Delete a topic filter from the trie.
-spec(delete(emqx_topic:topic()) -> ok).
delete(Topic) when is_binary(Topic) ->
    Paths = make_paths(Topic),
    lists:foreach(fun delete_tpath/1, Paths).

%% @doc Find trie nodes that match the topic name.
-spec(match(emqx_topic:topic()) -> list(emqx_topic:topic())).
match(Topic) when is_binary(Topic) ->
    Words = emqx_topic:words(Topic),
    false = emqx_topic:wildcard(Words), %% assert
    do_match(Words).

%% @doc Is the trie empty?
-spec(empty() -> boolean()).
empty() -> ets:info(?TOPICS_TAB, size) == 0.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_paths(Topic) ->
    Words = emqx_topic:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            Prefixes0 = make_prefixes(compact(Words)),
            Prefixes = lists:map(fun emqx_topic:join/1, Prefixes0),
            [topic_path(Topic) | lists:map(fun prefix_path/1, Prefixes)];
        false -> [topic_path(Topic)]
    end.

%% a/b/+/c/d => [a/b, +, c]
compact(Words) ->
    [I || I <- compact(Words, empty, []), I =/= empty].

compact([], Prefix, Acc) ->
    lists:reverse([Prefix | Acc]);
compact(['+' | T], Prefix, Acc) ->
    compact(T, empty, ['+', Prefix | Acc]);
compact(['#' | T], Prefix, Acc) ->
    compact(T, empty, ['#', Prefix | Acc]);
compact([H | T], Prefix, Acc) ->
    compact(T, join(Prefix, H), Acc).

join(empty, '+') -> <<"+">>;
join(empty, '#') -> <<"#">>;
join(empty, Word) -> Word;
join(Prefix, Word) -> emqx_topic:join([Prefix, Word]).

topic_path(Topic) ->
    #?TPATH{path = Topic, prefix_count = 0, topic_count = 1}.

prefix_path(Prefix) ->
    #?TPATH{path = Prefix, prefix_count = 1, topic_count = 0}.

make_prefixes(Words) ->
    make_prefixes(Words, [], []).

make_prefixes([_LastWord], _Prefix, Acc) ->
    lists:reverse(lists:map(fun lists:reverse/1, Acc));
make_prefixes([H | T], Prefix0, Acc0) ->
    Prefix = [H | Prefix0],
    Acc = [Prefix | Acc0],
    make_prefixes(T, Prefix, Acc).

insert_tpath(#?TPATH{ path = Path
                    , prefix_count = PcInc
                    , topic_count = TcInc
                    } = Tp0) ->
    Tp = case mnesia:wread({?TOPICS_TAB, Path}) of
             [#?TPATH{prefix_count = Pc, topic_count = Tc} = Tp1] ->
                 Tp1#?TPATH{prefix_count = Pc + PcInc, topic_count = Tc + TcInc};
             [] ->
                 Tp0
         end,
    ok = mnesia:write(Tp).

delete_tpath(#?TPATH{ path = Path
                    , prefix_count = PcInc
                    , topic_count = TcInc
                    } = Tp0) ->
    Tp = case mnesia:wread({?TOPICS_TAB, Path}) of
             [#?TPATH{prefix_count = Pc, topic_count = Tc} = Tp1] ->
                 Tp1#?TPATH{prefix_count = Pc - PcInc, topic_count = Tc - TcInc};
             [] ->
                 Tp0
         end,
    case Tp#?TPATH.topic_count =:= 0 andalso Tp#?TPATH.prefix_count =:= 0 of
        true ->
            ok = mnesia:delete(?TOPICS_TAB, Path, write);
        false ->
            ok = mnesia:write(Tp)
    end.

lookup_topic(Topic) when is_binary(Topic) ->
    case ets:lookup(?TOPICS_TAB, Topic) of
        [#?TPATH{topic_count = Tc}] ->
            [Topic || Tc > 0];
        [] ->
            []
    end.

has_prefix(Prefix) ->
    case ets:lookup(?TOPICS_TAB, Prefix) of
        [#?TPATH{prefix_count = Pc}] -> Pc > 0;
        [] -> false
    end.

do_match([<<"$", _/binary>> = Prefix | Words]) ->
    do_match(Words, Prefix, []);
do_match(Words) ->
    do_match(Words, empty, []).

do_match([], Topic, Acc) ->
    lookup_topic(Topic) ++ Acc;
do_match([Word | Words], Prefix, Acc0) ->
    Acc = match_multi_level(Prefix) ++
          match_single_level(Words, Prefix) ++
          Acc0,
    do_match(Words, join(Prefix, Word), Acc).

match_multi_level(Prefix) ->
    MlTopic = join(Prefix, '#'),
    lookup_topic(MlTopic).

match_single_level([], Prefix) ->
    %% no more remaining words
    SlTopic = join(Prefix, '+'),
    lookup_topic(SlTopic);
match_single_level(Words, Prefix) ->
    SlTopic = join(Prefix, '+'),
    case has_prefix(SlTopic) of
        true ->
            %% there is foo/+ prefix
            %% the input topic has more levels down
            %% go deeper
            do_match(Words, SlTopic, []);
        false ->
            []
    end.

