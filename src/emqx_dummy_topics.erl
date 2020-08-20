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

-module(emqx_dummy_topics).

-behaviour(gen_server).

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% APIs
-export([ match/1
        , add/1
        , del/1
        , list/0
        , has/1
        ]).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-record(dummy_topics, {
    topic :: emqx_types:topic(),
    created_at :: integer()
}).

-define(TIRE, {?MODULE, trie}).
-define(TOPICS, ?MODULE).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TOPICS, [
                {type, set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, dummy_topics},
                {attributes, record_info(fields, dummy_topics)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}
            ]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TOPICS).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec match(emqx_types:topic()) -> boolean().
match(Topic) ->
    trie_match(Topic, get_trie()).

-spec add(emqx_types:topic()) -> ok.
add(Topic) ->
    gen_server:cast(?MODULE, {add_topic, Topic}).

-spec del(emqx_types:topic()) -> ok.
del(Topic) ->
    gen_server:cast(?MODULE, {del_topic, Topic}).

-spec list() -> [emqx_types:topic()].
list() ->
    [#{topic => Topic, created_at => Timestamp}
     || #dummy_topics{topic = Topic, created_at = Timestamp}
        <- ets:tab2list(?TOPICS)].

-spec has(emqx_types:topic()) -> boolean().
has(Topic) ->
    ets:member(?TOPICS, Topic).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ok = init_trie(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({add_topic, Topic}, State) ->
    ok = mnesia:dirty_write(?TOPICS,
            #dummy_topics{
                topic = Topic,
                created_at = erlang:system_time(millisecond)
            }),
    ok = set_trie(trie_add(Topic, get_trie())),
    {noreply, State};

handle_cast({del_topic, Topic}, State) ->
    true = mnesia:dirty_delete(?TOPICS, Topic),
    ok = set_trie(trie_del(Topic, get_trie())),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Trie
%%%===================================================================

init_trie() ->
    set_trie(
        lists:foldl(fun(#dummy_topics{topic = Topic}, Trie) ->
                trie_add(Topic, Trie)
            end, new_trie(), ets:tab2list(?TOPICS))).

new_trie() ->
    #{counters => nil}.

get_trie() ->
    persistent_term:get(?TIRE).

set_trie(Trie) ->
    persistent_term:put(?TIRE, Trie).

trie_add(Topic, Trie) ->
    do_trie_add(emqx_topic:words(Topic), Trie).

do_trie_add([W | Words], Trie = #{counters := Cnt}) ->
    SubTrie = maps:get(W, Trie, new_trie()),
    Trie#{W => do_trie_add(Words, SubTrie),
          counters => incr(W, Cnt)};
do_trie_add([], Trie) ->
    Trie.

trie_del(Topic, Trie) ->
    do_trie_del(emqx_topic:words(Topic), Trie).

do_trie_del(_Words, Trie = #{counters := nil}) ->
    Trie;
do_trie_del([W | Words], Trie = #{counters := Cnt}) when is_map(Cnt) ->
    SubTrie = maps:get(W, Trie, new_trie()),
    case maps:get(W, Cnt, 1) of
        1 ->
            Trie2 = maps:remove(W, Trie),
            Trie2#{counters => maps:remove(W, Cnt)};
        N ->
            Trie#{W => do_trie_del(Words, SubTrie),
                  counters => N - 1}
    end;
do_trie_del([], Trie) ->
    Trie.

trie_match(_Topic, #{counters := nil}) ->
    false;
trie_match(Topic, Trie) ->
    do_trie_match(emqx_topic:words(Topic), Trie).

do_trie_match([], #{counters := nil}) ->
    true;
do_trie_match([], #{'#' := _}) ->
    true;
do_trie_match([], _Trie) ->
    false;
do_trie_match(_Words, #{counters := nil}) ->
    false;
do_trie_match([W | Words], Trie = #{counters := Cnt}) when is_map(Cnt) ->
    case maps:find(W, Trie) of
        error -> false;
        {ok, SubTrie} ->
            do_trie_match(Words, SubTrie)
    end.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

incr(Word, nil) -> #{Word => 1};
incr(Word, Counters) when is_map(Counters) ->
    Counters#{Word => maps:get(Word, Counters, 0) + 1}.
