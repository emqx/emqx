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
-export([ start_link/0
        , stop/0
        ]).

-export([ match/1
        , match/2
        , add/1
        , del/1
        , list/0
        , has/1
        ]).

%% export for debug
-export([ get_trie/0
        ]).

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

-define(TIMEOUT, 5000).

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

stop() ->
    gen_server:stop(?MODULE).

-spec match(emqx_types:topic()) -> boolean().
match(Topic) ->
    has(Topic) orelse trie_match(Topic, get_trie(), #{'special_$' => true}).

-spec match(emqx_types:topic(), no_special_leading_dollar) -> boolean().
match(Topic, no_special_leading_dollar) ->
    has(Topic) orelse trie_match(Topic, get_trie(), #{'special_$' => false}).

-spec add(emqx_types:topic()) -> ok.
add(Topic) ->
    gen_server:call(?MODULE, {add_topic, Topic}, ?TIMEOUT).

-spec del(emqx_types:topic()) -> ok.
del(Topic) ->
    gen_server:call(?MODULE, {del_topic, Topic}, ?TIMEOUT).

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

handle_call({add_topic, Topic}, _From, State) ->
    ok = mnesia:dirty_write(?TOPICS,
            #dummy_topics{
                topic = Topic,
                created_at = erlang:system_time(millisecond)
            }),
    {reply, set_trie(trie_add(Topic, get_trie())), State};

handle_call({del_topic, Topic}, _From, State) ->
    ok = mnesia:dirty_delete(?TOPICS, Topic),
    {reply, set_trie(trie_del(Topic, get_trie())), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    persistent_term:erase(?TIRE),
    mnesia:clear_table(?TOPICS),
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

%% Trie is only for match topics WITH wildcards, that is, '+' and '#'.
%% We use persistent_term for the trie implementation as the user would
%% unlikely add/delete dummy topics too frequently using APIs/CLIs.
%%
%% persistent_term lookup (using get/1), is done in constant time and
%% without taking any locks, and the term is not copied to the heap
%% (as is the case with terms stored in ETS tables).
%%
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
                  counters => Cnt#{W => (N - 1)}}
    end;
do_trie_del([], Trie) ->
    Trie.

trie_match(_Topic, #{counters := nil}, _) ->
    false;
trie_match(Topic, Trie, #{'special_$' := false}) ->
    do_trie_match(emqx_topic:words(Topic), Trie);
trie_match(Topic, Trie, #{'special_$' := true}) ->
    %% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901246
    %% ## 4.7.2  Topics beginning with $
    %% The Server MUST NOT match Topic Filters starting with a wildcard
    %% character (# or +) with Topic Names beginning with a $ character.
    case emqx_topic:words(Topic) of
        [<<$$, _/binary>> = W | Words] ->
            case maps:find(W, Trie) of
                {ok, SubTrie} -> do_trie_match(Words, SubTrie);
                error -> false
            end;
        Words ->
            do_trie_match(Words, Trie)
    end.

do_trie_match([], #{counters := nil}) ->
    true;
do_trie_match([], #{'#' := _}) ->
    true;
do_trie_match([], _Trie) ->
    false;
do_trie_match(_Words, #{counters := nil}) ->
    false;
do_trie_match([W | Words], Trie) ->
    case match_w(W, Trie) of
        ok -> true;
        {ok, SubTrie} -> do_trie_match(Words, SubTrie);
        error -> false
    end.

match_w(W, Trie) ->
    case maps:find(W, Trie) of
        {ok, _} when W == '#' -> ok;
        {ok, _} = Res -> Res;
        error when W == '#' -> match_w('+', Trie);
        error when W == '+' -> error;
        error -> match_w('#', Trie)
    end.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

incr(Word, nil) -> #{Word => 1};
incr(Word, Counters) when is_map(Counters) ->
    Counters#{Word => maps:get(Word, Counters, 0) + 1}.
