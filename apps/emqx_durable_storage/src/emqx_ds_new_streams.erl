%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module can be used by DS backends for notifying processes
%% about new streams. It's not a replacement for `emqx_ds:get_streams'
%% function, it's only meant to optimize its usage.
%%
%% `emqx_ds_new_streams' module tries to avoid waking up subscribers
%% too often. It's done like this:
%%
%% This module keeps a list of subscriptions, records that have an
%% "active" flag. Whenever it receives a notification about a new
%% stream, it matches all active subscriptions' topic-filters against
%% the topic-filter of the event, and sets `active' flags for every
%% match.
%%
%% Independently, it runs a loop that searches for subscriptions with
%% `active' flag set to `true', and sends events to their owners.
%% After sending the event, it resets the flag to `false'.
%%
%% Dispatching the events is done in chunks (configurable by
%% `emqx_durable_storage.new_streams_batch_size' application
%% environment variable), with a cooldown in between (configurable by
%% `emqx_durable_storage.new_streams_cooldown').
%%
%% This is done to avoid a storm of `emqx_ds:get_streams' calls from
%% the clients.
-module(emqx_ds_new_streams).

-behaviour(gen_statem).

%% API:
-export([start_link/1, where/1]).
-export([watch/2, unwatch/2]).
-export([notify_new_stream/2, set_dirty/1]).

-export_type([watch/0]).

%% Local API and RPC targets
-export([local_notify_new_stream/2, local_set_dirty/1]).

%% behavior callbacks:
-export([callback_mode/0, init/1, handle_event/4]).

%% For testing:
-export([list_subscriptions/1]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-opaque watch() :: reference().

-define(via(DB), {via, gproc, {n, l, {?MODULE, DB}}}).

%% States:
-define(clean, clean).
-define(dirty, dirty).

-type state() :: ?clean | ?dirty.

%% Timeouts:
-define(dirty_loop, dirty_loop).

-record(d, {
    db :: emqx_ds:db(),
    subs :: ets:tid(),
    trie :: ets:tid(),
    dispatch_iterator
}).

-type data() :: #d{}.

-record(sub, {id, tf, pid, active = false}).

-type sub() :: #sub{
    id :: watch(),
    tf :: emqx_ds:topic_filter(),
    pid :: pid(),
    active :: boolean()
}.

%% Calls and casts:
-record(watch_req, {topic_filter :: emqx_ds:topic_filter()}).
-record(unwatch_req, {ref :: watch()}).
-record(notify_req, {topic_filter :: emqx_ds:topic_filter()}).
-record(list_subs_req, {}).

%%================================================================================
%% API functions
%%================================================================================

-spec where(emqx_ds:db()) -> pid() | undefined.
where(DB) ->
    gproc:where({n, l, {?MODULE, DB}}).

%% @doc Process that calls this function will receive messages of type
%% `#new_stream_event{subref = Ref}' when new streams matching the
%% topic filters are created in the durable storage.
%%
%% Note: this function is not idempotent.
-spec watch(emqx_ds:db(), emqx_ds:topic_filter()) -> {ok, watch()} | {error, badarg}.
watch(DB, TopicFilter) ->
    gen_server:call(?via(DB), #watch_req{topic_filter = TopicFilter}).

-spec unwatch(emqx_ds:db(), watch()) -> ok.
unwatch(DB, Ref) ->
    gen_statem:call(?via(DB), #unwatch_req{ref = Ref}).

%% @doc Broadcast notification about appearance of new stream(s) to
%% all nodes.
-spec notify_new_stream(emqx_ds:db(), emqx_ds:topic_filter()) -> ok.
notify_new_stream(DB, TF) ->
    emqx_ds_new_streams_proto_v1:notify([node() | nodes()], DB, TF).

%% @doc Send notification about appearancoe of new streams to local
%% processes.
-spec local_notify_new_stream(emqx_ds:db(), emqx_ds:topic_filter()) -> ok.
local_notify_new_stream(DB, TF) ->
    gen_statem:cast(?via(DB), #notify_req{topic_filter = TF}).

%% @doc Backend can use this function when it's uncertain that
%% notifications were delivered or what streams are new. This can
%% happen, for example, after the backend restarts.
%%
%% This function will notify ALL subscribers on all nodes.
-spec set_dirty(emqx_ds:db()) -> ok.
set_dirty(DB) ->
    emqx_ds_new_streams_proto_v1:set_dirty([node() | nodes()], DB).

%% @doc Used in cases when it's uncertain what streams were seen by
%% the subscribers, e.g. after restart of the shard. It will
%% gracefully notify subscribers about changes to _all_ stream on the
%% local node.
-spec local_set_dirty(emqx_ds:db()) -> ok.
local_set_dirty(DB) ->
    gen_statem:cast(?via(DB), #notify_req{topic_filter = ['#']}).

%%================================================================================
%% Internal exports
%%================================================================================

list_subscriptions(DB) ->
    gen_statem:call(?via(DB), #list_subs_req{}).

-spec start_link(emqx_ds:db()) -> {ok, pid()}.
start_link(DB) ->
    gen_statem:start_link(?via(DB), ?MODULE, [DB], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() -> [handle_event_function, state_enter].

init([DB]) ->
    D = #d{
        db = DB,
        trie = trie_new(),
        subs = subs_new()
    },
    {ok, ?clean, D}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) ->
    gen_statem:event_handler_result(state()).
handle_event({call, From}, #watch_req{topic_filter = TF}, _State, Data) ->
    Reply = handle_watch(From, TF, Data),
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event({call, From}, #unwatch_req{ref = Ref}, _State, Data) ->
    Reply = do_unwatch(Ref, Data),
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event(info, {'DOWN', MRef, process, _Pid, _Info}, _State, Data) ->
    _ = do_unwatch(MRef, Data),
    keep_state_and_data;
handle_event({call, From}, #list_subs_req{}, _State, #d{subs = Subs}) ->
    Reply = ets:tab2list(Subs),
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event(cast, #notify_req{topic_filter = TF}, State, Data) ->
    HasMatches = mark_active(TF, Data),
    case State of
        ?clean when HasMatches ->
            {next_state, ?dirty, Data};
        _ ->
            keep_state_and_data
    end;
handle_event(enter, _OldState, ?dirty, Data) ->
    enter_dirty(Data);
handle_event(state_timeout, ?dirty_loop, ?dirty, Data) ->
    dirty_loop(Data);
handle_event(enter, _OldState, ?clean, _Data) ->
    keep_state_and_data;
handle_event(EventType, Event, State, Data) ->
    ?tp(
        warning,
        ds_new_streams_unexpected_event,
        #{
            event_type => EventType,
            event => Event,
            state => State,
            data => Data
        }
    ),
    keep_state_and_data.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

enter_dirty(Data) ->
    {keep_state, Data, [{state_timeout, cfg_cooldown(), ?dirty_loop}]}.

dirty_loop(Data = #d{dispatch_iterator = undefined, subs = Subs}) ->
    %% This is beginning of the loop.
    MS = {#sub{_ = '_', active = true}, [], ['$_']},
    case ets:select(Subs, [MS], cfg_batch_size()) of
        '$end_of_table' ->
            %% There are no dirty subscriptions, go back to sleep:
            {next_state, ?clean, Data};
        {Matches, It} ->
            dispatch(Matches, It, Data)
    end;
dirty_loop(Data0 = #d{dispatch_iterator = It0}) ->
    %% This is continuation of the loop:
    case ets:select(It0) of
        '$end_of_table' ->
            %% We reached the end of table, but new dirty subs could
            %% be added at the beginning of the table while we were
            %% traversing. Restart the loop:
            Data = Data0#d{dispatch_iterator = undefined},
            dirty_loop(Data);
        {Matches, It} ->
            dispatch(Matches, It, Data0)
    end.

-spec dispatch([sub()], _EtsContinuation, data()) ->
    gen_statem:event_handler_result(state()).
dispatch(Matches, It, Data0 = #d{subs = Subs}) ->
    lists:foreach(
        fun(#sub{id = Ref, pid = Pid}) ->
            ets:update_element(Subs, Ref, {#sub.active, false}),
            Pid ! #new_stream_event{subref = Ref}
        end,
        Matches
    ),
    Data = Data0#d{dispatch_iterator = It},
    {keep_state, Data, [{state_timeout, cfg_cooldown(), ?dirty_loop}]}.

handle_watch({Pid, _}, TopicFilter, Data) ->
    MRef = monitor(process, Pid),
    try
        Sub = #sub{id = MRef, tf = TopicFilter, pid = Pid},
        _ = insert(Sub, Data),
        {ok, MRef}
    catch
        EC:Err:Stack ->
            demonitor(MRef, [flush]),
            ?tp(
                error,
                ds_new_streams_failed_to_insert,
                #{EC => Err, pid => Pid, tf => TopicFilter, stacktrace => Stack}
            ),
            {error, badarg}
    end.

mark_active(TopicFilter, #d{trie = Trie, subs = Subs}) ->
    case matches(TopicFilter, Trie) of
        [] ->
            false;
        Matches ->
            [
                ets:update_element(Subs, emqx_trie_search:get_id(Match), {#sub.active, true})
             || Match <- Matches
            ],
            true
    end.

trie_new() ->
    ets:new(trie, [private, ordered_set]).

subs_new() ->
    ets:new(subs, [private, ordered_set, {keypos, #sub.id}]).

insert(Record = #sub{tf = Filter, id = Ref}, #d{trie = Trie, subs = Subs}) ->
    true = ets:insert(Subs, Record),
    %% Update the trie:
    TrieKey = emqx_trie_search:make_key(Filter, Ref),
    true = ets:insert(Trie, {TrieKey, Ref}),
    TrieKey.

do_unwatch(Ref, #d{trie = Trie, subs = Subs}) ->
    case ets:take(Subs, Ref) of
        [#sub{tf = TopicFilter}] ->
            demonitor(Ref, [flush]),
            TrieKey = emqx_trie_search:make_key(TopicFilter, Ref),
            ets:delete(Trie, TrieKey);
        [] ->
            false
    end.

matches(Filter, Trie) ->
    emqx_trie_search:matches_filter(Filter, nextf(Trie), []).

nextf(Trie) ->
    fun(Key) ->
        ets:next(Trie, Key)
    end.

%% Configuration:

cfg_batch_size() ->
    application:get_env(emqx_durable_storage, new_streams_batch_size, 100).

cfg_cooldown() ->
    application:get_env(emqx_durable_storage, new_streams_cooldown, 5).

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

-define(assertSameSet(A, B), ?assertEqual(lists:sort(A), lists:sort(B))).

%% Test simple matching of topic without wildcards against the trie:
match_topic_test() ->
    Trie = trie_new(),
    D = #d{trie = Trie, subs = subs_new()},
    K1 = insert(#sub{tf = [<<"foo">>, '+'], id = k1}, D),
    K2 = insert(#sub{tf = [<<"foo">>, <<"1">>], id = k2}, D),
    K3 = insert(#sub{tf = [<<"foo">>, <<"1">>, '+'], id = k3}, D),
    ?assertSameSet(
        [K1, K2],
        matches([<<"foo">>, <<"1">>], Trie)
    ),
    ?assertSameSet(
        [K1],
        matches([<<"foo">>, <<"2">>], Trie)
    ),
    ?assertSameSet(
        [K3],
        matches([<<"foo">>, <<"1">>, <<"2">>], Trie)
    ),
    ?assertSameSet(
        [],
        matches([<<"foo">>, <<"2">>, <<"2">>], Trie)
    ).

%% Test matching of topic filter with wildcards against the trie:
match_filter_test() ->
    Trie = trie_new(),
    D = #d{trie = Trie, subs = subs_new()},
    K1 = insert(#sub{tf = [<<"foo">>, '+'], id = k1}, D),
    K2 = insert(#sub{tf = [<<"foo">>, <<"1">>], id = k2}, D),
    K3 = insert(#sub{tf = [<<"foo">>, <<"1">>, '+'], id = k3}, D),
    K4 = insert(#sub{tf = [<<"bar">>], id = k4}, D),
    ?assertSameSet(
        [K1, K2],
        matches([<<"foo">>, '+'], Trie)
    ),
    ?assertSameSet(
        [K1, K2, K3],
        matches([<<"foo">>, '#'], Trie)
    ),
    ?assertSameSet(
        [K1, K2, K3, K4],
        matches(['#'], Trie)
    ),
    ?assertSameSet(
        [K3],
        matches(['+', '+', <<"1">>], Trie)
    ),
    ?assertSameSet(
        [K3],
        matches(['+', <<"1">>, '+'], Trie)
    ),
    ?assertSameSet(
        [],
        matches(['+', <<"2">>, '+'], Trie)
    ).

-endif.
