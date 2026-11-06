%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mq_ds_queue_deleter).
-moduledoc "Delete one queue's data in bounded batches across its slabs".
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([delete/2]).

-define(PAUSE, 1000).
-define(ERROR_LIMIT, 5).
-type deletion_error() :: {emqx_ds:slab(), emqx_ds:topic_filter(), emqx_ds:error(term())}.

-record(entry, {
    slab :: emqx_ds:slab(),
    stream :: emqx_ds:stream(),
    topic :: emqx_ds:topic_filter(),
    errors = 0 :: non_neg_integer()
}).
-record(retry, {
    tag :: reference(),
    entry :: #entry{}
}).
-record(st, {
    db :: emqx_ds:db(),
    tag = make_ref() :: reference(),
    refs = #{} :: #{reference() => #entry{}},
    retries = #{} :: #{#entry{} => reference()},
    errors = [] :: [deletion_error()]
}).

-doc "Delete all matching data".
-spec delete(emqx_ds:db(), [emqx_ds:topic_filter()]) ->
    ok
    | {error, [deletion_error()]}
    | {error, {stream_lookup, term()}}.
delete(DB, Topics) ->
    case discover(DB, Topics, []) of
        {ok, Entries} ->
            loop(lists:foldl(fun send/2, #st{db = DB}, Entries));
        {error, _} = Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

discover(_, [], Entries) ->
    {ok, Entries};
discover(DB, [Topic | Rest], Entries) ->
    case emqx_ds:get_streams(DB, Topic, 0, #{}) of
        {Streams, []} ->
            New = [
                #entry{slab = Slab, stream = Stream, topic = Topic}
             || {Slab, Stream} <- Streams
            ],
            discover(DB, Rest, New ++ Entries);
        {_, Errors} ->
            {error, {stream_lookup, Errors}}
    end.

send(#entry{stream = Stream, topic = Topic} = Entry, #st{db = DB, refs = Refs} = State) ->
    Opts = #{sync => false, dirty => true},
    case emqx_ds:del_batch(DB, Stream, Topic, 0, infinity, Opts) of
        {async, Ref} -> State#st{refs = Refs#{Ref => Entry}};
        Outcome -> outcome(Entry, Outcome, State)
    end.

loop(#st{refs = Refs, retries = Retries, errors = Errors}) when
    map_size(Refs) =:= 0 andalso map_size(Retries) =:= 0
->
    case Errors of
        [] -> ok;
        _ -> {error, lists:reverse(Errors)}
    end;
loop(#st{db = DB, tag = Tag, refs = Refs, retries = Retries} = State) ->
    %% DS sends a timeout reply for each request that does not complete.
    receive
        ?ds_tx_commit_reply(Ref, Reply) when is_map_key(Ref, Refs) ->
            Entry = maps:get(Ref, Refs),
            Outcome = emqx_ds:del_batch_outcome(DB, Ref, Reply),
            loop(outcome(Entry, Outcome, State#st{refs = maps:remove(Ref, Refs)}));
        #retry{tag = Tag, entry = Entry} when is_map_key(Entry, Retries) ->
            loop(send(Entry, State#st{retries = maps:remove(Entry, Retries)}))
    end.

outcome(#entry{slab = Slab, topic = Topic} = Entry, Outcome, State) ->
    ?tp(mq_queue_delete_batch, #{slab => Slab, topic => Topic, result => Outcome}),
    handle_outcome(Entry, Outcome, State).

handle_outcome(Entry, {ok, true}, State) ->
    send(Entry, State);
handle_outcome(_, {ok, false}, State) ->
    State;
handle_outcome(_, {error, unrecoverable, {storage_not_found, _}}, State) ->
    State;
handle_outcome(
    #entry{errors = N} = Entry0, {error, recoverable, _}, #st{tag = Tag, retries = Retries} = State
) when
    N + 1 < ?ERROR_LIMIT
->
    Entry = Entry0#entry{errors = N + 1},
    Timer = erlang:send_after(?PAUSE, self(), #retry{tag = Tag, entry = Entry}),
    State#st{retries = Retries#{Entry => Timer}};
handle_outcome(Entry, Error, State) ->
    fail(Entry, Error, State).

fail(#entry{slab = Slab, topic = Topic}, Error, #st{errors = Errors} = State) ->
    State#st{errors = [{Slab, Topic, Error} | Errors]}.
