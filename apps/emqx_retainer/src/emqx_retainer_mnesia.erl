%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_mnesia).

-behaviour(emqx_db_backup).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(emqx_retainer).
-export([
    create/1,
    update/2,
    close/1,
    delete_message/2,
    store_retained/2,
    read_message/2,
    page_read/5,
    match_messages/3,
    delete_cursor/2,
    clean/1,
    size/1
]).

-behaviour(emqx_retainer_gc).
-export([
    clear_expired/3
]).

%% Internal exports (RPC)
-export([
    do_populate_index_meta/1,
    do_reindex_batch/2,
    active_indices/0
]).

%% Management API:
-export([topics/0]).

-export([reindex/2, reindex_status/0]).

-export([populate_index_meta/0]).
-export([reindex/3]).

-export([
    backup_tables/0,
    on_backup_table_imported/2
]).

-record(retained_message, {topic, msg, expiry_time}).
-record(retained_index, {key, expiry_time}).
-record(retained_index_meta, {key, read_indices, write_indices, reindexing, extra}).

-define(META_KEY, index_meta).

-define(REINDEX_BATCH_SIZE, 1000).
-define(REINDEX_DISPATCH_WAIT, 30000).
-define(REINDEX_RPC_RETRY_INTERVAL, 1000).
-define(REINDEX_INDEX_UPDATE_WAIT, 30000).

-define(MESSAGE_SCAN_BATCH_SIZE, 100).

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

topics() ->
    [emqx_topic:join(I) || I <- mnesia:dirty_all_keys(?TAB_MESSAGE)].

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() ->
    {<<"builtin_retainer">>, tables_to_backup()}.

tables_to_backup() ->
    %% `backup_tables' is inspected to construct API docs (available table sets), and such
    %% docs are built in `emqx_conf:dump_schema', while there's no started node.
    try
        [?TAB_MESSAGE || is_enabled()]
    catch
        exit:{noproc, _} ->
            []
    end.

on_backup_table_imported(?TAB_MESSAGE, Opts) ->
    case is_enabled() of
        true ->
            maybe_print("Starting reindexing retained messages ~n", [], Opts),
            Res = reindex(false, mk_status_fun(Opts)),
            maybe_print("Reindexing retained messages finished~n", [], Opts),
            Res;
        false ->
            ok
    end;
on_backup_table_imported(_Tab, _Opts) ->
    ok.

mk_status_fun(Opts) ->
    fun(Done) ->
        log_status(Done),
        maybe_print("Reindexed ~p messages~n", [Done], Opts)
    end.

maybe_print(Fmt, Args, #{print_fun := Fun}) when is_function(Fun, 2) ->
    Fun(Fmt, Args);
maybe_print(_Fmt, _Args, _Opts) ->
    ok.

log_status(Done) ->
    ?SLOG(
        info,
        #{
            msg => "retainer_message_record_reindexing_progress",
            done => Done
        }
    ).

is_enabled() ->
    emqx_retainer:enabled() andalso emqx_retainer:backend_module() =:= ?MODULE.

%%--------------------------------------------------------------------
%% emqx_retainer callbacks
%%--------------------------------------------------------------------

create(#{storage_type := StorageType, max_retained_messages := MaxRetainedMessages} = Config) ->
    ok = create_table(
        ?TAB_INDEX_META,
        retained_index_meta,
        record_info(fields, retained_index_meta),
        set,
        StorageType
    ),
    ok = populate_index_meta(Config),
    ok = create_table(
        ?TAB_MESSAGE,
        retained_message,
        record_info(fields, retained_message),
        ordered_set,
        StorageType
    ),
    ok = create_table(
        ?TAB_INDEX,
        retained_index,
        record_info(fields, retained_index),
        ordered_set,
        StorageType
    ),
    #{storage_type => StorageType, max_retained_messages => MaxRetainedMessages}.

create_table(Table, RecordName, Attributes, Type, StorageType) ->
    Copies =
        case StorageType of
            ram -> ram_copies;
            disc -> disc_copies
        end,

    StoreProps = [
        {ets, [
            compressed,
            {read_concurrency, true},
            {write_concurrency, true}
        ]},
        {dets, [{auto_save, 1000}]}
    ],

    ok = mria:create_table(Table, [
        {type, Type},
        {rlog_shard, ?RETAINER_SHARD},
        {storage, Copies},
        {record_name, RecordName},
        {attributes, Attributes},
        {storage_properties, StoreProps}
    ]),
    ok = mria_rlog:wait_for_shards([?RETAINER_SHARD], infinity),
    ok = mria:wait_for_tables([Table]),
    case mnesia:table_info(Table, storage_type) of
        Copies ->
            ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(Table, node(), Copies),
            ok
    end.

update(_State, _NewConfig) ->
    need_recreate.

close(_State) -> ok.

store_retained(State, Msg = #message{topic = Topic}) ->
    ExpiryTime = emqx_retainer:get_expiry_time(Msg),
    Tokens = topic_to_tokens(Topic),
    case is_table_full(State) andalso is_new_topic(Tokens) of
        true ->
            ?SLOG_THROTTLE(
                error,
                #{
                    msg => failed_to_retain_message,
                    reason => table_is_full
                },
                #{topic => Topic}
            ),
            ok;
        false ->
            do_store_retained(Msg, Tokens, ExpiryTime),
            ?tp(message_retained, #{topic => Topic}),
            ok
    end.

clear_expired(_State, Deadline, Limit) ->
    S0 = ets_stream(?TAB_MESSAGE),
    AllowNeverExpire = emqx_conf:get([retainer, allow_never_expire]),
    FilterFn =
        case emqx_conf:get([retainer, msg_expiry_interval_override]) of
            disabled ->
                fun(#retained_message{expiry_time = ExpiryTime}) ->
                    ExpiryTime =/= 0 andalso ExpiryTime < Deadline
                end;
            OverrideMS ->
                fun(
                    #retained_message{
                        expiry_time = StoredExpiryTime,
                        msg = #message{timestamp = Ts}
                    }
                ) ->
                    case StoredExpiryTime of
                        0 when not AllowNeverExpire ->
                            Ts + OverrideMS < Deadline;
                        0 ->
                            false;
                        _ ->
                            min(Ts + OverrideMS, StoredExpiryTime) < Deadline
                    end
                end
        end,
    S1 = emqx_utils_stream:filter(FilterFn, S0),
    DirtyWriteIndices = dirty_indices(write),
    S2 = emqx_utils_stream:map(
        fun(RetainedMsg) ->
            delete_message_with_indices(RetainedMsg, DirtyWriteIndices)
        end,
        S1
    ),
    CountF = fun(_, N) -> N + 1 end,
    case Limit of
        all ->
            NCleared = emqx_utils_stream:fold(CountF, 0, S2),
            {_Complete = true, NCleared};
        Num ->
            {NCleared, SRest} = emqx_utils_stream:fold(CountF, 0, Num, S2),
            Complete = SRest == [],
            {Complete, NCleared}
    end.

delete_message(_State, Topic) ->
    Tokens = topic_to_tokens(Topic),
    case emqx_topic:wildcard(Topic) of
        false ->
            ok = delete_message_by_topic(Tokens, dirty_indices(write));
        true ->
            S = search_stream(Tokens, 0),
            DirtyWriteIndices = dirty_indices(write),
            emqx_utils_stream:foreach(
                fun(RetainedMsg) -> delete_message_with_indices(RetainedMsg, DirtyWriteIndices) end,
                S
            )
    end.

read_message(_State, Topic) ->
    {ok, read_messages(Topic)}.

match_messages(State, Topic, undefined) ->
    Tokens = topic_to_tokens(Topic),
    Now = erlang:system_time(millisecond),
    S = msg_stream(search_stream(Tokens, Now)),
    case batch_read_number() of
        all_remaining ->
            {ok, emqx_utils_stream:consume(S), undefined};
        BatchNum when is_integer(BatchNum) ->
            match_messages(State, Topic, {S, BatchNum})
    end;
match_messages(_State, _Topic, {S0, BatchNum}) ->
    case emqx_utils_stream:consume(BatchNum, S0) of
        {Rows, S1} ->
            {ok, Rows, {S1, BatchNum}};
        Rows when is_list(Rows) ->
            {ok, Rows, undefined}
    end.

delete_cursor(_State, _Cursor) ->
    ok.

page_read(_State, Topic, Deadline, Page, Limit) ->
    S0 =
        case Topic of
            undefined ->
                msg_stream(all_stream(Deadline));
            _ ->
                Tokens = topic_to_tokens(Topic),
                msg_stream(search_stream(Tokens, Deadline))
        end,
    %% This is very inefficient, but we are limited with inherited API
    S1 = emqx_utils_stream:list(
        lists:sort(
            fun compare_message/2,
            emqx_utils_stream:consume(S0)
        )
    ),
    NSkip = (Page - 1) * Limit,
    S2 = emqx_utils_stream:drop(NSkip, S1),
    case emqx_utils_stream:consume(Limit, S2) of
        {Rows, _S3} ->
            {ok, true, Rows};
        Rows when is_list(Rows) ->
            {ok, false, Rows}
    end.

clean(_) ->
    _ = mria:clear_table(?TAB_MESSAGE),
    _ = mria:clear_table(?TAB_INDEX),
    ok.

size(_) ->
    table_size().

reindex(Force, StatusFun) ->
    Config = emqx:get_config([retainer, backend]),
    reindex(config_indices(Config), Force, StatusFun).

reindex_status() ->
    case mnesia:dirty_read(?TAB_INDEX_META, ?META_KEY) of
        [#retained_index_meta{reindexing = true}] ->
            true;
        _ ->
            false
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_store_retained(Msg, TopicTokens, ExpiryTime) ->
    %% Retained message is stored syncronously on all core nodes
    %%
    %% No transaction, meaning that concurrent writes in the cluster may
    %% lead to inconsistent replicas. This could manifest in two clients
    %% getting different retained messages for the same topic, depending
    %% on which node they are connected to. We tolerate that.
    ok = do_store_retained_message(Msg, TopicTokens, ExpiryTime),
    %% Since retained message was stored syncronously on all core nodes,
    %% now we are sure that
    %% * either we will write correct indices
    %% * or if we a replicant with outdated write indices due to reindexing,
    %%   the correct indices will be added by reindexing
    %%
    %% No transacation as well, meaning that concurrent writes in the cluster
    %% may lead to inconsistent index replicas. This essentially allows for
    %% inconsistent query results, where index entry has different expiry time
    %% than the message it points to.
    ok = do_store_retained_indices(TopicTokens, ExpiryTime).

do_store_retained_message(Msg, TopicTokens, ExpiryTime) ->
    RetainedMessage = #retained_message{
        topic = TopicTokens,
        msg = Msg,
        expiry_time = ExpiryTime
    },
    ok = mria:dirty_write_sync(?TAB_MESSAGE, RetainedMessage).

do_store_retained_indices(TopicTokens, ExpiryTime) ->
    Indices = dirty_indices(write),
    ok = mria:async_dirty(?RETAINER_SHARD, fun() ->
        emqx_retainer_index:foreach_index_key(
            fun(Key) -> do_store_retained_index(Key, ExpiryTime) end,
            Indices,
            TopicTokens
        )
    end).

do_store_retained_index(Key, ExpiryTime) ->
    RetainedIndex = #retained_index{
        key = Key,
        expiry_time = ExpiryTime
    },
    mnesia:write(?TAB_INDEX, RetainedIndex, write).

msg_stream(SearchStream) ->
    emqx_utils_stream:map(
        fun(#retained_message{msg = Msg}) -> Msg end,
        SearchStream
    ).

search_stream(Tokens, Now) ->
    Indices = dirty_indices(read),
    Index = emqx_retainer_index:select_index(Tokens, Indices),
    search_stream(Index, Tokens, Now).

all_stream(Now) ->
    Ms = make_message_match_spec(Now),
    emqx_utils_stream:ets(
        fun
            (undefined) -> ets:select(?TAB_MESSAGE, Ms, 1);
            (Cont) -> ets:select(Cont)
        end
    ).

search_stream(undefined, Tokens, Now) ->
    Ms = make_message_match_spec(Tokens, Now),
    MsgStream = emqx_utils_stream:ets(
        fun
            (undefined) -> ets:select(?TAB_MESSAGE, Ms, 1);
            (Cont) -> ets:select(Cont)
        end
    ),
    case Tokens of
        %% NOTE: Can not match only with $SPECIAL topics [MQTT-4.7.2-1].
        [T | _] when T == '+' orelse T == '#' ->
            emqx_utils_stream:filter(
                fun(#retained_message{topic = [TopicToken | _]}) ->
                    emqx_topic:match([TopicToken], [T])
                end,
                MsgStream
            );
        _ ->
            MsgStream
    end;
search_stream(Index, FilterTokens, Now) ->
    {Ms, IsExactMs} = make_index_match_spec(Index, FilterTokens, Now),
    IndexRecordStream = emqx_utils_stream:ets(
        fun
            (undefined) -> ets:select(?TAB_INDEX, Ms, 1);
            (Cont) -> ets:select(Cont)
        end
    ),
    TopicStream = emqx_utils_stream:map(
        fun(#retained_index{key = Key}) -> emqx_retainer_index:restore_topic(Key) end,
        IndexRecordStream
    ),
    MatchingTopicStream = emqx_utils_stream:filter(
        fun(TopicTokens) -> match(IsExactMs, TopicTokens, FilterTokens) end,
        TopicStream
    ),
    RetainMsgStream = emqx_utils_stream:chainmap(
        fun(TopicTokens) -> emqx_utils_stream:list(ets:lookup(?TAB_MESSAGE, TopicTokens)) end,
        MatchingTopicStream
    ),
    ValidRetainMsgStream = emqx_utils_stream:filter(
        fun(#retained_message{expiry_time = ExpiryTime}) ->
            ExpiryTime =:= 0 orelse ExpiryTime > Now
        end,
        RetainMsgStream
    ),
    ValidRetainMsgStream.

match(_IsExactMs = true, [TopicToken | _], [FilterToken | _]) ->
    %% NOTE: Can not match only with $SPECIAL topics [MQTT-4.7.2-1].
    emqx_topic:match([TopicToken], [FilterToken]);
match(_IsExactMs = false, TopicTokens, FilterTokens) ->
    emqx_topic:match(TopicTokens, FilterTokens).

delete_message_by_topic(TopicTokens, Indices) ->
    case mnesia:dirty_read(?TAB_MESSAGE, TopicTokens) of
        [] -> ok;
        [RetainedMsg] -> delete_message_with_indices(RetainedMsg, Indices)
    end.

delete_message_with_indices(RetainedMsg, Indices) ->
    #retained_message{topic = TopicTokens, expiry_time = ExpiryTime} = RetainedMsg,
    ok = emqx_retainer_index:foreach_index_key(
        fun(Key) ->
            mria:dirty_delete_object(?TAB_INDEX, #retained_index{
                key = Key, expiry_time = ExpiryTime
            })
        end,
        Indices,
        TopicTokens
    ),
    ok = mria:dirty_delete_object(?TAB_MESSAGE, RetainedMsg).

compare_message(M1, M2) ->
    M1#message.timestamp =< M2#message.timestamp.

topic_to_tokens(Topic) ->
    emqx_topic:words(Topic).

-spec read_messages(emqx_types:topic()) ->
    [emqx_types:message()].
read_messages(Topic) ->
    Tokens = topic_to_tokens(Topic),
    case mnesia:dirty_read(?TAB_MESSAGE, Tokens) of
        [] ->
            [];
        [#retained_message{msg = Msg, expiry_time = Et}] ->
            case Et =:= 0 orelse Et >= erlang:system_time(millisecond) of
                true -> [Msg];
                false -> []
            end
    end.

make_message_match_spec(NowMs) ->
    MsHd = #retained_message{topic = '_', msg = '_', expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}].

make_message_match_spec(Tokens, NowMs) ->
    Cond = emqx_retainer_index:condition(Tokens),
    MsHd = #retained_message{topic = Cond, msg = '_', expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}].

make_index_match_spec(Index, Tokens, NowMs) ->
    {Cond, IsExact} = emqx_retainer_index:condition(Index, Tokens),
    MsHd = #retained_index{key = Cond, expiry_time = '$3'},
    {[{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}], IsExact}.

is_table_full(#{max_retained_messages := MaxRetainedMessages} = _State) ->
    MaxRetainedMessages > 0 andalso (table_size() >= MaxRetainedMessages).

is_new_topic(Tokens) ->
    case mnesia:dirty_read(?TAB_MESSAGE, Tokens) of
        [_] ->
            false;
        [] ->
            true
    end.

table_size() ->
    mnesia:table_info(?TAB_MESSAGE, size).

config_indices(#{index_specs := IndexSpecs}) ->
    IndexSpecs.

populate_index_meta() ->
    Config = emqx:get_config([retainer, backend]),
    populate_index_meta(Config).

populate_index_meta(Config) ->
    ConfigIndices = config_indices(Config),
    case mria:transaction(?RETAINER_SHARD, fun ?MODULE:do_populate_index_meta/1, [ConfigIndices]) of
        {atomic, ok} ->
            ok;
        {atomic, {error, DBWriteIndices, DBReadIndices}} ->
            ?SLOG(warning, #{
                msg => "emqx_retainer_outdated_indices",
                config_indices => ConfigIndices,
                db_write_indices => DBWriteIndices,
                db_read_indices => DBReadIndices
            }),
            ok;
        {aborted, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_populate_emqx_retainer_indices",
                reason => Reason
            }),
            {error, Reason}
    end.

do_populate_index_meta(ConfigIndices) ->
    case mnesia:read(?TAB_INDEX_META, ?META_KEY, write) of
        [
            #retained_index_meta{
                read_indices = ReadIndices,
                write_indices = WriteIndices,
                reindexing = Reindexing
            }
        ] ->
            case {ReadIndices, WriteIndices, Reindexing} of
                {_, _, true} ->
                    ok;
                {ConfigIndices, ConfigIndices, false} ->
                    ok;
                {DBWriteIndices, DBReadIndices, false} ->
                    {error, DBWriteIndices, DBReadIndices}
            end;
        [] ->
            mnesia:write(
                ?TAB_INDEX_META,
                #retained_index_meta{
                    key = ?META_KEY,
                    read_indices = ConfigIndices,
                    write_indices = ConfigIndices,
                    reindexing = false
                },
                write
            )
    end.

dirty_indices(Type) ->
    indices(ets:lookup(?TAB_INDEX_META, ?META_KEY), Type).

db_indices(Type) ->
    indices(mnesia:read(?TAB_INDEX_META, ?META_KEY), Type).

indices(IndexRecords, Type) ->
    case IndexRecords of
        [#retained_index_meta{read_indices = ReadIndices, write_indices = WriteIndices}] ->
            case Type of
                read -> ReadIndices;
                write -> WriteIndices
            end;
        [] ->
            []
    end.

batch_read_number() ->
    case emqx:get_config([retainer, flow_control, batch_read_number]) of
        0 -> all_remaining;
        BatchNum when is_integer(BatchNum) -> BatchNum
    end.

reindex(NewIndices, Force, StatusFun) when
    is_boolean(Force) andalso is_function(StatusFun, 1)
->
    %% Do not run on replicants
    core = mria_rlog:role(),
    %% Disable read indices and update write indices so that new records are written
    %% with correct indices. Also block parallel reindexing.
    case try_start_reindex(NewIndices, Force) of
        {atomic, ok} ->
            %% Wait for all nodes to have new indices, including rlog nodes
            true = wait_indices_updated({[], NewIndices}, ?REINDEX_INDEX_UPDATE_WAIT),

            %% Wait for all dispatch operations to be completed to avoid
            %% inconsistent results.
            true = wait_dispatch_complete(?REINDEX_DISPATCH_WAIT),

            %% All new dispatch operations will see
            %% indices disabled, so we feel free to clear index table.

            %% Clear old index records.
            {atomic, ok} = mria:clear_table(?TAB_INDEX),

            %% Fill index records in batches.
            TopicStream = emqx_utils_stream:map(
                fun(#retained_message{topic = Topic}) -> Topic end,
                ets_stream(?TAB_MESSAGE)
            ),
            ok = reindex_batch(TopicStream, 0, StatusFun),

            %% Enable read indices and unlock reindexing.
            finalize_reindex();
        {atomic, Reason} ->
            Reason
    end.

try_start_reindex(NewIndices, true) ->
    %% Note: we don't expect reindexing during upgrade, so this function is internal
    mria:transaction(
        ?RETAINER_SHARD,
        fun() -> start_reindex(NewIndices) end
    );
try_start_reindex(NewIndices, false) ->
    mria:transaction(
        ?RETAINER_SHARD,
        fun() ->
            case mnesia:read(?TAB_INDEX_META, ?META_KEY, write) of
                [#retained_index_meta{reindexing = false}] ->
                    start_reindex(NewIndices);
                _ ->
                    {error, already_started}
            end
        end
    ).

start_reindex(NewIndices) ->
    ?tp(warning, retainer_message_reindexing_started, #{
        hint => <<"during reindexing, subscription performance may degrade">>
    }),
    mnesia:write(
        ?TAB_INDEX_META,
        #retained_index_meta{
            key = ?META_KEY,
            read_indices = [],
            write_indices = NewIndices,
            reindexing = true
        },
        write
    ).

finalize_reindex() ->
    %% Note: we don't expect reindexing during upgrade, so this function is internal
    {atomic, ok} = mria:transaction(
        ?RETAINER_SHARD,
        fun() ->
            case mnesia:read(?TAB_INDEX_META, ?META_KEY, write) of
                [#retained_index_meta{write_indices = WriteIndices} = Meta] ->
                    mnesia:write(
                        ?TAB_INDEX_META,
                        Meta#retained_index_meta{
                            key = ?META_KEY,
                            read_indices = WriteIndices,
                            reindexing = false
                        },
                        write
                    );
                [] ->
                    ok
            end
        end
    ),
    ?tp(warning, retainer_message_reindexing_finished, #{}),
    ok.

reindex_topic(Indices, Topic) ->
    case mnesia:read(?TAB_MESSAGE, Topic, read) of
        [#retained_message{expiry_time = ExpiryTime}] ->
            ok = emqx_retainer_index:foreach_index_key(
                fun(Key) -> do_store_retained_index(Key, ExpiryTime) end,
                Indices,
                Topic
            );
        [] ->
            ok
    end.

reindex_batch(Stream0, Done, StatusFun) ->
    case mria:transaction(?RETAINER_SHARD, fun ?MODULE:do_reindex_batch/2, [Stream0, Done]) of
        {atomic, {more, NewDone, Stream1}} ->
            _ = StatusFun(NewDone),
            reindex_batch(Stream1, NewDone, StatusFun);
        {atomic, {done, NewDone}} ->
            _ = StatusFun(NewDone),
            ok;
        {aborted, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_reindex_retained_messages",
                reason => Reason
            }),
            {error, Reason}
    end.

do_reindex_batch(Stream0, Done) ->
    Indices = db_indices(write),
    Result = emqx_utils_stream:consume(?REINDEX_BATCH_SIZE, Stream0),
    Topics =
        case Result of
            {Rows, _Stream1} ->
                Rows;
            Rows when is_list(Rows) ->
                Rows
        end,
    ok = lists:foreach(
        fun(Topic) -> reindex_topic(Indices, Topic) end,
        Topics
    ),
    case Result of
        {_Rows, Stream1} ->
            {more, Done + length(Topics), Stream1};
        _Rows ->
            {done, Done + length(Topics)}
    end.

wait_dispatch_complete(Timeout) ->
    Nodes = mria:running_nodes(),
    {Results, []} = emqx_retainer_proto_v2:wait_dispatch_complete(Nodes, Timeout),
    lists:all(
        fun(Result) -> Result =:= ok end,
        Results
    ).

wait_indices_updated(_Indices, TimeLeft) when TimeLeft < 0 -> false;
wait_indices_updated(Indices, TimeLeft) ->
    case timer:tc(fun() -> are_indices_updated(Indices) end) of
        {_, true} ->
            true;
        {TimePassed, false} ->
            timer:sleep(?REINDEX_RPC_RETRY_INTERVAL),
            wait_indices_updated(
                Indices, TimeLeft - ?REINDEX_RPC_RETRY_INTERVAL - TimePassed / 1000
            )
    end.

active_indices() ->
    {dirty_indices(read), dirty_indices(write)}.

are_indices_updated(Indices) ->
    Nodes = mria:running_nodes(),
    case emqx_retainer_proto_v2:active_mnesia_indices(Nodes) of
        {Results, []} ->
            lists:all(
                fun(NodeIndices) -> NodeIndices =:= Indices end,
                Results
            );
        _ ->
            false
    end.

ets_stream(Tab) ->
    emqx_utils_stream:ets(
        fun
            (undefined) -> ets:match_object(Tab, '_', ?MESSAGE_SCAN_BATCH_SIZE);
            (Cont) -> ets:match_object(Cont)
        end
    ).
