%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(emqx_retainer).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% emqx_retainer callbacks
-export([
    delete_message/2,
    store_retained/2,
    read_message/2,
    page_read/4,
    match_messages/3,
    clear_expired/1,
    clean/1,
    size/1
]).

%% Internal exports (RPC)
-export([
    do_populate_index_meta/1,
    do_reindex_batch/2,
    active_indices/0
]).

%% Management API:
-export([topics/0]).

-export([create_resource/1]).

-export([reindex/2, reindex_status/0]).

-ifdef(TEST).
-export([populate_index_meta/0]).
-export([reindex/3]).
-endif.

-record(retained_message, {topic, msg, expiry_time}).
-record(retained_index, {key, expiry_time}).
-record(retained_index_meta, {key, read_indices, write_indices, reindexing, extra}).

-define(META_KEY, index_meta).

-define(CLEAR_BATCH_SIZE, 1000).
-define(REINDEX_BATCH_SIZE, 1000).
-define(REINDEX_DISPATCH_WAIT, 30000).
-define(REINDEX_RPC_RETRY_INTERVAL, 1000).
-define(REINDEX_INDEX_UPDATE_WAIT, 30000).

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

topics() ->
    [emqx_topic:join(I) || I <- mnesia:dirty_all_keys(?TAB_MESSAGE)].

%%--------------------------------------------------------------------
%% emqx_retainer callbacks
%%--------------------------------------------------------------------

create_resource(#{storage_type := StorageType}) ->
    ok = create_table(
        ?TAB_INDEX_META,
        retained_index_meta,
        record_info(fields, retained_index_meta),
        set,
        StorageType
    ),
    ok = populate_index_meta(),
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
    ).

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
    case mnesia:table_info(Table, storage_type) of
        Copies ->
            ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(Table, node(), Copies),
            ok
    end.

store_retained(_, Msg = #message{topic = Topic}) ->
    ExpiryTime = emqx_retainer:get_expiry_time(Msg),
    Tokens = topic_to_tokens(Topic),
    case is_table_full() andalso is_new_topic(Tokens) of
        true ->
            ?SLOG(error, #{
                msg => "failed_to_retain_message",
                topic => Topic,
                reason => table_is_full
            });
        false ->
            do_store_retained(Msg, Tokens, ExpiryTime)
    end.

clear_expired(_) ->
    NowMs = erlang:system_time(millisecond),
    QH = qlc:q([
        RetainedMsg
     || #retained_message{
            expiry_time = ExpiryTime
        } = RetainedMsg <- ets:table(?TAB_MESSAGE),
        (ExpiryTime =/= 0) and (ExpiryTime < NowMs)
    ]),
    QC = qlc:cursor(QH),
    clear_batch(dirty_indices(write), QC).

delete_message(_, Topic) ->
    Tokens = topic_to_tokens(Topic),
    case emqx_topic:wildcard(Topic) of
        false ->
            ok = delete_message_by_topic(Tokens, dirty_indices(write));
        true ->
            QH = search_table(Tokens, 0),
            qlc:fold(
                fun(RetainedMsg, _) ->
                    ok = delete_message_with_indices(RetainedMsg, dirty_indices(write))
                end,
                undefined,
                QH
            )
    end.

read_message(_, Topic) ->
    {ok, read_messages(Topic)}.

match_messages(_, Topic, undefined) ->
    Tokens = topic_to_tokens(Topic),
    Now = erlang:system_time(millisecond),
    QH = msg_table(search_table(Tokens, Now)),
    case batch_read_number() of
        all_remaining ->
            {ok, qlc:eval(QH), undefined};
        BatchNum when is_integer(BatchNum) ->
            Cursor = qlc:cursor(QH),
            match_messages(undefined, Topic, {Cursor, BatchNum})
    end;
match_messages(_, _Topic, {Cursor, BatchNum}) ->
    case qlc_next_answers(Cursor, BatchNum) of
        {closed, Rows} ->
            {ok, Rows, undefined};
        {more, Rows} ->
            {ok, Rows, {Cursor, BatchNum}}
    end.

page_read(_, Topic, Page, Limit) ->
    Now = erlang:system_time(millisecond),
    QH =
        case Topic of
            undefined ->
                msg_table(search_table(undefined, ['#'], Now));
            _ ->
                Tokens = topic_to_tokens(Topic),
                msg_table(search_table(Tokens, Now))
        end,
    OrderedQH = qlc:sort(QH, {order, fun compare_message/2}),
    Cursor = qlc:cursor(OrderedQH),
    NSkip = (Page - 1) * Limit,
    SkipResult =
        case NSkip > 0 of
            true ->
                {Result, _} = qlc_next_answers(Cursor, NSkip),
                Result;
            false ->
                more
        end,
    PageRows =
        case SkipResult of
            closed ->
                [];
            more ->
                case qlc_next_answers(Cursor, Limit) of
                    {closed, Rows} ->
                        Rows;
                    {more, Rows} ->
                        qlc:delete_cursor(Cursor),
                        Rows
                end
        end,
    {ok, PageRows}.

clean(_) ->
    _ = mria:clear_table(?TAB_MESSAGE),
    _ = mria:clear_table(?TAB_INDEX),
    ok.

size(_) ->
    table_size().

reindex(Force, StatusFun) ->
    reindex(config_indices(), Force, StatusFun).

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
    ok = do_store_retained_message(Msg, TopicTokens, ExpiryTime),
    %% Since retained message was stored syncronously on all core nodes,
    %% now we are sure that
    %% * either we will write correct indices
    %% * or if we a replicant with outdated write indices due to reindexing,
    %%   the correct indices will be added by reindexing
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
    ok = emqx_retainer_index:foreach_index_key(
        fun(Key) -> do_store_retained_index(Key, ExpiryTime) end,
        Indices,
        TopicTokens
    ).

do_store_retained_index(Key, ExpiryTime) ->
    RetainedIndex = #retained_index{
        key = Key,
        expiry_time = ExpiryTime
    },
    mria:dirty_write(?TAB_INDEX, RetainedIndex).

msg_table(SearchTable) ->
    qlc:q([
        Msg
     || #retained_message{
            msg = Msg
        } <- SearchTable
    ]).

search_table(Tokens, Now) ->
    Indices = dirty_indices(read),
    Index = emqx_retainer_index:select_index(Tokens, Indices),
    search_table(Index, Tokens, Now).

search_table(undefined, Tokens, Now) ->
    Ms = make_message_match_spec(Tokens, Now),
    ets:table(?TAB_MESSAGE, [{traverse, {select, Ms}}]);
search_table(Index, Tokens, Now) ->
    Ms = make_index_match_spec(Index, Tokens, Now),
    Topics = [
        emqx_retainer_index:restore_topic(Key)
     || #retained_index{key = Key} <- ets:select(?TAB_INDEX, Ms)
    ],
    RetainedMsgQH = qlc:q([
        ets:lookup(?TAB_MESSAGE, TopicTokens)
     || TopicTokens <- Topics
    ]),
    qlc:q([
        RetainedMsg
     || [
            #retained_message{
                expiry_time = ExpiryTime
            } = RetainedMsg
        ] <- RetainedMsgQH,
        (ExpiryTime == 0) or (ExpiryTime > Now)
    ]).

clear_batch(Indices, QC) ->
    {Result, Rows} = qlc_next_answers(QC, ?CLEAR_BATCH_SIZE),
    lists:foreach(
        fun(RetainedMsg) ->
            delete_message_with_indices(RetainedMsg, Indices)
        end,
        Rows
    ),
    case Result of
        closed -> ok;
        more -> clear_batch(Indices, QC)
    end.

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

qlc_next_answers(QC, N) ->
    case qlc:next_answers(QC, N) of
        NextAnswers when
            is_list(NextAnswers) andalso
                length(NextAnswers) < N
        ->
            qlc:delete_cursor(QC),
            {closed, NextAnswers};
        NextAnswers when is_list(NextAnswers) ->
            {more, NextAnswers};
        {error, Module, Reason} ->
            qlc:delete_cursor(QC),
            error({qlc_error, Module, Reason})
    end.

make_message_match_spec(Tokens, NowMs) ->
    Cond = emqx_retainer_index:condition(Tokens),
    MsHd = #retained_message{topic = Cond, msg = '_', expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}].

make_index_match_spec(Index, Tokens, NowMs) ->
    Cond = emqx_retainer_index:condition(Index, Tokens),
    MsHd = #retained_index{key = Cond, expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}].

is_table_full() ->
    Limit = emqx:get_config([retainer, backend, max_retained_messages]),
    Limit > 0 andalso (table_size() >= Limit).

is_new_topic(Tokens) ->
    case mnesia:dirty_read(?TAB_MESSAGE, Tokens) of
        [_] ->
            false;
        [] ->
            true
    end.

table_size() ->
    mnesia:table_info(?TAB_MESSAGE, size).

config_indices() ->
    lists:sort(emqx_config:get([retainer, backend, index_specs])).

populate_index_meta() ->
    ConfigIndices = config_indices(),
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
            QH = qlc:q([Topic || #retained_message{topic = Topic} <- ets:table(?TAB_MESSAGE)]),
            ok = reindex_batch(qlc:cursor(QH), 0, StatusFun),

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

reindex_batch(QC, Done, StatusFun) ->
    case mria:transaction(?RETAINER_SHARD, fun ?MODULE:do_reindex_batch/2, [QC, Done]) of
        {atomic, {more, NewDone}} ->
            _ = StatusFun(NewDone),
            reindex_batch(QC, NewDone, StatusFun);
        {atomic, {closed, NewDone}} ->
            _ = StatusFun(NewDone),
            ok;
        {aborted, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_reindex_retained_messages",
                reason => Reason
            }),
            {error, Reason}
    end.

do_reindex_batch(QC, Done) ->
    Indices = db_indices(write),
    {Status, Topics} = qlc_next_answers(QC, ?REINDEX_BATCH_SIZE),
    ok = lists:foreach(
        fun(Topic) -> reindex_topic(Indices, Topic) end,
        Topics
    ),
    {Status, Done + length(Topics)}.

wait_dispatch_complete(Timeout) ->
    Nodes = mria_mnesia:running_nodes(),
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
    Nodes = mria_mnesia:running_nodes(),
    case emqx_retainer_proto_v2:active_mnesia_indices(Nodes) of
        {Results, []} ->
            lists:all(
                fun(NodeIndices) -> NodeIndices =:= Indices end,
                Results
            );
        _ ->
            false
    end.
