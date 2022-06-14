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

store_retained(_, #message{topic = Topic} = Msg) ->
    ExpiryTime = emqx_retainer:get_expiry_time(Msg),
    Tokens = topic_to_tokens(Topic),
    Fun =
        case is_table_full() of
            false ->
                fun() ->
                    store_retained(db_indices(write), Msg, Tokens, ExpiryTime)
                end;
            _ ->
                fun() ->
                    case mnesia:read(?TAB_MESSAGE, Tokens, write) of
                        [_] ->
                            store_retained(db_indices(write), Msg, Tokens, ExpiryTime);
                        [] ->
                            mnesia:abort(table_is_full)
                    end
                end
        end,
    case mria:transaction(?RETAINER_SHARD, Fun) of
        {atomic, ok} ->
            ?tp(debug, message_retained, #{topic => Topic}),
            ok;
        {aborted, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_retain_message",
                topic => Topic,
                reason => Reason
            })
    end.

clear_expired(_) ->
    NowMs = erlang:system_time(millisecond),
    QH = qlc:q([
        TopicTokens
     || #retained_message{
            topic = TopicTokens,
            expiry_time = ExpiryTime
        } <- mnesia:table(?TAB_MESSAGE, [{lock, write}]),
        (ExpiryTime =/= 0) and (ExpiryTime < NowMs)
    ]),
    Fun = fun() ->
        QC = qlc:cursor(QH),
        clear_batch(db_indices(write), QC)
    end,
    {atomic, _} = mria:transaction(?RETAINER_SHARD, Fun),
    ok.

delete_message(_, Topic) ->
    Tokens = topic_to_tokens(Topic),
    DeleteFun =
        case emqx_topic:wildcard(Topic) of
            false ->
                fun() ->
                    ok = delete_message_by_topic(Tokens, db_indices(write))
                end;
            true ->
                fun() ->
                    QH = topic_search_table(Tokens),
                    qlc:fold(
                        fun(TopicTokens, _) ->
                            ok = delete_message_by_topic(TopicTokens, db_indices(write))
                        end,
                        undefined,
                        QH
                    )
                end
        end,
    {atomic, _} = mria:transaction(?RETAINER_SHARD, DeleteFun),
    ok.

read_message(_, Topic) ->
    {ok, read_messages(Topic)}.

match_messages(_, Topic, undefined) ->
    Tokens = topic_to_tokens(Topic),
    Now = erlang:system_time(millisecond),
    QH = search_table(Tokens, Now),
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
                search_table(undefined, ['#'], Now);
            _ ->
                Tokens = topic_to_tokens(Topic),
                search_table(Tokens, Now)
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
    Fun = fun() ->
        mnesia:read(?TAB_INDEX_META, ?META_KEY)
    end,
    case mria:transaction(?RETAINER_SHARD, Fun) of
        {atomic, [#retained_index_meta{reindexing = true}]} ->
            true;
        {atomic, _} ->
            false;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

store_retained(Indices, Msg, Tokens, ExpiryTime) ->
    ok = store_retained_message(Msg, Tokens, ExpiryTime),
    ok = emqx_retainer_index:foreach_index_key(
        fun(Key) -> store_retained_index(Key, ExpiryTime) end,
        Indices,
        Tokens
    ).

store_retained_message(Msg, Tokens, ExpiryTime) ->
    RetainedMessage = #retained_message{
        topic = Tokens,
        msg = Msg,
        expiry_time = ExpiryTime
    },
    mnesia:write(?TAB_MESSAGE, RetainedMessage, write).

store_retained_index(Key, ExpiryTime) ->
    RetainedIndex = #retained_index{
        key = Key,
        expiry_time = ExpiryTime
    },
    mnesia:write(?TAB_INDEX, RetainedIndex, write).

topic_search_table(Tokens) ->
    Index = emqx_retainer_index:select_index(Tokens, db_indices(read)),
    topic_search_table(Index, Tokens).

topic_search_table(undefined, Tokens) ->
    Cond = emqx_retainer_index:condition(Tokens),
    Ms = [{#retained_message{topic = Cond, msg = '_', expiry_time = '_'}, [], ['$_']}],
    MsgQH = mnesia:table(?TAB_MESSAGE, [{traverse, {select, Ms}}]),
    qlc:q([Topic || #retained_message{topic = Topic} <- MsgQH]);
topic_search_table(Index, Tokens) ->
    Cond = emqx_retainer_index:condition(Index, Tokens),
    Ms = [{#retained_index{key = Cond, expiry_time = '_'}, [], ['$_']}],
    IndexQH = mnesia:table(?TAB_INDEX, [{traverse, {select, Ms}}]),
    qlc:q([
        emqx_retainer_index:restore_topic(Key)
     || #retained_index{key = Key} <- IndexQH
    ]).

search_table(Tokens, Now) ->
    Indices = dirty_read_indices(),
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
        Msg
     || [
            #retained_message{
                msg = Msg,
                expiry_time = ExpiryTime
            }
        ] <- RetainedMsgQH,
        (ExpiryTime == 0) or (ExpiryTime > Now)
    ]).

dirty_read_indices() ->
    case ets:lookup(?TAB_INDEX_META, ?META_KEY) of
        [#retained_index_meta{read_indices = ReadIndices}] -> ReadIndices;
        [] -> []
    end.

clear_batch(Indices, QC) ->
    {Result, Rows} = qlc_next_answers(QC, ?CLEAR_BATCH_SIZE),
    lists:foreach(
        fun(TopicTokens) -> delete_message_by_topic(TopicTokens, Indices) end,
        Rows
    ),
    case Result of
        closed -> ok;
        more -> clear_batch(Indices, QC)
    end.

delete_message_by_topic(TopicTokens, Indices) ->
    ok = emqx_retainer_index:foreach_index_key(
        fun(Key) ->
            mnesia:delete({?TAB_INDEX, Key})
        end,
        Indices,
        TopicTokens
    ),
    ok = mnesia:delete({?TAB_MESSAGE, TopicTokens}).

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
    MsHd = #retained_message{topic = Cond, msg = '$2', expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$2']}].

make_index_match_spec(Index, Tokens, NowMs) ->
    Cond = emqx_retainer_index:condition(Index, Tokens),
    MsHd = #retained_index{key = Cond, expiry_time = '$3'},
    [{MsHd, [{'orelse', {'=:=', '$3', 0}, {'>', '$3', NowMs}}], ['$_']}].

-spec is_table_full() -> boolean().
is_table_full() ->
    Limit = emqx:get_config([retainer, backend, max_retained_messages]),
    Limit > 0 andalso (table_size() >= Limit).

-spec table_size() -> non_neg_integer().
table_size() ->
    mnesia:table_info(?TAB_MESSAGE, size).

config_indices() ->
    lists:sort(emqx_config:get([retainer, backend, index_specs])).

populate_index_meta() ->
    ConfigIndices = config_indices(),
    Fun = fun() ->
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
        end
    end,
    case mria:transaction(?RETAINER_SHARD, Fun) of
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

db_indices(Type) ->
    case mnesia:read(?TAB_INDEX_META, ?META_KEY) of
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
    %% Disable read indices and update write indices so that new records are written
    %% with correct indices. Also block parallel reindexing.
    case try_start_reindex(NewIndices, Force) of
        {atomic, ok} ->
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
                fun(Key) -> store_retained_index(Key, ExpiryTime) end,
                Indices,
                Topic
            );
        [] ->
            ok
    end.

reindex_batch(QC, Done, StatusFun) ->
    Fun = fun() ->
        Indices = db_indices(write),
        {Status, Topics} = qlc_next_answers(QC, ?REINDEX_BATCH_SIZE),
        ok = lists:foreach(
            fun(Topic) -> reindex_topic(Indices, Topic) end,
            Topics
        ),
        {Status, Done + length(Topics)}
    end,
    case mria:transaction(?RETAINER_SHARD, Fun) of
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

wait_dispatch_complete(Timeout) ->
    Nodes = mria_mnesia:running_nodes(),
    {Results, []} = emqx_retainer_proto_v1:wait_dispatch_complete(Nodes, Timeout),
    lists:all(
        fun(Result) -> Result =:= ok end,
        Results
    ).
