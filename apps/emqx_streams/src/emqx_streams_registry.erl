%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_registry).

-moduledoc """
The module contains the registry of Streams and provides API
to create, update, delete, and look up streams.

NOTE: in this module, we call `emqx_utils_stream` objects "iterators" to avoid confusion.
""".

-include("emqx_streams_internal.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_tables/0,
    create/1,
    find/1,
    is_present/2,
    match/1,
    delete/1,
    delete/2,
    mark_deleting/1,
    is_deleting/1,
    update/2,
    list/0,
    list/2,
    list_all/2,
    stream_count/0
]).

-dialyzer(no_improper_lists).

%% Only for testing/debugging/maintenance.
-export([
    delete_all/0,
    delete_legacy/0,
    create_pre_611_stream/1,
    names/0
]).

-define(STREAMS_REGISTRY_INDEX_TAB, emqx_streams_registry_index).
-define(STREAMS_REGISTRY_SHARD, emqx_streams_registry_shard).

-record(?STREAMS_REGISTRY_INDEX_TAB, {
    key :: emqx_topic_index:key(nil() | emqx_streams_types:stream_id()) | '_',
    id :: emqx_streams_types:stream_id() | '_',
    is_lastvalue :: boolean() | '_',
    key_expression :: emqx_variform:compiled() | '_',
    data_retention_period :: emqx_streams_types:interval_ms() | '_',
    read_max_unacked :: non_neg_integer() | '_',
    %% Stream limits
    max_shard_message_count :: infinity | pos_integer() | '_',
    max_shard_message_bytes :: infinity | pos_integer() | '_',
    extra = #{} :: map() | '_'
}).

-define(STREAMS_REGISTRY_NAME_INDEX_TAB, emqx_streams_registry_name_index).

-record(?STREAMS_REGISTRY_NAME_INDEX_TAB, {
    name :: binary() | '_',
    topic_filter :: emqx_streams_types:stream_topic() | '_',
    id :: emqx_streams_types:stream_id() | '_',
    extra = #{} :: map() | '_'
}).

-define(name_key, 1).

-type cursor() :: binary() | undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?STREAMS_REGISTRY_INDEX_TAB, [
        {type, ordered_set},
        {rlog_shard, ?STREAMS_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?STREAMS_REGISTRY_INDEX_TAB},
        {attributes, record_info(fields, ?STREAMS_REGISTRY_INDEX_TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria:create_table(?STREAMS_REGISTRY_NAME_INDEX_TAB, [
        {type, ordered_set},
        {rlog_shard, ?STREAMS_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?STREAMS_REGISTRY_NAME_INDEX_TAB},
        {attributes, record_info(fields, ?STREAMS_REGISTRY_NAME_INDEX_TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?STREAMS_REGISTRY_INDEX_TAB, ?STREAMS_REGISTRY_NAME_INDEX_TAB].

-doc """
Create a new Stream.
""".
-spec create(emqx_streams_types:stream()) ->
    {ok, emqx_streams_types:stream()}
    | {error, stream_exists}
    | {error, invalid_name}
    | {error, max_stream_count_reached}
    | {error, term()}.
create(
    #{
        name := Name,
        topic_filter := TopicFilter,
        key_expression := _KeyExpression,
        is_lastvalue := _IsLastValue,
        limits := _Limits,
        data_retention_period := _DataRetentionPeriod,
        read_max_unacked := _ReadMaxUnacked
    } = Stream0
) ->
    Id = emqx_guid:gen(),
    Stream = Stream0#{id => Id},
    Rec = stream_to_record(Stream),
    maybe
        ok ?= validate_name(Name),
        ok ?= validate_max_stream_count(),
        {atomic, ok} ?=
            mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
                case mnesia:read(?STREAMS_REGISTRY_NAME_INDEX_TAB, Name) of
                    [] ->
                        ok = mnesia:write(#?STREAMS_REGISTRY_NAME_INDEX_TAB{
                            name = Name, topic_filter = TopicFilter, id = Id
                        }),
                        ok = mnesia:write(Rec);
                    [_] ->
                        {error, stream_exists}
                end
            end),
        {ok, Stream}
    else
        {atomic, {error, _} = Error} ->
            Error;
        {aborted, ErrReason} ->
            {error, ErrReason};
        {error, _} = Error ->
            Error
    end.

-doc """
Find all Streams matching the given concrete topic.
""".
-spec match(emqx_types:topic()) -> [emqx_streams_types:stream()].
match(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?STREAMS_REGISTRY_INDEX_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
                [] ->
                    [];
                [#?STREAMS_REGISTRY_INDEX_TAB{extra = #{deleted_at := _}}] ->
                    [];
                [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
                    [record_to_stream(Rec)]
            end
        end,
        Keys
    ).

-doc """
Find the Stream by its name
""".
-spec find(emqx_streams_types:stream_topic()) -> {ok, emqx_streams_types:stream()} | not_found.
find(?LEGACY_STREAM_NAME(TopicFilter)) ->
    ?tp_debug(streams_registry_find_legacy, #{topic_filter => TopicFilter}),
    Key = make_legacy_key(TopicFilter),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
            {ok, record_to_stream(Rec)}
    end;
find(Name) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_NAME_INDEX_TAB, Name) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_NAME_INDEX_TAB{id = Id, topic_filter = TopicFilter, extra = _Extra}] ->
            case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, make_key(TopicFilter, Id)) of
                [] ->
                    not_found;
                [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
                    {ok, record_to_stream(Rec)}
            end
    end.

-doc """
Check if the Stream exists by its topic filter.
""".
-spec is_present(emqx_streams_types:stream_name(), emqx_streams_types:stream_topic()) -> boolean().
is_present(?LEGACY_STREAM_NAME(TopicFilter), TopicFilter) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, make_legacy_key(TopicFilter)) of
        [] ->
            false;
        [#?STREAMS_REGISTRY_INDEX_TAB{}] ->
            true
    end;
is_present(Name, TopicFilter) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_NAME_INDEX_TAB, Name) of
        [#?STREAMS_REGISTRY_NAME_INDEX_TAB{topic_filter = TopicFilter}] ->
            true;
        _ ->
            false
    end.

-doc """
Delete the Stream by its name.
""".
-spec delete(emqx_streams_types:stream_name()) -> ok | not_found | {error, term()}.
delete(Name) ->
    case find(Name) of
        {ok, #{id := Id}} -> delete(Name, Id);
        not_found -> not_found
    end.

-doc "Delete a stream only if its name still identifies the requested stream".
-spec delete(emqx_streams_types:stream_name(), emqx_streams_types:stream_id()) ->
    ok | not_found | {error, term()}.
delete(?LEGACY_STREAM_NAME(TopicFilter), Id) ->
    Key = make_legacy_key(TopicFilter),
    do_delete(Key, Id);
delete(Name, Id) ->
    ?tp_debug(mq_registry_delete, #{name => Name}),
    case mnesia:dirty_read(?STREAMS_REGISTRY_NAME_INDEX_TAB, Name) of
        [#?STREAMS_REGISTRY_NAME_INDEX_TAB{topic_filter = TopicFilter, id = Id} = IndexRec] ->
            Key = make_key(TopicFilter, Id),
            case do_delete(Key, Id) of
                {error, _} = Error ->
                    Error;
                %% `ok` or `not_found`
                Res ->
                    ok = mria:dirty_delete_object(IndexRec),
                    Res
            end;
        _ ->
            not_found
    end.

do_delete(Key, Id) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [#?STREAMS_REGISTRY_INDEX_TAB{id = Id} = Rec] ->
            Stream = record_to_stream(Rec),
            %% TODO Drop consumer groups when they appear
            case emqx_streams_message_db:drop(Stream) of
                ok ->
                    ok = mria:dirty_delete_object(Rec);
                {error, _} = Error ->
                    Error
            end;
        _ ->
            not_found
    end.

-doc """
Mark the Stream as being deleted. Publishing and listing ignore a marked Stream.
A repeated mark keeps the first `deleted_at`.
""".
-spec mark_deleting(emqx_streams_types:stream()) -> ok | not_found.
mark_deleting(#{id := Id} = Stream) ->
    Key = stream_key(Stream),
    {atomic, Result} = mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
        case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
            [#?STREAMS_REGISTRY_INDEX_TAB{id = Id, extra = #{deleted_at := _}}] ->
                ok;
            [#?STREAMS_REGISTRY_INDEX_TAB{id = Id, extra = Extra} = Rec] ->
                DeletedAt = erlang:system_time(millisecond),
                mnesia:write(Rec#?STREAMS_REGISTRY_INDEX_TAB{
                    extra = Extra#{deleted_at => DeletedAt}
                });
            _ ->
                not_found
        end
    end),
    Result.

-doc "Check whether the Stream is marked as being deleted".
-spec is_deleting(emqx_streams_types:stream()) -> boolean().
is_deleting(#{id := Id} = Stream) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, stream_key(Stream)) of
        [#?STREAMS_REGISTRY_INDEX_TAB{id = Id, extra = #{deleted_at := _}}] -> true;
        _ -> false
    end.

-doc """
Delete all Streams. Only for testing/maintenance.
""".
-spec delete_all() -> ok.
delete_all() ->
    _ = mria:clear_table(?STREAMS_REGISTRY_NAME_INDEX_TAB),
    _ = mria:clear_table(?STREAMS_REGISTRY_INDEX_TAB),
    ok = emqx_streams_message_db:delete_all(),
    %% TODO Drop all consumer groups when they appear
    ok.

-doc """
Delete every legacy stream, that is, every stream created before 6.1.1 without
a name of its own. Such a stream is listed with the name `/<topic-filter>`.
Return the number of deleted streams. A stream that fails to delete is logged and
skipped. Only for maintenance.
""".
-spec delete_legacy() -> non_neg_integer().
delete_legacy() ->
    Names = emqx_utils_stream:fold(
        fun
            (#{name := ?LEGACY_STREAM_NAME(_) = Name}, Acc) -> [Name | Acc];
            (_, Acc) -> Acc
        end,
        [],
        list()
    ),
    lists:foldl(fun delete_legacy/2, 0, Names).

delete_legacy(Name, Count) ->
    try delete(Name) of
        ok ->
            Count + 1;
        not_found ->
            Count;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_delete_legacy_stream", name => Name, reason => Reason
            }),
            Count
    catch
        Class:Reason:StackTrace ->
            ?SLOG(warning, #{
                msg => "failed_to_delete_legacy_stream",
                name => Name,
                exception => Class,
                reason => Reason,
                stacktrace => StackTrace
            }),
            Count
    end.

-doc """
Update the Stream by its name.
* `is_lastvalue` flag and `topic_filter` cannot be updated.
* limited streams cannot be updated to unlimited streams and vice versa.
""".
-spec update(emqx_streams_types:stream_topic(), map()) ->
    {ok, emqx_streams_types:stream()}
    | not_found
    | {error, is_lastvalue_not_allowed_to_be_updated}
    | {error, limit_presence_cannot_be_updated_for_regular_streams}
    | {error, being_deleted}
    | {error, term()}.
update(?LEGACY_STREAM_NAME(TopicFilter), UpdateFields) ->
    Key = make_legacy_key(TopicFilter),
    do_update(Key, UpdateFields);
update(Name, UpdateFields) ->
    case mnesia:dirty_read(?STREAMS_REGISTRY_NAME_INDEX_TAB, Name) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_NAME_INDEX_TAB{id = Id, topic_filter = TopicFilter}] ->
            Key = make_key(TopicFilter, Id),
            do_update(Key, UpdateFields)
    end.

do_update(Key, UpdateFields0) ->
    UpdateFields = maps:without([name, topic_filter, id], UpdateFields0),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_INDEX_TAB{extra = #{deleted_at := _}}] ->
            {error, being_deleted};
        [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
            #{id := Id} = Stream = record_to_stream(Rec),
            IsLastValueOld = emqx_streams_prop:is_lastvalue(Stream),
            IsLastValueNew = emqx_streams_prop:is_lastvalue(UpdateFields),
            IsLimitedOld = emqx_streams_prop:is_limited(Stream),
            IsLimitedNew = emqx_streams_prop:is_limited(UpdateFields),
            case UpdateFields of
                _ when IsLastValueOld =/= IsLastValueNew ->
                    {error, is_lastvalue_not_allowed_to_be_updated};
                _ when (not IsLastValueNew) andalso (IsLimitedOld =/= IsLimitedNew) ->
                    {error, limit_presence_cannot_be_updated_for_regular_streams};
                _ ->
                    update_index(Key, Id, UpdateFields)
            end
    end.

-doc """
List all Streams, except the Streams marked as being deleted.
""".
-spec list() -> emqx_utils_stream:stream(emqx_streams_types:stream()).
list() ->
    emqx_utils_stream:map(
        fun({_Key, Stream}) ->
            Stream
        end,
        record_iterator_to_streams(skip_deleting(record_iterator()))
    ).

-doc """
List at most `Limit` Streams starting from `Cursor` position, except the Streams marked as
being deleted.
""".
-spec list(cursor(), non_neg_integer()) ->
    {ok, [emqx_streams_types:stream()], cursor()} | {error, bad_cursor}.
list(Cursor, Limit) when Limit >= 1 ->
    case key_from_cursor(Cursor) of
        {ok, Key} ->
            {Streams, CursorNext} = do_list(skip_deleting(record_iterator(Key)), Limit),
            {ok, Streams, CursorNext};
        {error, bad_cursor} ->
            {error, bad_cursor}
    end.

-doc """
List at most `Limit` Streams starting from `Cursor` position, including the Streams marked as
being deleted.
""".
-spec list_all(cursor(), non_neg_integer()) ->
    {ok, [emqx_streams_types:stream()], cursor()} | {error, bad_cursor}.
list_all(Cursor, Limit) when Limit >= 1 ->
    case key_from_cursor(Cursor) of
        {ok, Key} ->
            {Streams, CursorNext} = do_list(record_iterator(Key), Limit),
            {ok, Streams, CursorNext};
        {error, bad_cursor} ->
            {error, bad_cursor}
    end.

do_list(RecordIterator, Limit) ->
    KeyStreams0 = emqx_utils_stream:consume(
        emqx_utils_stream:limit_length(
            Limit + 1,
            record_iterator_to_streams(RecordIterator)
        )
    ),
    case length(KeyStreams0) < Limit + 1 of
        true ->
            {_Keys, Streams} = lists:unzip(KeyStreams0),
            {Streams, undefined};
        false ->
            KeyStreams = lists:sublist(KeyStreams0, Limit),
            {Keys, Streams} = lists:unzip(KeyStreams),
            Key = lists:last(Keys),
            NewCursor = cursor_from_key(Key),
            {Streams, NewCursor}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

record_iterator() ->
    record_iterator(undefined).

record_iterator(Key) ->
    Stream = ets_record_iterator(Key),
    emqx_utils_stream:chainmap(
        fun(L) -> L end,
        Stream
    ).

key_from_cursor(undefined) ->
    {ok, undefined};
key_from_cursor(Cursor) ->
    try emqx_utils_json:decode(emqx_base62:decode(Cursor)) of
        #{<<"tf">> := TopicFilter, <<"id">> := EncodedId} ->
            Id = emqx_base62:decode(EncodedId),
            {ok, emqx_topic_index:make_key(TopicFilter, Id)};
        _ ->
            {error, bad_cursor}
    catch
        Class:Reason:StackTrace ->
            ?tp(warning, streams_registry_key_from_cursor_error, #{
                cursor => Cursor,
                class => Class,
                reason => Reason,
                stack_trace => StackTrace
            }),
            {error, bad_cursor}
    end.

cursor_from_key(Key) ->
    TopicFilter = emqx_topic_index:get_topic(Key),
    Id = emqx_topic_index:get_id(Key),
    emqx_base62:encode(
        emqx_utils_json:encode(#{<<"tf">> => TopicFilter, <<"id">> => emqx_base62:encode(Id)})
    ).

ets_record_iterator(Key) ->
    fun() ->
        case next_key(Key) of
            '$end_of_table' ->
                [];
            NextKey ->
                [ets:lookup(?STREAMS_REGISTRY_INDEX_TAB, NextKey) | ets_record_iterator(NextKey)]
        end
    end.

next_key(undefined) ->
    ets:first(?STREAMS_REGISTRY_INDEX_TAB);
next_key(Key) ->
    ets:next(?STREAMS_REGISTRY_INDEX_TAB, Key).

record_iterator_to_streams(Iterator) ->
    emqx_utils_stream:chainmap(
        fun(#?STREAMS_REGISTRY_INDEX_TAB{key = Key, extra = Extra} = Rec) ->
            [{Key, maps:merge(record_to_stream(Rec), maps:with([deleted_at], Extra))}]
        end,
        Iterator
    ).

skip_deleting(Iterator) ->
    emqx_utils_stream:filter(
        fun(#?STREAMS_REGISTRY_INDEX_TAB{extra = Extra}) -> not is_map_key(deleted_at, Extra) end,
        Iterator
    ).

make_key(TopicFilter, Id) ->
    emqx_topic_index:make_key(TopicFilter, Id).

make_legacy_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).

stream_key(#{name := ?LEGACY_STREAM_NAME(TopicFilter)}) ->
    make_legacy_key(TopicFilter);
stream_key(#{topic_filter := TopicFilter, id := Id}) ->
    make_key(TopicFilter, Id).

update_index(Key, Id, UpdateFields) ->
    {atomic, Result} = mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
        case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
            [#?STREAMS_REGISTRY_INDEX_TAB{id = Id, extra = Extra0} = Rec] ->
                Stream0 = record_to_stream(Rec),
                Stream = emqx_utils_maps:deep_merge(Stream0, UpdateFields),
                #?STREAMS_REGISTRY_INDEX_TAB{extra = Extra1} = Record = stream_to_record(Stream),
                %% Important: preserve the key and the deletion mark
                Extra = maps:merge(Extra1, maps:with([deleted_at], Extra0)),
                mnesia:write(Record#?STREAMS_REGISTRY_INDEX_TAB{key = Key, extra = Extra}),
                {ok, Stream#{topic_filter => emqx_topic_index:get_topic(Key)}};
            _ ->
                not_found
        end
    end),
    Result.

record_to_stream(
    #?STREAMS_REGISTRY_INDEX_TAB{
        key = Key,
        id = Id,
        is_lastvalue = IsLastValue,
        key_expression = KeyExpression,
        data_retention_period = DataRetentionPeriod,
        read_max_unacked = ReadMaxUnacked,
        max_shard_message_count = MaxShardMessageCount,
        max_shard_message_bytes = MaxShardMessageBytes,
        extra = Extra
    } = _Rec
) ->
    Name =
        case Extra of
            #{?name_key := N} ->
                N;
            _ ->
                TopicFilter = emqx_topic_index:get_topic(Key),
                ?LEGACY_STREAM_NAME(TopicFilter)
        end,
    #{
        id => Id,
        name => Name,
        topic_filter => emqx_topic_index:get_topic(Key),
        is_lastvalue => IsLastValue,
        data_retention_period => DataRetentionPeriod,
        read_max_unacked => ReadMaxUnacked,
        key_expression => KeyExpression,
        limits => #{
            max_shard_message_count => MaxShardMessageCount,
            max_shard_message_bytes => MaxShardMessageBytes
        }
    }.

stream_to_record(
    #{
        topic_filter := TopicFilter,
        name := Name,
        id := Id,
        is_lastvalue := IsLastValue,
        key_expression := KeyExpression,
        data_retention_period := DataRetentionPeriod,
        read_max_unacked := ReadMaxUnacked,
        limits := #{
            max_shard_message_count := MaxShardMessageCount,
            max_shard_message_bytes := MaxShardMessageBytes
        }
    } = _Stream
) ->
    #?STREAMS_REGISTRY_INDEX_TAB{
        key = make_key(TopicFilter, Id),
        id = Id,
        is_lastvalue = IsLastValue,
        key_expression = KeyExpression,
        data_retention_period = DataRetentionPeriod,
        read_max_unacked = ReadMaxUnacked,
        max_shard_message_count = MaxShardMessageCount,
        max_shard_message_bytes = MaxShardMessageBytes,
        extra = #{?name_key => Name}
    }.

stream_count() ->
    mnesia:table_info(?STREAMS_REGISTRY_INDEX_TAB, size).

validate_max_stream_count() ->
    case stream_count() >= emqx_streams_config:max_stream_count() of
        true ->
            {error, max_stream_count_reached};
        false ->
            ok
    end.

validate_name(Name) ->
    emqx_streams_schema:validate_name(Name).

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

names() ->
    lists:map(
        fun(#?STREAMS_REGISTRY_NAME_INDEX_TAB{name = Name, topic_filter = TopicFilter, id = Id}) ->
            #{name => Name, topic_filter => TopicFilter, id => Id}
        end,
        ets:tab2list(?STREAMS_REGISTRY_NAME_INDEX_TAB)
    ).

create_pre_611_stream(Stream0) ->
    TopicFilter = emqx_streams_prop:topic_filter(Stream0),
    Name = ?LEGACY_STREAM_NAME(TopicFilter),
    Rec0 = stream_to_record(Stream0#{id => emqx_guid:gen(), name => Name}),
    Rec = Rec0#?STREAMS_REGISTRY_INDEX_TAB{key = make_legacy_key(TopicFilter), extra = #{}},
    {atomic, ok} =
        mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
            mnesia:write(Rec)
        end),
    {ok, _} = find(Name).
