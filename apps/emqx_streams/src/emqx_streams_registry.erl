%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_registry).

-moduledoc """
The module contains the registry of Streams and provides API
to create, update, delete, and look up streams.

NOTE: in this module, we call `emqx_utils_stream` objects "iterators" to avoid confusion.
""".

-include("emqx_streams_internal.hrl").

-export([
    create_tables/0,
    create/1,
    find/1,
    is_present/1,
    match/1,
    delete/1,
    update/2,
    list/0,
    list/2
]).

-dialyzer(no_improper_lists).

%% Only for testing/debugging.
-export([
    delete_all/0
]).

-define(STREAMS_REGISTRY_INDEX_TAB, emqx_streams_registry_index).
-define(STREAMS_REGISTRY_SHARD, emqx_streams_registry_shard).

-record(?STREAMS_REGISTRY_INDEX_TAB, {
    key :: emqx_topic_index:key(nil()) | '_',
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
    [?STREAMS_REGISTRY_INDEX_TAB].

-doc """
Create a new Stream.
""".
-spec create(emqx_streams_types:stream()) ->
    {ok, emqx_streams_types:stream()}
    | {error, stream_exists}
    | {error, max_stream_count_reached}
    | {error, term()}.
create(
    #{
        topic_filter := TopicFilter,
        key_expression := _KeyExpression,
        is_lastvalue := _IsLastValue,
        limits := _Limits,
        data_retention_period := _DataRetentionPeriod,
        read_max_unacked := _ReadMaxUnacked
    } = Stream0
) ->
    Key = make_key(TopicFilter),
    Id = emqx_guid:gen(),
    Stream = Stream0#{id => Id},
    Rec = stream_to_record(Stream),
    maybe
        ok ?= validate_max_stream_count(),
        {atomic, ok} ?=
            mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
                case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
                    [] ->
                        ok = mnesia:write(Rec),
                        ok;
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
                [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
                    [record_to_stream(Rec)]
            end
        end,
        Keys
    ).

-doc """
Find the Stream by its topic filter.
""".
-spec find(emqx_streams_types:stream_topic()) -> {ok, emqx_streams_types:stream()} | not_found.
find(TopicFilter) ->
    ?tp_debug(mq_registry_find, #{topic_filter => TopicFilter}),
    Key = make_key(TopicFilter),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
            {ok, record_to_stream(Rec)}
    end.

-doc """
Check if the Stream exists by its topic filter.
""".
-spec is_present(emqx_streams_types:stream_topic()) -> boolean().
is_present(TopicFilter) ->
    Key = make_key(TopicFilter),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            false;
        [#?STREAMS_REGISTRY_INDEX_TAB{}] ->
            true
    end.

-doc """
Delete the Stream by its topic filter.
""".
-spec delete(emqx_streams_types:stream_topic()) -> ok | not_found | {error, term()}.
delete(TopicFilter) ->
    ?tp_debug(mq_registry_delete, #{topic_filter => TopicFilter}),
    Key = make_key(TopicFilter),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
            Stream = record_to_stream(Rec),
            %% TODO Drop consumer groups when they appear
            case emqx_streams_message_db:drop(Stream) of
                ok ->
                    ok = mria:dirty_delete_object(Rec);
                {error, _} = Error ->
                    Error
            end
    end.

-doc """
Delete all Streams. Only for testing/maintenance.
""".
-spec delete_all() -> ok.
delete_all() ->
    _ = mria:clear_table(?STREAMS_REGISTRY_INDEX_TAB),
    ok = emqx_streams_message_db:delete_all(),
    %% TODO Drop all consumer groups when they appear
    ok.

-doc """
Update the Stream by its topic filter.
* `is_lastvalue` flag cannot be updated.
* limited streams cannot be updated to unlimited streams and vice versa.
""".
-spec update(emqx_streams_types:stream_topic(), map()) ->
    {ok, emqx_streams_types:stream()}
    | not_found
    | {error, is_lastvalue_not_allowed_to_be_updated}
    | {error, limit_presence_cannot_be_updated_for_regular_streams}
    | {error, term()}.
update(TopicFilter, #{is_lastvalue := _IsLastValue} = UpdateFields0) ->
    Key = make_key(TopicFilter),
    UpdateFields = maps:without([topic_filter, id], UpdateFields0),
    case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
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
List all MQs.
""".
-spec list() -> emqx_utils_stream:stream(emqx_streams_types:stream()).
list() ->
    record_iterator_to_streams(record_iterator()).

-doc """
List at most `Limit` MQs starting from `Cursor` position.
""".
-spec list(cursor(), non_neg_integer()) -> {[emqx_streams_types:stream()], cursor()}.
list(Cursor, Limit) when Limit >= 1 ->
    Streams0 = emqx_utils_stream:consume(
        emqx_utils_stream:limit_length(
            Limit + 1,
            record_iterator_to_streams(record_iterator(Cursor))
        )
    ),
    case length(Streams0) < Limit + 1 of
        true ->
            {Streams0, undefined};
        false ->
            Streams = lists:sublist(Streams0, Limit),
            #{topic_filter := TopicFilter} = lists:last(Streams),
            NewCursor = TopicFilter,
            {Streams, NewCursor}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

record_iterator() ->
    record_iterator(undefined).

record_iterator(Cursor) ->
    Stream = ets_record_iterator(key_from_cursor(Cursor)),
    emqx_utils_stream:chainmap(
        fun(L) -> L end,
        Stream
    ).

key_from_cursor(undefined) ->
    undefined;
key_from_cursor(Cursor) ->
    make_key(Cursor).

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
        fun(#?STREAMS_REGISTRY_INDEX_TAB{} = Rec) ->
            [record_to_stream(Rec)]
        end,
        Iterator
    ).

make_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).

update_index(Key, Id, UpdateFields) ->
    {atomic, Result} = mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
        case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
            [#?STREAMS_REGISTRY_INDEX_TAB{id = Id} = Rec] ->
                Stream0 = record_to_stream(Rec),
                Stream = emqx_utils_maps:deep_merge(Stream0, UpdateFields),
                mnesia:write(stream_to_record(Stream)),
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
        extra = _Extra
    } = _Rec
) ->
    #{
        id => Id,
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
        key = make_key(TopicFilter),
        id = Id,
        is_lastvalue = IsLastValue,
        key_expression = KeyExpression,
        data_retention_period = DataRetentionPeriod,
        read_max_unacked = ReadMaxUnacked,
        max_shard_message_count = MaxShardMessageCount,
        max_shard_message_bytes = MaxShardMessageBytes
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
