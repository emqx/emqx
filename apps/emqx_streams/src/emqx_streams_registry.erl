%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_registry).

-moduledoc """
The module contains the registry of Streams.
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
    key_expr :: emqx_variform:compiled() | undefined | '_',
    limits :: emqx_streams_types:limits() | '_',
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
create(#{topic_filter := TopicFilter, is_lastvalue := IsLastValue, limits := Limits} = Stream0) when
    (not IsLastValue) orelse (IsLastValue andalso is_map(map_get(key_expression, Stream0)))
->
    Key = make_key(TopicFilter),
    Id = emqx_guid:gen(),
    maybe
        ok ?= validate_max_stream_count(),
        {atomic, ok} ?=
            mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
                case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
                    [] ->
                        ok = mnesia:write(#?STREAMS_REGISTRY_INDEX_TAB{
                            key = Key,
                            id = Id,
                            is_lastvalue = IsLastValue,
                            key_expr = maps:get(key_expression, Stream0, undefined),
                            limits = Limits
                        }),
                        ok;
                    [_] ->
                        {error, stream_exists}
                end
            end),
        Stream = Stream0#{id => Id},
        %% TODO
        %% Maybe create addtional state in storage
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
-spec match(emqx_types:topic()) -> [emqx_streams_types:stream_handle()].
match(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?STREAMS_REGISTRY_INDEX_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case mnesia:dirty_read(?STREAMS_REGISTRY_INDEX_TAB, Key) of
                [] ->
                    [];
                [#?STREAMS_REGISTRY_INDEX_TAB{} = Rec] ->
                    [record_to_stream_handle(Rec)]
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
            %% TODO
            %% Maybe read addtional state from storage
            {ok, record_to_stream_handle(Rec)}
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
            ok = mria:dirty_delete_object(Rec)
        %% TODO Drop Streams messages and maybe additional state from storage
    end.

-doc """
Delete all Streams. Only for testing/maintenance.
""".
-spec delete_all() -> ok.
delete_all() ->
    _ = mria:clear_table(?STREAMS_REGISTRY_INDEX_TAB),
    %% TODO Drop all Streams messages and maybe additional state from storage
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
            #{id := Id} = StreamHandle = record_to_stream_handle(Rec),
            IsLastvalueOld = emqx_streams_prop:is_lastvalue(StreamHandle),
            IsLastvalueNew = emqx_streams_prop:is_lastvalue(UpdateFields),
            IsLimitedOld = emqx_streams_prop:is_limited(StreamHandle),
            IsLimitedNew = emqx_streams_prop:is_limited(UpdateFields),
            NeedUpdateIndex = need_update_index(StreamHandle, UpdateFields),
            case UpdateFields of
                _ when IsLastvalueOld =/= IsLastvalueNew ->
                    {error, is_lastvalue_not_allowed_to_be_updated};
                _ when (not IsLastvalueNew) andalso (IsLimitedOld =/= IsLimitedNew) ->
                    {error, limit_presence_cannot_be_updated_for_regular_streams};
                _ when NeedUpdateIndex ->
                    case update_index(Key, Id, UpdateFields) of
                        ok ->
                            {ok, maps:merge(StreamHandle, UpdateFields)};
                        %% TODO
                        %% Maybe update addtional state in storage
                        not_found ->
                            not_found
                    end;
                _ ->
                    %% TODO
                    %% Update addtional state in storage
                    {ok, maps:merge(StreamHandle, UpdateFields)}
            end
    end.

-doc """
List all MQs.
""".
-spec list() -> emqx_utils_stream:stream(emqx_stream_types:stream()).
list() ->
    record_stream_to_streams(record_stream()).

-doc """
List at most `Limit` MQs starting from `Cursor` position.
""".
-spec list(cursor(), non_neg_integer()) -> {[emqx_mq_types:mq()], cursor()}.
list(Cursor, Limit) when Limit >= 1 ->
    Streams0 = emqx_utils_stream:consume(
        emqx_utils_stream:limit_length(
            Limit + 1,
            record_stream_to_streams(record_stream(Cursor))
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

need_update_index(
    #{is_lastvalue := true, key_expression := OldKeyExpr} = _StreamHandle,
    #{key_expression := NewKeyExpr} = _UpdateFields
) when NewKeyExpr =/= OldKeyExpr ->
    true;
need_update_index(
    #{limits := OldLimits} = _StreamHandle, #{limits := NewLimits} = _UpdateFields
) when
    OldLimits =/= NewLimits
->
    true;
need_update_index(_, _) ->
    false.

record_stream() ->
    record_stream(undefined).

record_stream(Cursor) ->
    Stream = ets_record_stream(key_from_cursor(Cursor)),
    emqx_utils_stream:chainmap(
        fun(L) -> L end,
        Stream
    ).

key_from_cursor(undefined) ->
    undefined;
key_from_cursor(Cursor) ->
    make_key(Cursor).

ets_record_stream(Key) ->
    fun() ->
        case next_key(Key) of
            '$end_of_table' ->
                [];
            NextKey ->
                [ets:lookup(?STREAMS_REGISTRY_INDEX_TAB, NextKey) | ets_record_stream(NextKey)]
        end
    end.

next_key(undefined) ->
    ets:first(?STREAMS_REGISTRY_INDEX_TAB);
next_key(Key) ->
    ets:next(?STREAMS_REGISTRY_INDEX_TAB, Key).

record_stream_to_streams(Stream) ->
    emqx_utils_stream:chainmap(
        fun(#?STREAMS_REGISTRY_INDEX_TAB{} = Rec) ->
            %% TODO
            %% Maybe read addtional state from storage
            [record_to_stream_handle(Rec)]
        end,
        Stream
    ).

make_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).

update_index(Key, Id, UpdateFields) ->
    {atomic, Result} = mria:transaction(?STREAMS_REGISTRY_SHARD, fun() ->
        case mnesia:read(?STREAMS_REGISTRY_INDEX_TAB, Key, write) of
            [] ->
                not_found;
            [#?STREAMS_REGISTRY_INDEX_TAB{id = Id} = Rec] ->
                NewKeyExpr = maps:get(key_expression, UpdateFields, undefined),
                NewLimits = maps:get(limits, UpdateFields, ?DEFAULT_STREAM_LIMITS),
                mnesia:write(Rec#?STREAMS_REGISTRY_INDEX_TAB{
                    key_expr = NewKeyExpr, limits = NewLimits
                }),
                ok
        end
    end),
    Result.

record_to_stream_handle(#?STREAMS_REGISTRY_INDEX_TAB{
    key = Key, id = Id, key_expr = KeyExpr, is_lastvalue = IsLastValue, limits = Limits
}) ->
    #{
        id => Id,
        topic_filter => emqx_topic_index:get_topic(Key),
        is_lastvalue => IsLastValue,
        key_expression => KeyExpr,
        limits => Limits
    }.

stream_count() ->
    mnesia:table_info(?STREAMS_REGISTRY_INDEX_TAB, size).

validate_max_stream_count() ->
    case stream_count() >= emqx_streams_config:max_stream_count() of
        true ->
            {error, max_queue_count_reached};
        false ->
            ok
    end.
