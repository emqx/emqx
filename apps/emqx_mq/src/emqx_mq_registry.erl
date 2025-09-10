%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry).

-moduledoc """
The module contains the registry of Message Queues.
""".

-include("emqx_mq_internal.hrl").

-export([
    create_tables/0,
    create/1,
    find/1,
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

-define(MQ_REGISTRY_INDEX_TAB, emqx_mq_registry_index).
-define(MQ_REGISTRY_SHARD, emqx_mq_registry_shard).

-record(?MQ_REGISTRY_INDEX_TAB, {
    key :: emqx_topic_index:key(nil()) | '_',
    id :: emqx_mq_types:mqid() | '_',
    is_lastvalue :: boolean() | '_',
    extra = #{} :: map() | '_'
}).

-type cursor() :: binary() | undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?MQ_REGISTRY_INDEX_TAB, [
        {type, ordered_set},
        {rlog_shard, ?MQ_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?MQ_REGISTRY_INDEX_TAB},
        {attributes, record_info(fields, ?MQ_REGISTRY_INDEX_TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?MQ_REGISTRY_INDEX_TAB].

-doc """
Create a new MQ.
""".
-spec create(emqx_mq_types:mq()) -> {ok, emqx_mq_types:mq()} | {error, queue_exists}.
create(#{topic_filter := TopicFilter, is_lastvalue := IsLastValue} = MQ0) ->
    Key = make_key(TopicFilter),
    Id = emqx_guid:gen(),
    {atomic, Result} = mria:transaction(?MQ_REGISTRY_SHARD, fun() ->
        case mnesia:read(?MQ_REGISTRY_INDEX_TAB, Key, write) of
            [] ->
                ok = mnesia:write(#?MQ_REGISTRY_INDEX_TAB{
                    key = Key, id = Id, is_lastvalue = IsLastValue
                }),
                ok;
            [_] ->
                {error, queue_exists}
        end
    end),
    case Result of
        ok ->
            MQ = MQ0#{id => Id},
            {ok, _} = emqx_mq_state_storage:create_mq_state(MQ),
            {ok, MQ};
        {error, _} = Error ->
            Error
    end.

-doc """
Find all MQs matching the given concrete topic.
""".
-spec match(emqx_types:topic()) -> [emqx_mq_types:mq_handle()].
match(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?MQ_REGISTRY_INDEX_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
                [] ->
                    [];
                [#?MQ_REGISTRY_INDEX_TAB{id = Id, is_lastvalue = IsLastValue}] ->
                    [
                        #{
                            id => Id,
                            topic_filter => emqx_topic_index:get_topic(Key),
                            is_lastvalue => IsLastValue
                        }
                    ]
            end
        end,
        Keys
    ).

-doc """
Find the MQ by its topic filter.
""".
-spec find(emqx_mq_types:mq_topic()) -> {ok, emqx_mq_types:mq()} | not_found.
find(TopicFilter) ->
    ?tp_debug(mq_registry_find, #{topic_filter => TopicFilter}),
    Key = make_key(TopicFilter),
    case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?MQ_REGISTRY_INDEX_TAB{id = Id}] ->
            emqx_mq_state_storage:find_mq(Id)
    end.

-doc """
Delete the MQ by its topic filter.
""".
-spec delete(emqx_mq_types:mq_topic()) -> ok | not_found.
delete(TopicFilter) ->
    ?tp_debug(mq_registry_delete, #{topic_filter => TopicFilter}),
    Key = make_key(TopicFilter),
    {atomic, Result} = mria:transaction(?MQ_REGISTRY_SHARD, fun() ->
        case mnesia:read(?MQ_REGISTRY_INDEX_TAB, Key, write) of
            [] ->
                not_found;
            [#?MQ_REGISTRY_INDEX_TAB{id = Id, is_lastvalue = IsLastValue}] ->
                ok = mnesia:delete(?MQ_REGISTRY_INDEX_TAB, Key, write),
                {ok, #{
                    id => Id,
                    topic_filter => emqx_topic_index:get_topic(Key),
                    is_lastvalue => IsLastValue
                }}
        end
    end),
    case Result of
        not_found ->
            not_found;
        {ok, #{id := Id} = MQHandle} ->
            case emqx_mq_consumer:find(Id) of
                {ok, ConsumerRef} ->
                    ok = emqx_mq_consumer:stop(ConsumerRef);
                not_found ->
                    ok
            end,
            ok = emqx_mq_state_storage:destroy_mq_state(MQHandle),
            ok = emqx_mq_state_storage:destroy_consumer_state(MQHandle),
            ok = emqx_mq_message_db:drop(MQHandle)
    end.

-doc """
Delete all MQs. Only for testing/maintenance.
""".
-spec delete_all() -> ok.
delete_all() ->
    _ = mria:clear_table(?MQ_REGISTRY_INDEX_TAB),
    _ = emqx_mq_state_storage:delete_all(),
    ok.

-doc """
Update the MQ by its topic filter.
`is_lastvalue` is not allowed to be updated.
""".
-spec update(emqx_mq_types:mq_topic(), emqx_mq_types:mq()) ->
    {ok, emqx_mq_types:mq()} | not_found | {error, term()}.
update(TopicFilter, UpdateFields0) ->
    Key = make_key(TopicFilter),
    UpdateFields = maps:without([topic_filter, id, is_lastvalue], UpdateFields0),
    case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?MQ_REGISTRY_INDEX_TAB{id = Id}] ->
            emqx_mq_state_storage:update_mq_state(Id, UpdateFields)
    end.

-doc """
List all MQs.
""".
-spec list() -> emqx_utils_stream:stream(emqx_mq_types:mq()).
list() ->
    mq_record_stream_to_queues(mq_record_stream()).

-doc """
List at most `Limit` MQs starting from `Cursor` position.
""".
-spec list(cursor(), non_neg_integer()) -> {[emqx_mq_types:mq()], cursor()}.
list(Cursor, Limit) when Limit >= 1 ->
    MQs0 = emqx_utils_stream:consume(
        emqx_utils_stream:limit_length(
            Limit + 1,
            mq_record_stream_to_queues(mq_record_stream(Cursor))
        )
    ),
    case length(MQs0) < Limit + 1 of
        true ->
            {MQs0, undefined};
        false ->
            MQs = lists:sublist(MQs0, Limit),
            #{topic_filter := TopicFilter} = lists:last(MQs),
            NewCursor = TopicFilter,
            {MQs, NewCursor}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

mq_record_stream() ->
    mq_record_stream(undefined).

mq_record_stream(Cursor) ->
    Stream = mq_ets_record_stream(key_from_cursor(Cursor)),
    emqx_utils_stream:chainmap(
        fun(L) -> L end,
        Stream
    ).

key_from_cursor(undefined) ->
    undefined;
key_from_cursor(Cursor) ->
    make_key(Cursor).

mq_ets_record_stream(Key) ->
    fun() ->
        case next_key(Key) of
            '$end_of_table' ->
                [];
            NextKey ->
                [ets:lookup(?MQ_REGISTRY_INDEX_TAB, NextKey) | mq_ets_record_stream(NextKey)]
        end
    end.

next_key(undefined) ->
    ets:first(?MQ_REGISTRY_INDEX_TAB);
next_key(Key) ->
    ets:next(?MQ_REGISTRY_INDEX_TAB, Key).

mq_record_stream_to_queues(Stream) ->
    emqx_utils_stream:chainmap(
        fun(#?MQ_REGISTRY_INDEX_TAB{id = Id}) ->
            case emqx_mq_state_storage:find_mq(Id) of
                {ok, MQ} ->
                    [MQ];
                not_found ->
                    []
            end
        end,
        Stream
    ).

make_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).
