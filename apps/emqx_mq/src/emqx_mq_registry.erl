%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry).

-moduledoc """
The module contains the registry of Message Queues.
""".

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_tables/0,
    create/1,
    find/1,
    match/1,
    delete/1
]).

%% Only for testing/debugging.
-export([
    delete_all/0
]).

-define(MQ_REGISTRY_TAB, emqx_mq_registry).
-define(MQ_REGISTRY_SHARD, emqx_mq_registry_shard).

%% NOTE
%% emqx_mq_types:mq() without topic_filter
-type registry_mq() :: map().

-record(?MQ_REGISTRY_TAB, {
    key :: emqx_topic_index:key(nil()) | '_',
    mq :: registry_mq() | '_'
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?MQ_REGISTRY_TAB, [
        {type, ordered_set},
        {rlog_shard, ?MQ_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?MQ_REGISTRY_TAB},
        {attributes, record_info(fields, ?MQ_REGISTRY_TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?MQ_REGISTRY_TAB].

-doc """
Create a new MQ.
""".
-spec create(emqx_mq_types:mq()) -> {ok, emqx_mq_types:mq()} | {error, queue_exists}.
create(#{topic_filter := TopicFilter} = MQ) ->
    case find(TopicFilter) of
        not_found ->
            Id = emqx_guid:gen(),
            Key = make_key(TopicFilter),
            RegistryMQ = maps:remove(topic_filter, MQ#{id => Id}),
            ok = mria:dirty_write(#?MQ_REGISTRY_TAB{key = Key, mq = RegistryMQ}),
            {ok, MQ#{id => Id}};
        {ok, _MQ} ->
            {error, queue_exists}
    end.

-doc """
Find all MQs matching the given concrete topic.
""".
-spec match(emqx_types:topic()) -> [emqx_mq_types:mq()].
match(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?MQ_REGISTRY_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case ets:lookup(?MQ_REGISTRY_TAB, Key) of
                [] ->
                    [];
                [#?MQ_REGISTRY_TAB{} = Record] ->
                    [from_record(Record)]
            end
        end,
        Keys
    ).

-doc """
Find the MQ by its topic filter.
""".
-spec find(emqx_mq_types:mq_topic()) -> {ok, emqx_mq_types:mq()} | not_found.
find(TopicFilter) ->
    ?tp(warning, mq_registry_find, #{topic_filter => TopicFilter}),
    Key = make_key(TopicFilter),
    case ets:lookup(?MQ_REGISTRY_TAB, Key) of
        [] ->
            not_found;
        [#?MQ_REGISTRY_TAB{} = Record] ->
            {ok, from_record(Record)}
    end.

-doc """
Delete the MQ by its topic filter.
""".
-spec delete(emqx_mq_types:mq_topic()) -> ok.
delete(TopicFilter) ->
    ?tp(warning, mq_registry_delete, #{topic_filter => TopicFilter}),
    case find(TopicFilter) of
        {ok, MQ} ->
            Key = make_key(TopicFilter),
            ok = mria:dirty_delete(?MQ_REGISTRY_TAB, Key),
            case emqx_mq_consumer_db:drop_claim(MQ, now_ms()) of
                {ok, ConsumerRef} ->
                    ok = emqx_mq_consumer:stop(ConsumerRef);
                not_found ->
                    ok
            end,
            ok = emqx_mq_consumer_db:drop_consumer_data(MQ),
            ok = emqx_mq_payload_db:drop(MQ);
        not_found ->
            ok
    end.

-doc """
Delete all MQs.
""".
-spec delete_all() -> ok.
delete_all() ->
    {atomic, ok} = mria:async_dirty(
        ?MQ_REGISTRY_SHARD,
        fun() ->
            mria:match_delete(?MQ_REGISTRY_TAB, #?MQ_REGISTRY_TAB{_ = '_'})
        end
    ),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).

from_record(#?MQ_REGISTRY_TAB{
    key = Key,
    mq = MQ
}) ->
    MQ#{topic_filter => emqx_topic_index:get_topic(Key)}.

now_ms() ->
    erlang:system_time(millisecond).
