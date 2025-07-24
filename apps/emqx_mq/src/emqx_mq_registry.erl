%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry).

-moduledoc """
The module contains the registry of Message Queues.
""".

-export([
    create_tables/0,
    create/2,
    find/1,
    delete/1
]).

%% Only for testing/debugging.
-export([
    delete_all/0
]).

-define(MQ_REGISTRY_TAB, emqx_mq_registry).
-define(MQ_REGISTRY_SHARD, emqx_mq_registry_shard).

-record(?MQ_REGISTRY_TAB, {
    key :: emqx_topic_index:key(nil()) | '_',
    is_compacted :: boolean() | '_'
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

-spec create(emqx_mq_types:mq_topic(), boolean()) -> ok.
create(TopicFilter, IsCompacted) ->
    Key = make_key(TopicFilter),
    ok = mria:dirty_write(#?MQ_REGISTRY_TAB{key = Key, is_compacted = IsCompacted}).

-spec find(emqx_mq_types:mq_topic()) -> [emqx_mq_types:mq()].
find(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?MQ_REGISTRY_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case ets:lookup(?MQ_REGISTRY_TAB, Key) of
                [] ->
                    [];
                [#?MQ_REGISTRY_TAB{is_compacted = IsCompacted}] ->
                    TopicFilter = emqx_topic_index:get_topic(Key),
                    [#{topic_filter => TopicFilter, is_compacted => IsCompacted}]
            end
        end,
        Keys
    ).

-spec delete(emqx_mq_types:mq_topic()) -> ok.
delete(TopicFilter) ->
    Key = make_key(TopicFilter),
    ok = mria:dirty_delete(?MQ_REGISTRY_TAB, Key).

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
