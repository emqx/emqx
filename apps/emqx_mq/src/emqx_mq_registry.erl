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
    find/1
]).

-define(MQ_REGISTRY_TAB, emqx_mq_registry).
-define(MQ_REGISTRY_SHARD, emqx_mq_registry_shard).

-record(?MQ_REGISTRY_TAB, {
    topic_filter :: emqx_mq_types:mq_topic(),
    is_compacted :: boolean()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?MQ_REGISTRY_TAB, [
        {type, set},
        {rlog_shard, ?MQ_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?MQ_REGISTRY_TAB},
        {attributes, record_info(fields, ?MQ_REGISTRY_TAB)}
    ]),
    [?MQ_REGISTRY_TAB].

-spec create(emqx_mq_types:mq_topic(), boolean()) -> ok.
create(TopicFilter, IsCompacted) ->
    ok = mria:dirty_write(#?MQ_REGISTRY_TAB{topic_filter = TopicFilter, is_compacted = IsCompacted}).

-spec find(emqx_types:topic()) -> [emqx_mq_types:mq()].
find(Topic) ->
    Keys = mnesia:dirty_all_keys(?MQ_REGISTRY_TAB),
    lists:filtermap(
        fun(TopicFilter) ->
            case emqx_topic:match(Topic, TopicFilter) of
                true ->
                    case mnesia:dirty_read(?MQ_REGISTRY_TAB, TopicFilter) of
                        [] ->
                            false;
                        [#?MQ_REGISTRY_TAB{is_compacted = IsCompacted}] ->
                            {true, #{topic_filter => TopicFilter, is_compacted => IsCompacted}}
                    end;
                false ->
                    false
            end
        end,
        Keys
    ).
