%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% API callbacks
-export([
    '/message_queues'/2,
    '/message_queues/:topic_filter'/2
]).

-define(TAGS, [<<"Message Queue">>]).

namespace() -> "mq".

%%--------------------------------------------------------------------
%% Minirest
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/message_queues",
        "/message_queues/:topic_filter"
    ].

schema("/message_queues") ->
    #{
        'operationId' => '/message_queues',
        get => #{
            tags => ?TAGS,
            summary => <<"List all message queues">>,
            description => ?DESC(message_queues_list),
            parameters => [],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(ref(emqx_mq_schema, message_queue)),
                    [get_message_queue_example()]
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Message queue not found">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Create message queue">>,
            description => ?DESC(message_queues_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(emqx_mq_schema, message_queue),
                post_message_queue_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_mq_schema, message_queue),
                    get_message_queue_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['ALREADY_EXISTS'], <<"Message queue already exists">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    };
schema("/message_queues/:topic_filter") ->
    #{
        'operationId' => '/message_queues/:topic_filter',
        get => #{
            tags => ?TAGS,
            summary => <<"Get message queue">>,
            description => ?DESC(message_queues_get),
            parameters => [topic_filter_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_mq_schema, message_queue),
                    get_message_queue_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Message queue not found">>
                )
            }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update message queue">>,
            description => ?DESC(message_queues_update),
            parameters => [topic_filter_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(emqx_mq_schema, message_queue_api_put),
                put_message_queue_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_mq_schema, message_queue),
                    get_message_queue_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Message queue not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_CREDENTIAL'], <<"Invalid message queue">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete message queue">>,
            description => ?DESC(message_queues_delete),
            parameters => [topic_filter_param()],
            responses => #{
                204 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Message queue not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_CREDENTIAL'], <<"Invalid message queue">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    }.

%%--------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------

topic_filter_param() ->
    {topic_filter,
        hoconsc:mk(binary(), #{
            default => <<>>,
            required => true,
            desc => ?DESC(name),
            validator => fun emqx_schema:non_empty_string/1,
            in => path
        })}.

put_message_queue_example() ->
    maps:without([<<"topic_filter">>, <<"is_compacted">>], get_message_queue_example()).

get_message_queue_example() ->
    #{
        <<"busy_session_retry_interval">> => 100,
        <<"consumer_max_inactive">> => 30000,
        <<"consumer_persistence_interval">> => 10000,
        <<"data_retention_period">> => 604800000,
        <<"dispatch_expression">> => <<"m.clientid(message)">>,
        <<"dispatch_strategy">> => <<"random">>,
        <<"is_compacted">> => false,
        <<"local_max_inflight">> => 10,
        <<"ping_interval">> => 10000,
        <<"redispatch_interval">> => 100,
        <<"stream_max_buffer_size">> => 2000,
        <<"stream_max_unacked">> => 1000,
        <<"topic_filter">> => <<"t/1">>
    }.

post_message_queue_example() ->
    get_message_queue_example().

%%--------------------------------------------------------------------
%% Minirest handlers
%%--------------------------------------------------------------------

'/message_queues'(get, _Params) ->
    %% TODO
    %% Add pagination
    {200, get_message_queues()};
'/message_queues'(post, #{body := NewMessageQueueRaw}) ->
    case add_message_queue(NewMessageQueueRaw) of
        {ok, CreatedMessageQueueRaw} ->
            {200, CreatedMessageQueueRaw};
        {error, queue_exists} ->
            {400, #{code => 'ALREADY_EXISTS', message => <<"Message queue already exists">>}}
    end.

'/message_queues/:topic_filter'(get, #{bindings := #{topic_filter := TopicFilter}}) ->
    case get_message_queue(TopicFilter) of
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Message queue not found">>}};
        {ok, MessageQueue} ->
            {200, MessageQueue}
    end;
'/message_queues/:topic_filter'(put, #{
    body := UpdatedMessageQueue, bindings := #{topic_filter := TopicFilter}
}) ->
    case update_message_queue(TopicFilter, UpdatedMessageQueue) of
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Message queue not found">>}};
        {ok, MQRaw} ->
            {200, MQRaw}
    end;
'/message_queues/:topic_filter'(delete, #{bindings := #{topic_filter := TopicFilter}}) ->
    case delete_message_queue(TopicFilter) of
        not_found ->
            {204};
        ok ->
            {204}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_message_queues() ->
    [emqx_mq_config:mq_to_raw_config(MQ) || MQ <- emqx_mq_registry:list()].

add_message_queue(NewMessageQueueRaw) ->
    NewMessageQueue = emqx_mq_config:mq_from_raw_config(NewMessageQueueRaw),
    case emqx_mq_registry:create(NewMessageQueue) of
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_config(MQ)};
        {error, queue_exists} ->
            {error, queue_exists}
    end.

get_message_queue(TopicFilter) ->
    case emqx_mq_registry:find(TopicFilter) of
        not_found ->
            not_found;
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_config(MQ)}
    end.

update_message_queue(TopicFilter, UpdatedMessageQueueRaw) ->
    UpdatedMessageQueue = emqx_mq_config:mq_update_from_raw_config(UpdatedMessageQueueRaw),
    case emqx_mq_registry:update(TopicFilter, UpdatedMessageQueue) of
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_config(MQ)};
        not_found ->
            not_found
    end.

delete_message_queue(TopicFilter) ->
    emqx_mq_registry:delete(TopicFilter).

ref(Module, Name) ->
    hoconsc:ref(Module, Name).
