%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% API callbacks
-export([
    '/message_queues/queues'/2,
    '/message_queues/queues/:topic_filter'/2,
    '/message_queues/config'/2
]).

-export([
    check_ready/2
]).

-define(TAGS, [<<"Message Queue">>]).

namespace() -> "mq".

%%--------------------------------------------------------------------
%% Minirest
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true
    }).

paths() ->
    [
        "/message_queues/queues",
        "/message_queues/queues/:topic_filter",
        "/message_queues/config"
    ].

schema("/message_queues/queues") ->
    #{
        'operationId' => '/message_queues/queues',
        filter => fun ?MODULE:check_ready/2,
        get => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_list),
            description => ?DESC(message_queues_list),
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, cursor),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_mq_schema:mq_sctype_api_get()),
                    get_message_queues_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(bad_cursor)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        },
        post => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_create),
            description => ?DESC(message_queues_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_mq_schema:mq_sctype_api_post(),
                post_message_queue_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_mq_schema:mq_sctype_api_get(),
                    get_message_queue_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['ALREADY_EXISTS', 'MAX_QUEUE_COUNT_REACHED'],
                    ?DESC(cannot_create_message_queue)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        }
    };
schema("/message_queues/queues/:topic_filter") ->
    #{
        'operationId' => '/message_queues/queues/:topic_filter',
        filter => fun ?MODULE:check_ready/2,
        get => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_get),
            description => ?DESC(message_queues_get),
            parameters => [topic_filter_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_mq_schema:mq_sctype_api_get(),
                    get_message_queue_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(message_queue_not_found)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        },
        put => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_update),
            description => ?DESC(message_queues_update),
            parameters => [topic_filter_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_mq_schema:mq_sctype_api_put(),
                put_message_queue_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_mq_schema:mq_sctype_api_get(),
                    get_message_queue_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(message_queue_not_found)
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(invalid_message_queue)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_delete),
            description => ?DESC(message_queues_delete),
            parameters => [topic_filter_param()],
            responses => #{
                204 => ?DESC(message_queues_delete_success),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(message_queue_not_found)
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(invalid_message_queue)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        }
    };
schema("/message_queues/config") ->
    #{
        'operationId' => '/message_queues/config',
        get => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_config_get),
            description => ?DESC(message_queues_config_get),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_mq_schema, api_config_get),
                    get_message_queue_config_example()
                )
            }
        },
        put => #{
            tags => ?TAGS,
            summary => ?DESC(message_queues_config_update),
            description => ?DESC(message_queues_config_update),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(emqx_mq_schema, api_config_put),
                put_message_queue_config_example()
            ),
            responses => #{
                204 => ?DESC(message_queues_config_update_success),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(invalid_message_queue_config)
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
            desc => ?DESC(topic_filter),
            validator => fun emqx_schema:non_empty_string/1,
            in => path
        })}.

put_message_queue_example() ->
    maps:without([<<"topic_filter">>], get_message_queue_example()).

get_message_queue_example() ->
    #{
        <<"topic_filter">> => <<"t/1">>,
        <<"is_lastvalue">> => true,
        <<"data_retention_period">> => 604800000,
        <<"dispatch_strategy">> => <<"random">>,
        <<"key_expression">> => <<"message.from">>

        %% Hidden fields
        %% <<"busy_session_retry_interval">> => 100,
        %% <<"consumer_max_inactive">> => 30000,
        %% <<"consumer_persistence_interval">> => 10000,
        %% <<"local_max_inflight">> => 10,
        %% <<"ping_interval">> => 10000,
        %% <<"redispatch_interval">> => 100,
        %% <<"stream_max_buffer_size">> => 2000,
        %% <<"stream_max_unacked">> => 1000
    }.

post_message_queue_example() ->
    get_message_queue_example().

get_message_queues_example() ->
    #{
        data => [get_message_queue_example()],
        meta => #{
            <<"cursor">> => <<"g2wAAAADYQFhAm0AAAACYzJq">>,
            <<"hasnext">> => true
        }
    }.

get_message_queue_config_example() ->
    #{
        <<"gc_interval">> => <<"1h">>,
        <<"regular_queue_retention_period">> => <<"7d">>,
        <<"find_queue_retry_interval">> => <<"10s">>
    }.

put_message_queue_config_example() ->
    get_message_queue_config_example().

%%--------------------------------------------------------------------
%% Minirest handlers
%%--------------------------------------------------------------------

'/message_queues/queues'(get, #{query_string := QString}) ->
    EncodedCursor = maps:get(<<"cursor">>, QString, undefined),
    Limit = maps:get(<<"limit">>, QString),
    case decode_cursor(EncodedCursor) of
        {ok, Cursor} ->
            {MessageQueues, CursorNext} = get_message_queues(Cursor, Limit),
            case CursorNext of
                undefined ->
                    ?OK(#{data => MessageQueues, meta => #{hasnext => false}});
                _ ->
                    ?OK(#{
                        data => MessageQueues,
                        meta => #{cursor => encode_cursor(CursorNext), hasnext => true}
                    })
            end;
        bad_cursor ->
            ?BAD_REQUEST(<<"Invalid cursor">>)
    end;
'/message_queues/queues'(post, #{body := NewMessageQueueRaw}) ->
    case add_message_queue(NewMessageQueueRaw) of
        {ok, CreatedMessageQueueRaw} ->
            ?OK(CreatedMessageQueueRaw);
        {error, queue_exists} ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"Message queue already exists">>);
        {error, max_queue_count_reached} ->
            ?BAD_REQUEST('MAX_QUEUE_COUNT_REACHED', <<"Max queue count reached">>);
        {error, Reason} ->
            ?SERVICE_UNAVAILABLE(Reason)
    end.

'/message_queues/queues/:topic_filter'(get, #{bindings := #{topic_filter := TopicFilter}}) ->
    case get_message_queue(TopicFilter) of
        not_found ->
            ?NOT_FOUND(<<"Message queue not found">>);
        {ok, MessageQueue} ->
            ?OK(MessageQueue)
    end;
'/message_queues/queues/:topic_filter'(put, #{
    body := UpdatedMessageQueue, bindings := #{topic_filter := TopicFilter}
}) ->
    case update_message_queue(TopicFilter, UpdatedMessageQueue) of
        not_found ->
            ?NOT_FOUND(<<"Message queue not found">>);
        {ok, MQRaw} ->
            ?OK(MQRaw);
        {error, is_lastvalue_not_allowed_to_be_updated} ->
            ?BAD_REQUEST(<<"LastValue flag is not allowed to be updated">>);
        {error, limit_presence_cannot_be_updated_for_regular_queues} ->
            ?BAD_REQUEST(
                <<"Regular queues cannot be updated from limited to unlimited and vice versa">>
            );
        {error, _} = Error ->
            ?SERVICE_UNAVAILABLE(Error)
    end;
'/message_queues/queues/:topic_filter'(delete, #{bindings := #{topic_filter := TopicFilter}}) ->
    case delete_message_queue(TopicFilter) of
        not_found ->
            ?NOT_FOUND(<<"Message queue not found">>);
        {error, Reason} ->
            ?SERVICE_UNAVAILABLE(Reason);
        ok ->
            ?NO_CONTENT
    end.

'/message_queues/config'(get, _) ->
    ?OK(emqx_mq_config:raw_api_config());
'/message_queues/config'(put, #{body := Body}) ->
    case emqx_mq_config:update_config(Body) of
        {ok, _} ->
            ?NO_CONTENT;
        {error, {post_config_update, emqx_mq_config, #{reason := cannot_disable_mq_in_runtime}}} ->
            ?BAD_REQUEST(<<"Cannot disable MQ subsystem via API">>);
        {error,
            {post_config_update, emqx_mq_config, #{
                reason := cannot_enable_both_regular_and_lastvalue_auto_create
            }}} ->
            ?BAD_REQUEST(
                <<"Queues should be configured to be automatically created either as regular or lastvalue">>
            );
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

check_ready(Request, _Meta) ->
    case emqx_mq_config:is_enabled() of
        true ->
            case emqx_mq_app:is_ready() of
                true ->
                    {ok, Request};
                false ->
                    ?SERVICE_UNAVAILABLE(<<"Not ready">>)
            end;
        false ->
            ?SERVICE_UNAVAILABLE(<<"Not enabled">>)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_message_queues(Cursor, Limit) ->
    {MessageQueues, CursorNext} = emqx_mq_registry:list(Cursor, Limit),
    {[emqx_mq_config:mq_to_raw_get(MQ) || MQ <- MessageQueues], CursorNext}.

encode_cursor(Cursor) ->
    emqx_base62:encode(Cursor).

decode_cursor(undefined) ->
    {ok, undefined};
decode_cursor(EncodedCursor) ->
    try
        {ok, emqx_base62:decode(EncodedCursor)}
    catch
        _:_ ->
            bad_cursor
    end.

add_message_queue(NewMessageQueueRaw) ->
    NewMessageQueue = emqx_mq_config:mq_from_raw_post(NewMessageQueueRaw),
    case emqx_mq_registry:create(NewMessageQueue) of
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_get(MQ)};
        {error, Reason} ->
            {error, Reason}
    end.

get_message_queue(TopicFilter) ->
    case emqx_mq_registry:find(TopicFilter) of
        not_found ->
            not_found;
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_get(MQ)}
    end.

update_message_queue(TopicFilter, UpdatedMessageQueueRaw) ->
    UpdatedMessageQueue = emqx_mq_config:mq_update_from_raw_put(UpdatedMessageQueueRaw),
    case emqx_mq_registry:update(TopicFilter, UpdatedMessageQueue) of
        {ok, MQ} ->
            {ok, emqx_mq_config:mq_to_raw_get(MQ)};
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

delete_message_queue(TopicFilter) ->
    emqx_mq_registry:delete(TopicFilter).

ref(Module, Name) ->
    hoconsc:ref(Module, Name).
