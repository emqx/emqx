%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_api).

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
    '/message_streams/streams'/2,
    '/message_streams/streams/:name'/2,
    '/message_streams/config'/2
]).

-export([
    check_ready/2
]).

-define(TAGS, [<<"Message Stream">>]).

namespace() -> "stream".

%%--------------------------------------------------------------------
%% Minirest
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true
    }).

paths() ->
    [
        "/message_streams/streams",
        "/message_streams/streams/:name",
        "/message_streams/config"
    ].

schema("/message_streams/streams") ->
    #{
        'operationId' => '/message_streams/streams',
        filter => fun ?MODULE:check_ready/2,
        get => #{
            tags => ?TAGS,
            description => ?DESC(message_streams_list),
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, cursor),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_streams_schema:stream_sctype_api_get()),
                    get_message_streams_example()
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
            description => ?DESC(message_streams_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_streams_schema:stream_sctype_api_post(),
                post_message_stream_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_streams_schema:stream_sctype_api_get(),
                    get_message_stream_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['ALREADY_EXISTS', 'MAX_STREAM_COUNT_REACHED'],
                    ?DESC(cannot_create_message_stream)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        }
    };
schema("/message_streams/streams/:name") ->
    #{
        'operationId' => '/message_streams/streams/:name',
        filter => fun ?MODULE:check_ready/2,
        get => #{
            tags => ?TAGS,
            description => ?DESC(message_streams_get),
            parameters => [name_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_streams_schema:stream_sctype_api_get(),
                    get_message_stream_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(message_stream_not_found)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(message_streams_update),
            parameters => [name_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_streams_schema:stream_sctype_api_put(),
                put_message_stream_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_streams_schema:stream_sctype_api_get(),
                    get_message_stream_example()
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
            description => ?DESC(message_streams_delete),
            parameters => [name_param()],
            responses => #{
                204 => ?DESC(message_streams_delete_success),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(message_stream_not_found)
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(invalid_message_stream)
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], ?DESC(service_unavailable)
                )
            }
        }
    };
schema("/message_streams/config") ->
    #{
        'operationId' => '/message_streams/config',
        get => #{
            tags => ?TAGS,
            description => ?DESC(message_streams_config_get),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_streams_schema, api_config_get),
                    get_message_stream_config_example()
                )
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(message_streams_config_update),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(emqx_streams_schema, api_config_put),
                put_message_stream_config_example()
            ),
            responses => #{
                204 => ?DESC(message_streams_config_update_success),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(invalid_message_stream_config)
                )
            }
        }
    }.

%%--------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------

name_param() ->
    {name,
        hoconsc:mk(binary(), #{
            required => true,
            desc => ?DESC(name),
            validator => fun emqx_streams_schema:validate_name/1,
            in => path
        })}.

put_message_stream_example() ->
    maps:without([<<"name">>, <<"topic_filter">>], get_message_stream_example()).

get_message_stream_example() ->
    #{
        <<"name">> => <<"s1">>,
        <<"topic_filter">> => <<"t/1">>,
        <<"is_lastvalue">> => true,
        <<"data_retention_period">> => 604800000,
        <<"key_expression">> => <<"message.from">>

        %% Hidden fields
        %% <<"read_max_unacked">> => 1000
    }.

post_message_stream_example() ->
    get_message_stream_example().

get_message_streams_example() ->
    #{
        data => [get_message_stream_example()],
        meta => #{
            <<"cursor">> => <<"g2wAAAADYQFhAm0AAAACYzJq">>,
            <<"hasnext">> => true
        }
    }.

get_message_stream_config_example() ->
    #{
        <<"gc_interval">> => <<"1h">>,
        <<"regular_stream_retention_period">> => <<"7d">>
    }.

put_message_stream_config_example() ->
    get_message_stream_config_example().

%%--------------------------------------------------------------------
%% Minirest handlers
%%--------------------------------------------------------------------

'/message_streams/streams'(get, #{query_string := QString}) ->
    Cursor = maps:get(<<"cursor">>, QString, undefined),
    Limit = maps:get(<<"limit">>, QString),
    maybe
        {ok, MessageStreams, CursorNext} ?= get_message_streams(Cursor, Limit),
        case CursorNext of
            undefined ->
                ?OK(#{data => MessageStreams, meta => #{hasnext => false}});
            _ ->
                ?OK(#{
                    data => MessageStreams,
                    meta => #{cursor => CursorNext, hasnext => true}
                })
        end
    else
        {error, bad_cursor} ->
            ?BAD_REQUEST(<<"Invalid cursor">>)
    end;
'/message_streams/streams'(post, #{body := NewMessageStreamRaw}) ->
    case add_message_stream(NewMessageStreamRaw) of
        {ok, CreatedMessageStreamRaw} ->
            ?OK(CreatedMessageStreamRaw);
        {error, stream_exists} ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"Message stream already exists">>);
        {error, max_stream_count_reached} ->
            ?BAD_REQUEST('MAX_STREAM_COUNT_REACHED', <<"Max stream count reached">>);
        {error, Reason} ->
            ?SERVICE_UNAVAILABLE(Reason)
    end.

'/message_streams/streams/:name'(get, #{bindings := #{name := Name}}) ->
    case get_message_stream(Name) of
        not_found ->
            ?NOT_FOUND(<<"Message stream not found">>);
        {ok, Stream} ->
            ?OK(Stream)
    end;
'/message_streams/streams/:name'(put, #{
    body := UpdatedMessageStream, bindings := #{name := Name}
}) ->
    case update_message_stream(Name, UpdatedMessageStream) of
        not_found ->
            ?NOT_FOUND(<<"Message stream not found">>);
        {ok, StreamRaw} ->
            ?OK(StreamRaw);
        {error, is_lastvalue_not_allowed_to_be_updated} ->
            ?BAD_REQUEST(<<"LastValue flag is not allowed to be updated">>);
        {error, limit_presence_cannot_be_updated_for_regular_streams} ->
            ?BAD_REQUEST(
                <<"Regular streams cannot be updated from limited to unlimited and vice versa">>
            );
        {error, _} = Error ->
            ?SERVICE_UNAVAILABLE(Error)
    end;
'/message_streams/streams/:name'(delete, #{bindings := #{name := Name}}) ->
    case delete_message_stream(Name) of
        not_found ->
            ?NOT_FOUND(<<"Message stream not found">>);
        {error, Reason} ->
            ?SERVICE_UNAVAILABLE(Reason);
        ok ->
            ?NO_CONTENT
    end.

'/message_streams/config'(get, _) ->
    ?OK(emqx_streams_config:raw_api_config());
'/message_streams/config'(put, #{body := Body}) ->
    case emqx_streams_config:update_config(Body) of
        {ok, _} ->
            ?NO_CONTENT;
        {error,
            {post_config_update, emqx_streams_config, #{
                reason := cannot_stop_streams_with_existing_streams
            }}} ->
            ?BAD_REQUEST(
                <<"Cannot disable streams subsystem via API when there are existing streams">>
            );
        {error,
            {post_config_update, emqx_streams_config, #{
                reason := cannot_enable_both_regular_and_lastvalue_auto_create
            }}} ->
            ?BAD_REQUEST(
                <<"Streams should be configured to be automatically created either as regular or lastvalue">>
            );
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

check_ready(Request, _Meta) ->
    case emqx_streams_controller:status() of
        stopped -> ?SERVICE_UNAVAILABLE(<<"Not enabled">>);
        starting -> ?SERVICE_UNAVAILABLE(<<"Not ready">>);
        started -> {ok, Request}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_message_streams(Cursor, Limit) ->
    case emqx_streams_registry:list(Cursor, Limit) of
        {ok, MessageStreams, CursorNext} ->
            {ok, [emqx_streams_config:stream_to_raw_get(Stream) || Stream <- MessageStreams],
                CursorNext};
        {error, _} ->
            {error, bad_cursor}
    end.

add_message_stream(NewStreamRaw) ->
    NewStream = emqx_streams_config:stream_from_raw_post(NewStreamRaw),
    case emqx_streams_registry:create(NewStream) of
        {ok, Stream} ->
            {ok, emqx_streams_config:stream_to_raw_get(Stream)};
        {error, Reason} ->
            {error, Reason}
    end.

get_message_stream(Name) ->
    case emqx_streams_registry:find(Name) of
        not_found ->
            not_found;
        {ok, Stream} ->
            {ok, emqx_streams_config:stream_to_raw_get(Stream)}
    end.

update_message_stream(Name, UpdatedStreamRaw) ->
    UpdatedStream = emqx_streams_config:stream_update_from_raw_put(UpdatedStreamRaw),
    case emqx_streams_registry:update(Name, UpdatedStream) of
        {ok, Stream} ->
            {ok, emqx_streams_config:stream_to_raw_get(Stream)};
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

delete_message_stream(Name) ->
    emqx_streams_registry:delete(Name).

ref(Module, Name) ->
    hoconsc:ref(Module, Name).
