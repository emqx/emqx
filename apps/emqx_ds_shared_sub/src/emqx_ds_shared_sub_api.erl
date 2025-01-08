%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DESC_BAD_REQUEST, <<"Erroneous request">>).
-define(RESP_BAD_REQUEST(MSG),
    {400, #{code => <<"BAD_REQUEST">>, message => MSG}}
).

-define(DESC_NOT_FOUND, <<"Queue not found">>).
-define(DESC_DISABLED, <<"Feature is disabled">>).
-define(RESP_NOT_FOUND, ?RESP_NOT_FOUND(?DESC_NOT_FOUND)).
-define(RESP_NOT_FOUND(MSG),
    {404, #{code => <<"NOT_FOUND">>, message => MSG}}
).

-define(DESC_CREATE_CONFICT, <<"Queue with given group name and topic filter already exists">>).
-define(RESP_CREATE_CONFLICT,
    {409, #{code => <<"CONFLICT">>, message => ?DESC_CREATE_CONFICT}}
).

-define(DESC_DELETE_CONFLICT, <<"Queue is currently active">>).
-define(RESP_DELETE_CONFLICT,
    {409, #{code => <<"CONFLICT">>, message => ?DESC_DELETE_CONFLICT}}
).

-define(RESP_INTERNAL_ERROR(MSG),
    {500, #{code => <<"INTERNAL_ERROR">>, message => MSG}}
).

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    fields/1,
    roots/0
]).

-define(TAGS, [<<"Durable Queues">>]).

%% API callbacks
-export([
    '/durable_queues'/2,
    '/durable_queues/:id'/2
]).

%% Internal exports
-export([
    check_enabled/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

namespace() -> "durable_queues".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true,
        filter => fun ?MODULE:check_enabled/2
    }).

paths() ->
    [
        "/durable_queues",
        "/durable_queues/:id"
    ].

schema("/durable_queues") ->
    #{
        'operationId' => '/durable_queues',
        get => #{
            tags => ?TAGS,
            summary => <<"List declared durable queues">>,
            description => ?DESC("durable_queues_get"),
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, cursor),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => resp_list_durable_queues(),
                400 => error_codes(['BAD_REQUEST'], ?DESC_BAD_REQUEST),
                404 => error_codes(['NOT_FOUND'], ?DESC_DISABLED)
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Declare a durable queue">>,
            description => ?DESC("durable_queues_post"),
            'requestBody' => durable_queue_post(),
            responses => #{
                201 => resp_create_durable_queue(),
                409 => error_codes(['CONFLICT'], ?DESC_CREATE_CONFICT),
                404 => error_codes(['NOT_FOUND'], ?DESC_DISABLED)
            }
        }
    };
schema("/durable_queues/:id") ->
    #{
        'operationId' => '/durable_queues/:id',
        get => #{
            tags => ?TAGS,
            summary => <<"Get a declared durable queue">>,
            description => ?DESC("durable_queue_get"),
            parameters => [param_queue_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    durable_queue_get(),
                    durable_queue_get_example()
                ),
                404 => error_codes(['NOT_FOUND'], ?DESC_NOT_FOUND)
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete a declared durable queue">>,
            description => ?DESC("durable_queue_delete"),
            parameters => [param_queue_id()],
            responses => #{
                200 => <<"Queue deleted">>,
                404 => error_codes(['NOT_FOUND'], ?DESC_NOT_FOUND),
                409 => error_codes(['CONFLICT'], ?DESC_DELETE_CONFLICT)
            }
        }
    }.

check_enabled(Request, _ReqMeta) ->
    case emqx_ds_shared_sub_config:enabled() of
        true -> {ok, Request};
        false -> ?RESP_NOT_FOUND(<<"Durable queues are disabled">>)
    end.

'/durable_queues'(get, #{query_string := QString}) ->
    Cursor = maps:get(<<"cursor">>, QString, undefined),
    Limit = maps:get(<<"limit">>, QString),
    try queue_list(Cursor, Limit) of
        {Queues, CursorNext} ->
            Data = [encode_props(ID, Props) || {ID, Props} <- Queues],
            case CursorNext of
                undefined ->
                    Meta = #{hasnext => false};
                _Cursor ->
                    Meta = #{hasnext => true, cursor => CursorNext}
            end,
            {200, #{data => Data, meta => Meta}}
    catch
        throw:Error ->
            ?RESP_BAD_REQUEST(emqx_utils:readable_error_msg(Error))
    end;
'/durable_queues'(post, #{body := Params}) ->
    case queue_declare(Params) of
        {ok, Queue} ->
            {201, encode_queue(Queue)};
        exists ->
            ?RESP_CREATE_CONFLICT;
        {error, _Class, Reason} ->
            ?RESP_INTERNAL_ERROR(emqx_utils:readable_error_msg(Reason))
    end.

'/durable_queues/:id'(get, Params) ->
    case queue_get(Params) of
        {ok, Queue} ->
            {200, encode_queue(Queue)};
        false ->
            ?RESP_NOT_FOUND
    end;
'/durable_queues/:id'(delete, Params) ->
    case queue_delete(Params) of
        ok ->
            {200, <<"Queue deleted">>};
        not_found ->
            ?RESP_NOT_FOUND;
        conflict ->
            ?RESP_DELETE_CONFLICT;
        {error, _Class, Reason} ->
            ?RESP_INTERNAL_ERROR(emqx_utils:readable_error_msg(Reason))
    end.

queue_list(Cursor, Limit) ->
    emqx_ds_shared_sub_queue:list(Cursor, Limit).

queue_get(#{bindings := #{id := ID}}) ->
    emqx_ds_shared_sub_queue:lookup(ID).

queue_delete(#{bindings := #{id := ID}}) ->
    emqx_ds_shared_sub_queue:destroy(ID).

queue_declare(#{<<"group">> := Group, <<"topic">> := TopicFilter} = Params) ->
    CreatedAt = emqx_message:timestamp_now(),
    StartTime = maps:get(<<"start_time">>, Params, CreatedAt),
    emqx_ds_shared_sub_queue:declare(Group, TopicFilter, CreatedAt, StartTime).

%%--------------------------------------------------------------------

encode_queue(Queue) ->
    maps:merge(
        #{id => emqx_ds_shared_sub_queue:id(Queue)},
        emqx_ds_shared_sub_queue:properties(Queue)
    ).

encode_props(ID, Props) ->
    maps:merge(#{id => ID}, Props).

%%--------------------------------------------------------------------
%% Schemas
%%--------------------------------------------------------------------

param_queue_id() ->
    {
        id,
        mk(binary(), #{
            in => path,
            desc => ?DESC(param_queue_id),
            required => true,
            validator => fun validate_queue_id/1
        })
    }.

resp_list_durable_queues() ->
    emqx_dashboard_swagger:schema_with_example(
        ref(resp_list_durable_queues),
        durable_queues_list_example()
    ).

resp_create_durable_queue() ->
    emqx_dashboard_swagger:schema_with_example(
        durable_queue_post(),
        durable_queue_get_example()
    ).

validate_queue_id(Id) ->
    case emqx_topic:words(Id) of
        [Segment] when is_binary(Segment) -> true;
        _ -> {error, <<"Invalid queue id">>}
    end.

durable_queue_get() ->
    ref(durable_queue).

durable_queue_post() ->
    ref(durable_queue_args).

roots() -> [].

fields(durable_queue) ->
    [
        {id,
            mk(binary(), #{
                desc => <<"Identifier assigned at creation time">>
            })},
        {created_at,
            mk(emqx_utils_calendar:epoch_millisecond(), #{
                desc => <<"Queue creation time">>
            })}
        | fields(durable_queue_args)
    ];
fields(durable_queue_args) ->
    [
        {group, mk(binary(), #{required => true})},
        {topic, mk(binary(), #{required => true})},
        {start_time, mk(emqx_utils_calendar:epoch_millisecond(), #{})}
    ];
fields(resp_list_durable_queues) ->
    [
        {data, hoconsc:mk(hoconsc:array(durable_queue_get()), #{})},
        {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta_with_cursor), #{})}
    ].

%%--------------------------------------------------------------------
%% Examples
%%--------------------------------------------------------------------

durable_queue_get_example() ->
    #{
        id => <<"mygrp:1234EF">>,
        created_at => <<"2024-01-01T12:34:56.789+02:00">>,
        group => <<"mygrp">>,
        topic => <<"t/devices/#">>,
        start_time => <<"2024-01-01T00:00:00.000+02:00">>
    }.

durable_queues_list_example() ->
    #{
        data => [
            durable_queue_get_example(),
            #{
                id => <<"mygrp:567890AABBCC">>,
                created_at => <<"2024-02-02T22:33:44.000+02:00">>,
                group => <<"mygrp">>,
                topic => <<"t/devices/#">>,
                start_time => <<"1970-01-01T02:00:00+02:00">>
            }
        ],
        %% TODO: Probably needs to be defined in `emqx_dashboard_swagger`.
        meta => #{
            <<"count">> => 2,
            <<"cursor">> => <<"g2wAAAADYQFhAm0AAAACYzJq">>,
            <<"hasnext">> => true
        }
    }.
