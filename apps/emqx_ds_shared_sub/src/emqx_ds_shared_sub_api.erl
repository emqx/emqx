%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DESC_NOT_FOUND, <<"Queue not found">>).
-define(RESP_NOT_FOUND,
    {404, #{code => <<"NOT_FOUND">>, message => ?DESC_NOT_FOUND}}
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

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

namespace() -> "durable_queues".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

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
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    durable_queues_get(),
                    durable_queues_get_example()
                )
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Declare a durable queue">>,
            description => ?DESC("durable_queues_post"),
            'requestBody' => durable_queue_post(),
            responses => #{
                201 => emqx_dashboard_swagger:schema_with_example(
                    durable_queue_get(),
                    durable_queue_get_example()
                ),
                409 => error_codes(['CONFLICT'], ?DESC_CREATE_CONFICT)
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

'/durable_queues'(get, _Params) ->
    {200, queue_list()};
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

queue_list() ->
    %% TODO
    [].

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

validate_queue_id(Id) ->
    case emqx_topic:words(Id) of
        [Segment] when is_binary(Segment) -> true;
        _ -> {error, <<"Invalid queue id">>}
    end.

durable_queues_get() ->
    hoconsc:array(ref(durable_queue_get)).

durable_queue_get() ->
    ref(durable_queue_get).

durable_queue_post() ->
    map().

roots() -> [].

fields(durable_queue_get) ->
    [
        {id, mk(binary(), #{})}
    ].

%%--------------------------------------------------------------------
%% Examples
%%--------------------------------------------------------------------

durable_queue_get_example() ->
    #{
        id => <<"queue1">>
    }.

durable_queues_get_example() ->
    [
        #{
            id => <<"queue1">>
        },
        #{
            id => <<"queue2">>
        }
    ].
