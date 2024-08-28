%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

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

-define(NOT_FOUND, 'NOT_FOUND').

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
                404 => error_codes([?NOT_FOUND], <<"Queue Not Found">>)
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete a declared durable queue">>,
            description => ?DESC("durable_queue_delete"),
            parameters => [param_queue_id()],
            responses => #{
                200 => <<"Queue deleted">>,
                404 => error_codes([?NOT_FOUND], <<"Queue Not Found">>)
            }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Declare a durable queue">>,
            description => ?DESC("durable_queues_put"),
            parameters => [param_queue_id()],
            'requestBody' => durable_queue_put(),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    durable_queue_get(),
                    durable_queue_get_example()
                )
            }
        }
    }.

'/durable_queues'(get, _Params) ->
    {200, queue_list()}.

'/durable_queues/:id'(get, Params) ->
    case queue_get(Params) of
        {ok, Queue} -> {200, Queue};
        not_found -> serialize_error(not_found)
    end;
'/durable_queues/:id'(delete, Params) ->
    case queue_delete(Params) of
        ok -> {200, <<"Queue deleted">>};
        not_found -> serialize_error(not_found)
    end;
'/durable_queues/:id'(put, Params) ->
    {200, queue_put(Params)}.

%%--------------------------------------------------------------------
%% Actual handlers: stubs
%%--------------------------------------------------------------------

queue_list() ->
    persistent_term:get({?MODULE, queues}, []).

queue_get(#{bindings := #{id := ReqId}}) ->
    case [Q || #{id := Id} = Q <- queue_list(), Id =:= ReqId] of
        [Queue] -> {ok, Queue};
        [] -> not_found
    end.

queue_delete(#{bindings := #{id := ReqId}}) ->
    Queues0 = queue_list(),
    Queues1 = [Q || #{id := Id} = Q <- Queues0, Id =/= ReqId],
    persistent_term:put({?MODULE, queues}, Queues1),
    case Queues0 =:= Queues1 of
        true -> not_found;
        false -> ok
    end.

queue_put(#{bindings := #{id := ReqId}}) ->
    Queues0 = queue_list(),
    Queues1 = [Q || #{id := Id} = Q <- Queues0, Id =/= ReqId],
    NewQueue = #{
        id => ReqId
    },
    Queues2 = [NewQueue | Queues1],
    persistent_term:put({?MODULE, queues}, Queues2),
    NewQueue.

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

durable_queue_put() ->
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

%%--------------------------------------------------------------------
%% Error codes
%%--------------------------------------------------------------------

serialize_error(not_found) ->
    {404, #{
        code => <<"NOT_FOUND">>,
        message => <<"Queue Not Found">>
    }}.
