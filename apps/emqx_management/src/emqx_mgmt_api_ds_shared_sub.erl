%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_ds_shared_sub).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DESC_BAD_REQUEST, <<"Erroneous request">>).
-define(RESP_BAD_REQUEST(MSG),
    {400, #{code => <<"BAD_REQUEST">>, message => MSG}}
).

-define(DESC_NOT_FOUND, <<"Subscription not found">>).
-define(RESP_NOT_FOUND, ?RESP_NOT_FOUND(?DESC_NOT_FOUND)).
-define(RESP_NOT_FOUND(MSG),
    {404, #{code => <<"NOT_FOUND">>, message => MSG}}
).

-define(DESC_CREATE_CONFICT,
    <<"Subscription with given group name and topic filter already exists">>
).
-define(RESP_CREATE_CONFLICT,
    {409, #{code => <<"CONFLICT">>, message => ?DESC_CREATE_CONFICT}}
).

-define(DESC_DELETE_CONFLICT, <<"Subscription is currently active">>).
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

-define(TAGS, [<<"Durable Shared Subscription">>]).

%% API callbacks
-export([
    '/durable_shared_subs'/2,
    '/durable_shared_subs/:id'/2
]).

%% Internal exports
-export([
    check_enabled/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

namespace() -> "durable_shared_subs".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true,
        filter => fun ?MODULE:check_enabled/2
    }).

paths() ->
    [
        "/durable_shared_subs",
        "/durable_shared_subs/:id"
    ].

schema("/durable_shared_subs") ->
    #{
        'operationId' => '/durable_shared_subs',
        get => #{
            tags => ?TAGS,
            summary => <<"List declared durable subscriptions">>,
            description => ?DESC("get_subs"),
            parameters => [
                ref(emqx_dashboard_swagger, cursor),
                ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => resp_list_subs(),
                400 => error_codes(['BAD_REQUEST'], ?DESC_BAD_REQUEST)
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Declare a durable shared subscription">>,
            description => ?DESC("post_sub"),
            'requestBody' => post_sub(),
            responses => #{
                201 => resp_create_sub(),
                409 => error_codes(['CONFLICT'], ?DESC_CREATE_CONFICT)
            }
        }
    };
schema("/durable_shared_subs/:id") ->
    #{
        'operationId' => '/durable_shared_subs/:id',
        get => #{
            tags => ?TAGS,
            summary => <<"Get a declared durable shared subscription">>,
            description => ?DESC("get_sub"),
            parameters => [param_sub_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    get_sub(),
                    get_sub_example()
                ),
                404 => error_codes(['NOT_FOUND'], ?DESC_NOT_FOUND)
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete a declared durable shared subscription">>,
            description => ?DESC("delete_sub"),
            parameters => [param_sub_id()],
            responses => #{
                200 => <<"Subscription deleted">>,
                404 => error_codes(['NOT_FOUND'], ?DESC_NOT_FOUND),
                409 => error_codes(['CONFLICT'], ?DESC_DELETE_CONFLICT)
            }
        }
    }.

check_enabled(Request, _ReqMeta) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true -> {ok, Request};
        false -> ?RESP_NOT_FOUND(<<"Durable sessions are disabled">>)
    end.

'/durable_shared_subs'(get, #{query_string := QString}) ->
    Cursor = maps:get(<<"cursor">>, QString, undefined),
    Limit = maps:get(<<"limit">>, QString),
    try do_list_subs(Cursor, Limit) of
        {Subs, CursorNext} ->
            Data = [encode_props(Id, Props) || {Id, Props} <- Subs],
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
'/durable_shared_subs'(post, #{body := Params}) ->
    case do_declare_sub(Params) of
        {ok, Subscription} ->
            {201, encode_shared_sub(Subscription)};
        %% exists ->
        %%     ?RESP_CREATE_CONFLICT;
        {error, _Class, Reason} ->
            ?RESP_INTERNAL_ERROR(emqx_utils:readable_error_msg(Reason))
    end.

'/durable_shared_subs/:id'(get, Params) ->
    case do_get_sub(Params) of
        {ok, Subscription} ->
            {200, encode_shared_sub(Subscription)};
        false ->
            ?RESP_NOT_FOUND
    end;
'/durable_shared_subs/:id'(delete, Params) ->
    case do_delete_sub(Params) of
        ok ->
            {200, <<"Shared subscription deleted">>};
        not_found ->
            ?RESP_NOT_FOUND;
        {error, recoverable, _} ->
            ?RESP_DELETE_CONFLICT;
        {error, unrecoverable, Reason} ->
            ?RESP_INTERNAL_ERROR(emqx_utils:readable_error_msg(Reason))
    end.

do_list_subs(Cursor, Limit) ->
    emqx_ds_shared_sub:list(Cursor, Limit).

do_get_sub(#{bindings := #{id := Id}}) ->
    emqx_ds_shared_sub:lookup(Id).

do_delete_sub(#{bindings := #{id := Id}}) ->
    emqx_ds_shared_sub:destroy(Id).

do_declare_sub(#{<<"group">> := Group, <<"topic">> := TopicFilter} = Params) ->
    Options = maps:fold(
        fun
            (<<"start_time">>, T, Acc) ->
                [{start_time, T} | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        Params
    ),
    emqx_ds_shared_sub:declare(Group, TopicFilter, maps:from_list(Options)).

%%--------------------------------------------------------------------

encode_shared_sub(Sub) ->
    emqx_ds_shared_sub:lookup(Sub).

encode_props(Id, Props) ->
    maps:merge(#{id => Id}, Props).

%%--------------------------------------------------------------------
%% Schemas
%%--------------------------------------------------------------------

param_sub_id() ->
    {
        id,
        mk(binary(), #{
            in => path,
            desc => ?DESC(param_sub_id),
            required => true,
            validator => fun validate_sub_id/1
        })
    }.

resp_list_subs() ->
    emqx_dashboard_swagger:schema_with_example(
        ref(resp_list_durable_shared_subs),
        sub_list_example()
    ).

resp_create_sub() ->
    emqx_dashboard_swagger:schema_with_example(
        post_sub(),
        get_sub_example()
    ).

validate_sub_id(Id) ->
    case emqx_topic:words(Id) of
        [Segment] when is_binary(Segment) -> true;
        _ -> {error, <<"Invalid shared sub id">>}
    end.

get_sub() ->
    ref(durable_shared_sub).

post_sub() ->
    ref(durable_shared_sub_args).

roots() -> [].

fields(durable_shared_sub) ->
    [
        {id,
            mk(binary(), #{
                desc => <<"Identifier assigned at creation time">>
            })},
        {created_at,
            mk(emqx_utils_calendar:epoch_millisecond(), #{
                desc => <<"Creation time">>
            })}
        | fields(durable_shared_sub_args)
    ];
fields(durable_shared_sub_args) ->
    [
        {group, mk(binary(), #{required => true})},
        {topic, mk(binary(), #{required => true})},
        {start_time, mk(emqx_utils_calendar:epoch_millisecond(), #{})}
    ];
fields(resp_list_durable_shared_subs) ->
    [
        {data, mk(hoconsc:array(get_sub()), #{})},
        {meta, mk(ref(emqx_dashboard_swagger, meta_with_cursor), #{})}
    ].

%%--------------------------------------------------------------------
%% Examples
%%--------------------------------------------------------------------

get_sub_example() ->
    #{
        id => <<"mygrp:1234EF">>,
        created_at => <<"2024-01-01T12:34:56.789+02:00">>,
        group => <<"mygrp">>,
        topic => <<"t/devices/#">>,
        start_time => <<"2024-01-01T00:00:00.000+02:00">>
    }.

sub_list_example() ->
    #{
        data => [
            get_sub_example(),
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
