%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_api).

-behaviour(minirest_api).

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    paths/0,
    fields/1,
    schema/1
]).

%% `minirest' handlers
-export([
    '/a2a/cards/list'/2,
    '/a2a/cards/card/:org_id/:unit_id/:agent_id'/2
]).

%% Internal exports
-export([filter/2]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(TAGS, [<<"A2A Registry">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "a2a".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/a2a/cards/list",
        "/a2a/cards/card/:org_id/:unit_id/:agent_id"
    ].

schema("/a2a/cards/list") ->
    #{
        'operationId' => '/a2a/cards/list',
        filter => fun ?MODULE:filter/2,
        get => #{
            tags => ?TAGS,
            description => ?DESC("card_list"),
            parameters => [
                only_global_query_param(),
                ns_query_param(),
                param_query_org_id(),
                param_query_unit_id(),
                param_query_agent_id()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(ref(card_out)),
                            example_card_list()
                        ),
                    503 => error_schema(?SERVICE_UNAVAILABLE, ?DESC("service_unavailable"))
                }
        }
    };
schema("/a2a/cards/card/:org_id/:unit_id/:agent_id") ->
    #{
        'operationId' => '/a2a/cards/card/:org_id/:unit_id/:agent_id',
        filter => fun ?MODULE:filter/2,
        get => #{
            tags => ?TAGS,
            description => ?DESC("card_get"),
            parameters => [
                ns_query_param(),
                param_path_org_id(),
                param_path_unit_id(),
                agent_id_path()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ref(card_out),
                            example_card_get()
                        ),
                    404 => error_schema(?NOT_FOUND, ?DESC("not_found")),
                    503 => error_schema(?SERVICE_UNAVAILABLE, ?DESC("service_unavailable"))
                }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC("card_delete"),
            parameters => [
                ns_query_param(),
                param_path_org_id(),
                param_path_unit_id(),
                agent_id_path()
            ],
            responses =>
                #{
                    204 => <<"">>,
                    503 => error_schema(?SERVICE_UNAVAILABLE, ?DESC("service_unavailable"))
                }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC("card_register"),
            parameters => [
                ns_query_param(),
                param_path_org_id(),
                param_path_unit_id(),
                agent_id_path()
            ],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(register_card_in),
                example_register_card_in()
            ),
            responses =>
                #{
                    204 => <<"">>,
                    400 => error_schema(?BAD_REQUEST, ?DESC("bad_request")),
                    500 => error_schema(?INTERNAL_ERROR, ?DESC("internal_error")),
                    503 => error_schema(?SERVICE_UNAVAILABLE, ?DESC("service_unavailable"))
                }
        }
    }.

fields(register_card_in) ->
    [{card, mk(binary(), #{required => true})}];
fields(card_out) ->
    [
        {namespace, mk(binary(), #{})},
        {id, mk(binary(), #{})},
        {name, mk(binary(), #{})},
        {version, mk(binary(), #{})},
        {description, mk(binary(), #{})},
        {status, mk(hoconsc:enum([?A2A_PROP_ONLINE_VAL_ATOM, ?A2A_PROP_OFFLINE_VAL_ATOM]), #{})},
        {raw, mk(binary(), #{})}
    ].

param_query_org_id() ->
    {org_id,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"my.org">>,
                desc => ?DESC("org_id_query_param")
            }
        )}.

param_query_unit_id() ->
    {unit_id,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"my.unit">>,
                desc => ?DESC("unit_id_query_param")
            }
        )}.

param_query_agent_id() ->
    {agent_id,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"my.agent">>,
                desc => ?DESC("agent_id_query_param")
            }
        )}.

ns_query_param() ->
    {ns, mk(binary(), #{in => query, required => false})}.

only_global_query_param() ->
    {only_global, mk(boolean(), #{in => query, required => false, default => false})}.

param_path_org_id() ->
    {org_id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my.org">>,
                desc => ?DESC("org_id_path")
            }
        )}.

param_path_unit_id() ->
    {unit_id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my.unit">>,
                desc => ?DESC("unit_id_path")
            }
        )}.

agent_id_path() ->
    {agent_id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my.agent">>,
                desc => ?DESC("agent_id_path")
            }
        )}.

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

'/a2a/cards/list'(get, #{query_string := QueryParams} = Req) ->
    Namespace0 = get_namespace(Req),
    Namespace =
        case maps:get(<<"only_global">>, QueryParams, false) of
            false when Namespace0 == ?global_ns -> all;
            _ -> Namespace0
        end,
    handle_list_cards(Namespace, QueryParams).

'/a2a/cards/card/:org_id/:unit_id/:agent_id'(get, Req) ->
    Namespace = get_namespace(Req),
    #{bindings := Bindings} = Req,
    handle_get_card(Namespace, Bindings);
'/a2a/cards/card/:org_id/:unit_id/:agent_id'(delete, Req) ->
    Namespace = get_namespace(Req),
    #{bindings := Bindings} = Req,
    handle_delete_card(Namespace, Bindings);
'/a2a/cards/card/:org_id/:unit_id/:agent_id'(post, Req) ->
    Namespace = get_namespace(Req),
    #{bindings := Bindings, body := Body} = Req,
    handle_register_card(Namespace, Bindings, Body).

%%-------------------------------------------------------------------------------------------------
%% Handler implementations
%%-------------------------------------------------------------------------------------------------

handle_list_cards(Namespace, QueryParams) ->
    Opts0 = parse_query_params(QueryParams),
    Opts = Opts0#{namespace => Namespace},
    Cards = emqx_a2a_registry:list_cards(Opts),
    ?OK(lists:map(fun card_out/1, Cards)).

handle_get_card(Namespace, Bindings) ->
    Opts0 = maps:with([org_id, unit_id, agent_id], Bindings),
    Opts = Opts0#{namespace => Namespace},
    case emqx_a2a_registry:list_cards(Opts) of
        [] ->
            ?NOT_FOUND(<<"Card not found">>);
        [Card | _] ->
            ?OK(card_out(Card))
    end.

handle_delete_card(Namespace, Bindings) ->
    Opts0 = maps:with([org_id, unit_id, agent_id], Bindings),
    Opts = Opts0#{namespace => Namespace},
    ok = emqx_a2a_registry:delete_card(Opts),
    ?NO_CONTENT.

handle_register_card(Namespace, Bindings, Body) ->
    #{<<"card">> := CardBin} = Body,
    Opts0 = maps:with([org_id, unit_id, agent_id], Bindings),
    Opts = Opts0#{card_bin => CardBin, namespace => Namespace},
    case emqx_a2a_registry:write_card(Opts) of
        ok ->
            ?NO_CONTENT;
        {error, Reason} ->
            case emqx_a2a_registry_adapter:format_register_error(Reason) of
                {ok, Msg} ->
                    ?BAD_REQUEST(Msg);
                error ->
                    Msg = iolist_to_binary(io_lib:format("~0p", [Reason])),
                    ?INTERNAL_ERROR(Msg)
            end
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal exports
%%-------------------------------------------------------------------------------------------------

filter(Req0, Meta) ->
    case emqx_a2a_registry_config:is_enabled() of
        true ->
            maybe
                {ok, Req1} ?= resolve_namespace(Req0, Meta),
                validate_managed_namespace(Req1, Meta)
            end;
        false ->
            ?SERVICE_UNAVAILABLE(<<"Not enabled">>)
    end.

%%-------------------------------------------------------------------------------------------------
%% Examples
%%-------------------------------------------------------------------------------------------------

example_card_list() ->
    #{
        <<"list">> =>
            #{
                summary => ?DESC("example_list"),
                value => [sample_card()]
            }
    }.

example_card_get() ->
    #{
        <<"get">> =>
            #{
                summary => ?DESC("example_get"),
                value => sample_card()
            }
    }.

example_register_card_in() ->
    #{<<"card">> => sample_card()}.

sample_card() ->
    #{
        <<"name">> => <<"some_agent">>,
        <<"description">> => <<"description">>,
        <<"version">> => <<"1">>,
        <<"supportedInterfaces">> => [
            #{
                <<"url">> => <<"http://httpbin.org/get">>,
                <<"protocolBinding">> => <<"JSONRPC">>,
                <<"protocolVersion">> => <<"0.3">>
            }
        ],
        <<"capabilities">> => #{},
        <<"defaultInputModes">> => [<<"text/plain">>],
        <<"defaultOutputModes">> => [<<"text/plain">>],
        <<"skills">> => [
            #{
                <<"id">> => <<"skill-1">>,
                <<"name">> => <<"test_skill">>,
                <<"description">> => <<"A test skill">>,
                <<"tags">> => [<<"test">>]
            }
        ]
    }.

%%-------------------------------------------------------------------------------------------------
%% Helper functions
%%-------------------------------------------------------------------------------------------------

mk(Type, Props) -> hoconsc:mk(Type, Props).
ref(Name) -> hoconsc:ref(?MODULE, Name).
array(Type) -> hoconsc:array(Type).

error_schema(Code, ?DESC(_) = MessageRef) ->
    emqx_dashboard_swagger:error_codes([Code], MessageRef).

parse_query_params(QueryParams) ->
    maps:fold(
        fun
            (<<"org_id">>, OrgId, Acc) ->
                Acc#{org_id => OrgId};
            (<<"unit_id">>, OrgId, Acc) ->
                Acc#{unit_id => OrgId};
            (<<"agent_id">>, OrgId, Acc) ->
                Acc#{agent_id => OrgId};
            (_K, _V, Acc) ->
                Acc
        end,
        #{},
        QueryParams
    ).

card_out(Card) ->
    emqx_a2a_registry_adapter:card_out(Card).

get_namespace(#{resolved_ns := Namespace}) ->
    Namespace.

parse_namespace(#{query_string := QueryString} = Req) ->
    ActorNamespace = emqx_dashboard:get_namespace(Req),
    case maps:get(<<"ns">>, QueryString, ActorNamespace) of
        QSNamespace when QSNamespace /= ActorNamespace andalso ActorNamespace /= ?global_ns ->
            {error, not_authorized};
        QSNamespace ->
            {ok, QSNamespace}
    end.

resolve_namespace(Req, _Meta) ->
    case parse_namespace(Req) of
        {ok, Namespace} ->
            {ok, Req#{resolved_ns => Namespace}};
        {error, not_authorized} ->
            ?FORBIDDEN(<<"User not authorized to operate on requested namespace">>)
    end.

validate_managed_namespace(#{resolved_ns := ?global_ns} = Req, _Meta) ->
    {ok, Req};
validate_managed_namespace(#{resolved_ns := Namespace} = Req, _Meta) ->
    Res = emqx_hooks:run_fold('namespace.resource_pre_create', [#{namespace => Namespace}], #{
        exists => false
    }),
    case Res of
        #{exists := false} ->
            ?BAD_REQUEST(<<"Managed namespace not found">>);
        #{exists := true} ->
            {ok, Req}
    end.
