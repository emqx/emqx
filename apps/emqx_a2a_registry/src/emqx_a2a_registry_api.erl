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

'/a2a/cards/list'(get, #{query_string := QueryParams} = _Req) ->
    handle_list_cards(QueryParams).

'/a2a/cards/card/:org_id/:unit_id/:agent_id'(get, Req) ->
    #{bindings := Bindings} = Req,
    handle_get_card(Bindings);
'/a2a/cards/card/:org_id/:unit_id/:agent_id'(delete, Req) ->
    #{bindings := Bindings} = Req,
    handle_delete_card(Bindings);
'/a2a/cards/card/:org_id/:unit_id/:agent_id'(post, Req) ->
    #{bindings := Bindings, body := Body} = Req,
    handle_register_card(Bindings, Body).

%%-------------------------------------------------------------------------------------------------
%% Handler implementations
%%-------------------------------------------------------------------------------------------------

handle_list_cards(QueryParams) ->
    Opts = parse_query_params(QueryParams),
    Cards = emqx_a2a_registry:list_cards(Opts),
    ?OK(lists:map(fun card_out/1, Cards)).

handle_get_card(Bindings) ->
    Opts = maps:with([org_id, unit_id, agent_id], Bindings),
    case emqx_a2a_registry:list_cards(Opts) of
        [] ->
            ?NOT_FOUND(<<"Card not found">>);
        [Card | _] ->
            ?OK(card_out(Card))
    end.

handle_delete_card(Bindings) ->
    Opts = maps:with([org_id, unit_id, agent_id], Bindings),
    ok = emqx_a2a_registry:delete_card(Opts),
    ?NO_CONTENT.

handle_register_card(Bindings, Body) ->
    #{<<"card">> := CardBin} = Body,
    Opts0 = maps:with([org_id, unit_id, agent_id], Bindings),
    Opts = Opts0#{card_bin => CardBin},
    case emqx_a2a_registry:write_card(Opts) of
        ok ->
            ?NO_CONTENT;
        {error, {bad_id, Field, Id}} ->
            Msg = iolist_to_binary(io_lib:format("Bad ~s id: ~s", [Field, Id])),
            ?BAD_REQUEST(Msg);
        {error, bad_card} ->
            Msg = <<"Card does not conform to schema">>,
            ?BAD_REQUEST(Msg);
        {error, Reason} ->
            Msg = iolist_to_binary(io_lib:format("~0p", [Reason])),
            ?INTERNAL_ERROR(Msg)
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal exports
%%-------------------------------------------------------------------------------------------------

filter(Req, _Meta) ->
    case emqx_a2a_registry_config:is_enabled() of
        true ->
            {ok, Req};
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
        <<"name">> => <<"my agent">>,
        <<"version">> => <<"1.2.3">>,
        <<"description">> => <<"My agent">>
    }.

%%-------------------------------------------------------------------------------------------------
%% helper functions
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
