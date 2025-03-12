%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include("emqx_mt.hrl").
%% -include_lib("emqx/include/logger.hrl").

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    fields/1,
    paths/0,
    schema/1
]).

%% `minirest' handlers
-export([
    ns_list/2,
    client_list/2,
    client_count/2,
    tenant_limiter/2,
    client_limiter/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"Multi-tenancy">>]).

-define(tenant_limiter, tenant).
-define(client_limiter, client).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "mt".

api_spec() ->
    emqx_dashboard_swagger:spec(
        ?MODULE,
        #{check_schema => true, translate_body => {true, atom_keys}}
    ).

paths() ->
    [
        "/mt/ns_list",
        "/mt/ns/:ns/client_list",
        "/mt/ns/:ns/client_count",
        "/mt/ns/:ns/limiter/tenant",
        "/mt/ns/:ns/limiter/client"
    ].

schema("/mt/ns_list") ->
    #{
        'operationId' => ns_list,
        get => #{
            tags => ?TAGS,
            summary => <<"List Namespaces">>,
            description => ?DESC("ns_list"),
            parameters => [
                last_ns_in_query(),
                limit_in_query()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(binary()),
                            example_ns_list()
                        )
                }
        }
    };
schema("/mt/ns/:ns/client_list") ->
    #{
        'operationId' => client_list,
        get => #{
            tags => ?TAGS,
            summary => <<"List Clients in a Namespace">>,
            description => ?DESC("client_list"),
            parameters => [
                param_path_ns(),
                last_clientid_in_query(),
                limit_in_query()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(binary()),
                            example_client_list()
                        ),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/ns/:ns/client_count") ->
    #{
        'operationId' => client_count,
        get => #{
            tags => ?TAGS,
            summary => <<"Count Clients in a Namespace">>,
            description => ?DESC("client_count"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    200 => [{count, mk(non_neg_integer(), #{desc => <<"Client count">>})}],
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/ns/:ns/limiter/tenant") ->
    #{
        'operationId' => tenant_limiter,
        get => #{
            tags => ?TAGS,
            summary => <<"Get tenant limiter configuration">>,
            description => ?DESC("get_tenant_limiter"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    201 => ref(limiter_out),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Create tenant limiter configuration">>,
            description => ?DESC("create_tenant_limiter"),
            parameters => [param_path_ns()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(limiter_in),
                example_limiter_out()
            ),
            responses =>
                #{
                    204 => <<"">>,
                    400 => error_schema('BAD_REQUEST', "Maximum number of configurations reached"),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update tenant limiter configuration">>,
            description => ?DESC("update_tenant_limiter"),
            parameters => [param_path_ns()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(limiter_in),
                example_limiter_out()
            ),
            responses =>
                #{
                    200 => ref(limiter_out),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Update tenant limiter configuration">>,
            description => ?DESC("update_tenant_limiter"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    204 => <<"">>,
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/ns/:ns/limiter/client") ->
    #{
        'operationId' => client_limiter,
        get => #{
            tags => ?TAGS,
            summary => <<"Get client limiter configuration">>,
            description => ?DESC("get_client_limiter"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    201 => ref(limiter_out),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Create client limiter configuration">>,
            description => ?DESC("create_client_limiter"),
            parameters => [param_path_ns()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(limiter_in),
                example_limiter_out()
            ),
            responses =>
                #{
                    204 => <<"">>,
                    400 => error_schema('BAD_REQUEST', "Maximum number of configurations reached"),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update client limiter configuration">>,
            description => ?DESC("update_client_limiter"),
            parameters => [param_path_ns()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(limiter_in),
                example_limiter_out()
            ),
            responses =>
                #{
                    200 => ref(limiter_out),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Update client limiter configuration">>,
            description => ?DESC("update_client_limiter"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    204 => <<"">>,
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    }.

param_path_ns() ->
    {ns,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"ns1">>,
                desc => ?DESC("param_path_ns")
            }
        )}.

last_ns_in_query() ->
    {last_ns,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"ns1">>,
                desc => ?DESC("last_ns_in_query")
            }
        )}.

limit_in_query() ->
    {limit,
        mk(
            pos_integer(),
            #{
                in => query,
                required => false,
                example => 100,
                desc => ?DESC("limit_in_query")
            }
        )}.

last_clientid_in_query() ->
    {last_clientid,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"clientid1">>,
                desc => ?DESC("last_clientid_in_query")
            }
        )}.

fields(limiter_in) ->
    [
        {bytes, mk(ref(limiter_options), #{})},
        {messages, mk(ref(limiter_options), #{})}
    ];
fields(limiter_out) ->
    fields(limiter_in);
fields(limiter_options) ->
    [
        {rate, mk(emqx_limiter_schema:rate_type(), #{})},
        {burst, mk(emqx_limiter_schema:burst_type(), #{})}
    ].

error_schema(Code, Message) ->
    BinMsg = unicode:characters_to_binary(Message),
    emqx_dashboard_swagger:error_codes([Code], BinMsg).

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

ns_list(get, Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastNs = maps:get(<<"last_ns">>, QS, ?MIN_NS),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    ?OK(emqx_mt:list_ns(LastNs, Limit)).

client_list(get, #{bindings := #{ns := Ns}} = Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastClientId = maps:get(<<"last_clientid">>, QS, ?MIN_CLIENTID),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    case emqx_mt:list_clients(Ns, LastClientId, Limit) of
        {ok, Clients} -> ?OK(Clients);
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

client_count(get, #{bindings := #{ns := Ns}}) ->
    case emqx_mt:count_clients(Ns) of
        {ok, Count} -> ?OK(#{count => Count});
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

tenant_limiter(get, #{bindings := #{ns := Ns}}) ->
    %% TODO: check NS exists and was explicitly created.
    case emqx_mt_config:get_tenant_limiter_config(Ns) of
        {ok, Limiter} ->
            ?OK(limiter_out(Limiter));
        {error, not_found} ->
            ?NOT_FOUND(<<"Limiter configuration not found">>)
    end;
tenant_limiter(delete, #{bindings := #{ns := Ns}}) ->
    case emqx_mt_config:delete_tenant_limiter_config(Ns) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NO_CONTENT;
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end;
tenant_limiter(post, #{bindings := #{ns := Ns}, body := Params}) ->
    case emqx_mt_config:get_tenant_limiter_config(Ns) of
        {error, not_found} ->
            handle_create_limiter(?tenant_limiter, Ns, Params);
        {ok, _} ->
            ?BAD_REQUEST(<<"Limiter config already exists">>)
    end;
tenant_limiter(put, #{bindings := #{ns := Ns}, body := Params}) ->
    case emqx_mt_config:get_tenant_limiter_config(Ns) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Limiter config not found">>);
        {ok, _} ->
            handle_update_limiter(?tenant_limiter, Ns, Params)
    end.

client_limiter(get, #{bindings := #{ns := Ns}}) ->
    %% TODO: check NS exists and was explicitly created.
    case emqx_mt_config:get_client_limiter_config(Ns) of
        {ok, Limiter} ->
            ?OK(limiter_out(Limiter));
        {error, not_found} ->
            ?NOT_FOUND(<<"Limiter configuration not found">>)
    end;
client_limiter(delete, #{bindings := #{ns := Ns}}) ->
    case emqx_mt_config:delete_client_limiter_config(Ns) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NO_CONTENT;
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end;
client_limiter(post, #{bindings := #{ns := Ns}, body := Params}) ->
    case emqx_mt_config:get_client_limiter_config(Ns) of
        {error, not_found} ->
            handle_create_limiter(?client_limiter, Ns, Params);
        {ok, _} ->
            ?BAD_REQUEST(<<"Limiter config already exists">>)
    end;
client_limiter(put, #{bindings := #{ns := Ns}, body := Params}) ->
    case emqx_mt_config:get_client_limiter_config(Ns) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Limiter config not found">>);
        {ok, _} ->
            handle_update_limiter(?client_limiter, Ns, Params)
    end.

%%-------------------------------------------------------------------------------------------------
%% Handler implementations
%%-------------------------------------------------------------------------------------------------

%% TODO: check NS exists and was explicitly created.
handle_create_limiter(?tenant_limiter, Ns, Limiter) ->
    case emqx_mt_config:set_tenant_limiter_config(Ns, Limiter) of
        ok ->
            ?CREATED(limiter_out(Limiter));
        {error, table_is_full} ->
            ?BAD_REQUEST(<<"Maximum number of configurations reached">>);
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end;
handle_create_limiter(?client_limiter, Ns, Limiter) ->
    case emqx_mt_config:set_client_limiter_config(Ns, Limiter) of
        ok ->
            ?CREATED(limiter_out(Limiter));
        {error, table_is_full} ->
            ?BAD_REQUEST(<<"Maximum number of configurations reached">>);
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

handle_update_limiter(?tenant_limiter, Ns, Limiter) ->
    case emqx_mt_config:set_tenant_limiter_config(Ns, Limiter) of
        ok ->
            ?OK(limiter_out(Limiter));
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end;
handle_update_limiter(?client_limiter, Ns, Limiter) ->
    case emqx_mt_config:set_client_limiter_config(Ns, Limiter) of
        ok ->
            ?OK(limiter_out(Limiter));
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

%%-------------------------------------------------------------------------------------------------
%% helper functions
%%-------------------------------------------------------------------------------------------------

mk(Type, Props) -> hoconsc:mk(Type, Props).
array(Type) -> hoconsc:array(Type).
ref(Name) -> hoconsc:ref(?MODULE, Name).

example_ns_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [<<"ns1">>, <<"ns2">>]
            }
    }.

example_client_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [<<"client1">>, <<"client2">>]
            }
    }.

example_limiter_out() ->
    #{
        <<"limiter">> =>
            #{
                <<"bytes">> => #{
                    <<"rate">> => <<"10MB/10s">>,
                    <<"burst">> => <<"200MB/1m">>
                },
                <<"messages">> => #{
                    <<"rate">> => <<"3000/1s">>,
                    <<"burst">> => <<"40/1m">>
                }
            }
    }.

limiter_out(LimiterConfigs) ->
    maps:map(fun limiter_config_out/2, LimiterConfigs).

limiter_config_out(Unit0, LimiterConfig) ->
    Unit =
        case Unit0 of
            bytes -> bytes;
            _ -> no_unit
        end,
    maps:map(
        fun(_K, V) ->
            emqx_limiter_schema:rate_to_str(V, Unit)
        end,
        LimiterConfig
    ).
