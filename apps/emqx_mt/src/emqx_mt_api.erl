%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include("emqx_mt.hrl").

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
    '/mt/ns_list'/2,
    '/mt/managed_ns_list'/2,
    '/mt/bulk_import_configs'/2,
    '/mt/ns/:ns'/2,
    '/mt/ns/:ns/config'/2,
    '/mt/ns/:ns/client_list'/2,
    '/mt/ns/:ns/client_count'/2,
    '/mt/ns/:ns/kick_all_clients'/2
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
        "/mt/managed_ns_list",
        "/mt/ns/:ns/client_list",
        "/mt/ns/:ns/client_count",
        "/mt/ns/:ns",
        "/mt/ns/:ns/config",
        "/mt/ns/:ns/kick_all_clients",
        "/mt/bulk_import_configs"
    ].

schema("/mt/ns_list") ->
    #{
        'operationId' => '/mt/ns_list',
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
        'operationId' => '/mt/ns/:ns/client_list',
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
        'operationId' => '/mt/ns/:ns/client_count',
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
schema("/mt/managed_ns_list") ->
    #{
        'operationId' => '/mt/managed_ns_list',
        get => #{
            tags => ?TAGS,
            summary => <<"List managed namespaces">>,
            description => ?DESC("managed_ns_list"),
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
schema("/mt/ns/:ns") ->
    #{
        'operationId' => '/mt/ns/:ns',
        post => #{
            tags => ?TAGS,
            summary => <<"Create managed namespace">>,
            description => ?DESC("create_managed_ns"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    204 => <<"">>,
                    400 => error_schema('BAD_REQUEST', "Maximum number of configurations reached"),
                    409 => error_schema(
                        'CONFLICT', "Clients from this namespace are still being kicked"
                    )
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete namespace">>,
            description => ?DESC("delete_ns"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    204 => <<"">>
                }
        }
    };
schema("/mt/ns/:ns/kick_all_clients") ->
    #{
        'operationId' => '/mt/ns/:ns/kick_all_clients',
        post => #{
            tags => ?TAGS,
            summary => <<"Kick all clients in namespace (async)">>,
            description => ?DESC("kick_all_clients"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    202 => <<"Kicking process started">>,
                    409 => error_schema('CONFLICT', "Kick process already underway"),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/ns/:ns/config") ->
    #{
        'operationId' => '/mt/ns/:ns/config',
        get => #{
            tags => ?TAGS,
            summary => <<"Get managed namespace configuration">>,
            description => ?DESC("get_managed_ns_config"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ref(config_out),
                            example_config_out()
                        ),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update managed namespace configuration">>,
            description => ?DESC("update_managed_ns_config"),
            parameters => [param_path_ns()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(config_in),
                example_config_in()
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ref(config_out),
                            example_config_out()
                        ),
                    400 => error_schema('BAD_REQUEST', "Invalid configuration"),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/bulk_import_configs") ->
    #{
        'operationId' => '/mt/bulk_import_configs',
        post => #{
            tags => ?TAGS,
            summary => <<"Upsert namespace configurations in bulk">>,
            description => ?DESC("bulk_import_configs"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                hoconsc:array(ref(bulk_config_in)),
                example_bulk_configs_in()
            ),
            responses =>
                #{
                    204 => <<"">>,
                    400 => error_schema('BAD_REQUEST', "Invalid configurations"),
                    500 => error_schema('INTERNAL_ERROR', "Some side-effects failed to execute")
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

fields(config_in) ->
    [
        {limiter, mk(ref(limiter_config_in), #{})},
        {session, mk(ref(session_config_in), #{})}
    ];
fields(limiter_config_in) ->
    [
        {tenant, mk(hoconsc:union([disabled, ref(limiter_in)]), #{})},
        {client, mk(hoconsc:union([disabled, ref(limiter_in)]), #{})}
    ];
fields(limiter_in) ->
    [
        {bytes, mk(ref(limiter_options), #{})},
        {messages, mk(ref(limiter_options), #{})}
    ];
fields(session_config_in) ->
    [
        {max_sessions, mk(hoconsc:union([infinity, non_neg_integer()]), #{})}
    ];
fields(bulk_config_in) ->
    [
        {ns, mk(binary(), #{})},
        {config, mk(ref(config_in), #{})}
    ];
fields(config_out) ->
    %% At this moment, same schema as input
    fields(config_in);
fields(limiter_out) ->
    %% At this moment, same schema as input
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

'/mt/ns_list'(get, Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastNs = maps:get(<<"last_ns">>, QS, ?MIN_NS),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    ?OK(emqx_mt:list_ns(LastNs, Limit)).

'/mt/ns/:ns/client_list'(get, #{bindings := #{ns := Ns}} = Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastClientId = maps:get(<<"last_clientid">>, QS, ?MIN_CLIENTID),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    case emqx_mt:list_clients(Ns, LastClientId, Limit) of
        {ok, Clients} -> ?OK(Clients);
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

'/mt/ns/:ns/client_count'(get, #{bindings := #{ns := Ns}}) ->
    case emqx_mt:count_clients(Ns) of
        {ok, Count} -> ?OK(#{count => Count});
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

'/mt/managed_ns_list'(get, Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastNs = maps:get(<<"last_ns">>, QS, ?MIN_NS),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    ?OK(emqx_mt:list_managed_ns(LastNs, Limit)).

'/mt/ns/:ns'(post, #{bindings := #{ns := Ns}}) ->
    handle_create_managed_ns(Ns);
'/mt/ns/:ns'(delete, #{bindings := #{ns := Ns}}) ->
    case emqx_mt_config:delete_managed_ns(Ns) of
        ok ->
            ?NO_CONTENT;
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

'/mt/ns/:ns/config'(get, #{bindings := #{ns := Ns}}) ->
    with_known_managed_ns(Ns, fun() -> handle_get_managed_ns_config(Ns) end);
'/mt/ns/:ns/config'(put, #{body := Params, bindings := #{ns := Ns}}) ->
    with_known_managed_ns(Ns, fun() -> handle_update_managed_ns_config(Ns, Params) end).

'/mt/bulk_import_configs'(post, #{body := Params}) ->
    handle_bulk_import_configs(Params).

'/mt/ns/:ns/kick_all_clients'(post, #{bindings := #{ns := Ns}}) ->
    with_known_ns(Ns, fun() -> handle_kick_all_clients(Ns) end).

%%-------------------------------------------------------------------------------------------------
%% Handler implementations
%%-------------------------------------------------------------------------------------------------

handle_get_managed_ns_config(Ns) ->
    case emqx_mt_config:get_managed_ns_config(Ns) of
        {ok, Configs} ->
            ?OK(configs_out(Configs));
        {error, not_found} ->
            managed_ns_not_found()
    end.

handle_create_managed_ns(Ns) ->
    case emqx_mt_client_kicker:whereis_kicker(Ns) of
        {error, not_found} ->
            case emqx_mt_config:create_managed_ns(Ns) of
                ok ->
                    ?NO_CONTENT;
                {error, table_is_full} ->
                    ?BAD_REQUEST(<<"Maximum number of managed namespaces reached">>);
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end;
        {ok, _} ->
            Msg = <<
                "Clients from this namespace are still being kicked;"
                " please try again later"
            >>,
            ?CONFLICT(Msg)
    end.

handle_update_managed_ns_config(Ns, Configs) ->
    case emqx_mt_config:update_managed_ns_config(Ns, Configs) of
        {ok, #{configs := NewConfigs, errors := []}} ->
            ?OK(configs_out(NewConfigs));
        {error, not_found} ->
            managed_ns_not_found();
        {ok, #{errors := Errors}} ->
            Msg = #{
                hint => <<
                    "Configurations were persisted, but some necessary"
                    " side-effects failed to execute; please check the logs"
                >>,
                errors => Errors
            },
            ?INTERNAL_ERROR(Msg)
    end.

handle_bulk_import_configs(Entries) ->
    case emqx_mt_config:bulk_import_configs(Entries) of
        {ok, #{errors := []}} ->
            ?NO_CONTENT;
        {ok, #{errors := Errors}} ->
            Msg = #{
                hint => <<
                    "Configurations were persisted, but some necessary"
                    " side-effects failed to execute; please check the logs"
                >>,
                errors => Errors
            },
            ?INTERNAL_ERROR(Msg);
        {error, {aborted, table_is_full}} ->
            ?BAD_REQUEST(<<"Maximum number of managed namespaces reached">>);
        {error, {duplicated_nss, Duplicated}} ->
            Msg = iolist_to_binary([
                <<"Duplicated namespaces in input: ">>,
                lists:join(<<", ">>, Duplicated)
            ]),
            ?BAD_REQUEST(Msg);
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

handle_kick_all_clients(Ns) ->
    case emqx_mt_client_kicker:start_kicking(Ns) of
        ok ->
            ?ACCEPTED;
        {error, already_started} ->
            ?CONFLICT(<<"Kick process already underway">>)
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

example_config_in() ->
    #{
        <<"managed_ns_config">> =>
            #{
                <<"session">> => #{
                    <<"max_sessions">> => 10
                },
                <<"limiter">> => #{
                    <<"tenant">> => maps:get(<<"limiter">>, example_limiter_out()),
                    <<"client">> => maps:get(<<"limiter">>, example_limiter_out())
                }
            }
    }.

example_config_out() ->
    %% Same as input.  For now.
    example_config_in().

example_bulk_configs_in() ->
    #{
        <<"bulk_import_configs">> =>
            [
                #{
                    <<"ns">> => <<"ns1">>,
                    <<"config">> => #{
                        <<"limiter">> => #{
                            <<"tenant">> => maps:get(<<"limiter">>, example_limiter_out()),
                            <<"client">> => maps:get(<<"limiter">>, example_limiter_out())
                        }
                    }
                },
                #{
                    <<"ns">> => <<"ns1">>,
                    <<"config">> => #{
                        <<"session">> => #{
                            <<"max_sessions">> => 10
                        }
                    }
                }
            ]
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

configs_out(RootConfigs) ->
    maps:map(
        fun
            (limiter, Config) ->
                maps:map(
                    fun
                        (_K, disabled) -> <<"disabled">>;
                        (_K, #{} = Cfg) -> limiter_out(Cfg)
                    end,
                    Config
                );
            (_RootKey, Config) ->
                Config
        end,
        RootConfigs
    ).

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

with_known_managed_ns(Ns, Fn) ->
    case emqx_mt_config:is_known_managed_ns(Ns) of
        true ->
            Fn();
        false ->
            managed_ns_not_found()
    end.

with_known_ns(Ns, Fn) ->
    case emqx_mt_state:is_known_ns(Ns) of
        true ->
            Fn();
        false ->
            ns_not_found()
    end.

ns_not_found() ->
    ?NOT_FOUND(<<"Namespace not found">>).

managed_ns_not_found() ->
    ?NOT_FOUND(<<"Managed namespace not found">>).
