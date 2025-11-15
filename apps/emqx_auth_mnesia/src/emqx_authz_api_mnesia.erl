%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia).

-behaviour(minirest_api).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/1, mk/2, ref/1, ref/2, array/1, enum/1]).

-define(ACL_USERNAME_QSCHEMA, [{<<"like_username">>, binary}]).
-define(ACL_CLIENTID_QSCHEMA, [{<<"like_clientid">>, binary}]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

%% operation funs
-export([
    users/2,
    clients/2,
    user/2,
    client/2,
    all/2,
    rules/2
]).

%% query funs
-export([
    run_fuzzy_filter/2,
    format_result/1
]).

%% minirest filter callback
-export([is_configured_authz_source/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(ALREADY_EXISTS, 'ALREADY_EXISTS').

-define(TYPE_REF, ref).
-define(TYPE_ARRAY, array).
-define(PAGE_QUERY_EXAMPLE, example_in_data).
-define(PUT_MAP_EXAMPLE, in_put_requestBody).
-define(POST_ARRAY_EXAMPLE, in_post_requestBody).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/sources/built_in_database/rules/users",
        "/authorization/sources/built_in_database/rules/clients",
        "/authorization/sources/built_in_database/rules/users/:username",
        "/authorization/sources/built_in_database/rules/clients/:clientid",
        "/authorization/sources/built_in_database/rules/all",
        "/authorization/sources/built_in_database/rules"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources/built_in_database/rules/users") ->
    #{
        'operationId' => users,
        filter => fun ?MODULE:is_configured_authz_source/2,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_username_get),
                parameters =>
                    [
                        ref(emqx_dashboard_swagger, page),
                        ref(emqx_dashboard_swagger, limit),
                        {like_username,
                            mk(binary(), #{
                                in => query,
                                required => false,
                                desc => ?DESC(fuzzy_username)
                            })},
                        ns_qs_param()
                    ],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {username_response_data, ?TYPE_REF},
                            {username, ?PAGE_QUERY_EXAMPLE}
                        )
                    }
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_username_post),
                parameters => [ns_qs_param()],
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_ARRAY},
                    {username, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_username")
                        ),
                        409 => emqx_dashboard_swagger:error_codes(
                            [?ALREADY_EXISTS], ?DESC(?ALREADY_EXISTS)
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/rules/clients") ->
    #{
        'operationId' => clients,
        filter => fun ?MODULE:is_configured_authz_source/2,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_clientid_get),
                parameters =>
                    [
                        ref(emqx_dashboard_swagger, page),
                        ref(emqx_dashboard_swagger, limit),
                        {like_clientid,
                            mk(
                                binary(),
                                #{
                                    in => query,
                                    required => false,
                                    desc => ?DESC(fuzzy_clientid)
                                }
                            )},
                        ns_qs_param()
                    ],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {clientid_response_data, ?TYPE_REF},
                            {clientid, ?PAGE_QUERY_EXAMPLE}
                        )
                    }
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_clientid_post),
                parameters => [ns_qs_param()],
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_ARRAY},
                    {clientid, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_clientid")
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/rules/users/:username") ->
    #{
        'operationId' => user,
        filter => fun ?MODULE:is_configured_authz_source/2,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_get),
                parameters => [ref(username), ns_qs_param()],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_username, ?TYPE_REF},
                            {username, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC(?NOT_FOUND)
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_put),
                parameters => [ref(username), ns_qs_param()],
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_REF},
                    {username, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_username")
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_delete),
                parameters => [ref(username), ns_qs_param()],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_username")
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC(?NOT_FOUND)
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/rules/clients/:clientid") ->
    #{
        'operationId' => client,
        filter => fun ?MODULE:is_configured_authz_source/2,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_get),
                parameters => [ref(clientid), ns_qs_param()],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_clientid, ?TYPE_REF},
                            {clientid, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC(?NOT_FOUND)
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_put),
                parameters => [ref(clientid), ns_qs_param()],
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_REF},
                    {clientid, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_clientid")
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_delete),
                parameters => [ref(clientid), ns_qs_param()],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request_clientid")
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC(?NOT_FOUND)
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/rules/all") ->
    #{
        'operationId' => all,
        filter => fun ?MODULE:is_configured_authz_source/2,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_all_get),
                parameters => [ns_qs_param()],
                responses =>
                    #{200 => swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE})}
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_all_post),
                parameters => [ns_qs_param()],
                'requestBody' =>
                    swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE}),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request")
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_all_delete),
                parameters => [ns_qs_param()],
                responses =>
                    #{
                        204 => <<"Deleted">>
                    }
            }
    };
schema("/authorization/sources/built_in_database/rules") ->
    #{
        'operationId' => rules,
        filter => fun ?MODULE:is_configured_authz_source/2,
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_delete),
                parameters => [ns_qs_param()],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], ?DESC("bad_request")
                        )
                    }
            }
    }.

fields(rule_item) ->
    [
        {topic,
            mk(
                string(),
                #{
                    required => true,
                    desc => ?DESC(topic),
                    example => <<"test/topic/1">>
                }
            )},
        {permission,
            mk(
                enum([allow, deny]),
                #{
                    desc => ?DESC(permission),
                    required => true,
                    example => allow
                }
            )},
        {action,
            mk(
                enum([publish, subscribe, all]),
                #{
                    desc => ?DESC(action),
                    required => true,
                    example => publish
                }
            )},
        {qos,
            mk(
                array(emqx_schema:qos()),
                #{
                    desc => ?DESC(qos),
                    default => ?DEFAULT_RULE_QOS
                }
            )},
        {retain,
            mk(
                hoconsc:union([all, boolean()]),
                #{
                    desc => ?DESC(retain),
                    default => ?DEFAULT_RULE_RETAIN
                }
            )},
        {clientid_re,
            mk(
                binary(),
                #{
                    desc => ?DESC(clientid_re),
                    example => <<"client[0-9]+">>
                }
            )},
        {username_re,
            mk(
                binary(),
                #{
                    desc => ?DESC(username_re),
                    example => <<"user[0-9]+">>
                }
            )},
        {zone,
            mk(
                binary(),
                #{
                    desc => ?DESC(zone),
                    example => <<"zone1">>
                }
            )},
        {zone_re,
            mk(
                binary(),
                #{
                    desc => ?DESC(zone_re),
                    example => <<"zone[0-9]+">>
                }
            )},
        {listener,
            mk(
                binary(),
                #{
                    desc => ?DESC(listener),
                    example => <<"tcp:default">>
                }
            )},
        {listener_re,
            mk(
                binary(),
                #{
                    desc => ?DESC(listener_re),
                    example => <<"^tcp:.+">>
                }
            )},
        {ipaddr,
            mk(
                binary(),
                #{
                    desc => ?DESC(ipaddr),
                    example => <<"192.168.1.0/24">>
                }
            )}
    ];
fields(clientid) ->
    [
        {clientid,
            mk(
                binary(),
                #{
                    in => path,
                    required => true,
                    desc => ?DESC(clientid),
                    example => <<"client1">>
                }
            )}
    ];
fields(username) ->
    [
        {username,
            mk(
                binary(),
                #{
                    in => path,
                    required => true,
                    desc => ?DESC(username),
                    example => <<"user1">>
                }
            )}
    ];
fields(rules_for_username) ->
    fields(rules) ++
        fields(username);
fields(username_response_data) ->
    [
        {data, mk(array(ref(rules_for_username)), #{})},
        {meta, ref(emqx_dashboard_swagger, meta)}
    ];
fields(rules_for_clientid) ->
    fields(rules) ++
        fields(clientid);
fields(clientid_response_data) ->
    [
        {data, mk(array(ref(rules_for_clientid)), #{})},
        {meta, ref(emqx_dashboard_swagger, meta)}
    ];
fields(rules) ->
    [{rules, mk(array(ref(rule_item)))}].

ns_qs_param() ->
    {ns, mk(binary(), #{in => query, required => false})}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

is_configured_authz_source(Params, _Meta) ->
    emqx_authz_api_sources:with_source(
        ?AUTHZ_TYPE_BIN,
        fun(_Source) ->
            {ok, Params}
        end
    ).

users(get, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_get_rules(Namespace, username, Req) end);
users(post, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_create_rules(Namespace, username, Req) end).

clients(get, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_get_rules(Namespace, clientid, Req) end);
clients(post, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_create_rules(Namespace, clientid, Req) end).

user(get, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_get_user(Namespace, Req) end);
user(put, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_update_user(Namespace, Req) end);
user(delete, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_delete_rules(Namespace, Req) end).

client(get, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_get_user(Namespace, Req) end);
client(put, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_update_user(Namespace, Req) end);
client(delete, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_delete_rules(Namespace, Req) end).

all(get, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_get_rules(Namespace, Req) end);
all(post, Req) ->
    with_namespace(Req, fun(Namespace) -> handle_create_rules(Namespace, all, Req) end);
all(delete, Req) ->
    with_namespace(Req, fun(Namespace) ->
        emqx_authz_mnesia:store_rules(Namespace, all, []),
        {204}
    end).

rules(delete, Req) ->
    with_namespace(Req, fun(Namespace) ->
        case emqx_authz_api_sources:get_raw_source(<<"built_in_database">>) of
            [#{<<"enable">> := false}] ->
                ok = emqx_authz_mnesia:purge_rules(Namespace),
                {204};
            [#{<<"enable">> := true}] ->
                {400, #{
                    code => <<"BAD_REQUEST">>,
                    message =>
                        <<"'built_in_database' type source must be disabled before purge.">>
                }};
            [] ->
                {404, #{
                    code => <<"BAD_REQUEST">>,
                    message => <<"'built_in_database' type source is not found.">>
                }}
        end
    end).

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

mk_query_username_fn(Namespace) ->
    fun(_Tab, {_QString, FuzzyQString}) ->
        #{
            match_spec => emqx_authz_mnesia:list_username_rules(Namespace),
            fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
        }
    end.

mk_query_clientid_fn(Namespace) ->
    fun(_Tab, {_QString, FuzzyQString}) ->
        #{
            match_spec => emqx_authz_mnesia:list_clientid_rules(Namespace),
            fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
        }
    end.

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = [{username, Username}, _Rule],
    [{username, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(Username, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy);
run_fuzzy_filter(
    E = [{clientid, ClientId}, _Rule],
    [{clientid, like, ClientIdSubStr} | Fuzzy]
) ->
    binary:match(ClientId, ClientIdSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

%% format result from mnesia tab
format_result([{username, Username}, {rules, Rules}]) ->
    #{
        username => Username,
        rules => format_rules(Rules)
    };
format_result([{clientid, ClientId}, {rules, Rules}]) ->
    #{
        clientid => ClientId,
        rules => format_rules(Rules)
    }.

format_rules(Rules) ->
    [emqx_authz_rule_raw:format_rule(Rule) || Rule <- Rules].

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

handle_get_rules(Namespace, _Req) ->
    case emqx_authz_mnesia:get_rules(Namespace, all) of
        not_found ->
            {200, #{rules => []}};
        {ok, Rules} ->
            {200, #{
                rules => format_rules(Rules)
            }}
    end.

handle_get_rules(Namespace, username, #{query_string := QueryString} = _Req) ->
    Table = table(Namespace),
    case
        emqx_mgmt_api:node_query(
            node(),
            Table,
            QueryString,
            ?ACL_USERNAME_QSCHEMA,
            mk_query_username_fn(Namespace),
            fun ?MODULE:format_result/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, Error} ->
            Message = list_to_binary(
                io_lib:format("bad rpc call ~p, Reason ~p", [Node, Error])
            ),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end;
handle_get_rules(Namespace, clientid, #{query_string := QueryString} = _Req) ->
    Table = table(Namespace),
    case
        emqx_mgmt_api:node_query(
            node(),
            Table,
            QueryString,
            ?ACL_CLIENTID_QSCHEMA,
            mk_query_clientid_fn(Namespace),
            fun ?MODULE:format_result/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, Error} ->
            Message = list_to_binary(
                io_lib:format("bad rpc call ~p, Reason ~p", [Node, Error])
            ),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end.

handle_create_rules(Namespace, username, #{body := Body}) when is_list(Body) ->
    case ensure_rules_is_valid(Namespace, <<"username">>, username, Body) of
        ok ->
            lists:foreach(
                fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
                    emqx_authz_mnesia:store_rules(Namespace, {username, Username}, Rules)
                end,
                Body
            ),
            {204};
        {error, {Username, too_many_rules}} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    binfmt(
                        <<"The rules length of User '~ts' exceeds the maximum limit.">>,
                        [Username]
                    )
            }};
        {error, {already_exists, Exists}} ->
            {409, #{
                code => <<"ALREADY_EXISTS">>,
                message => binfmt("User '~ts' already exist", [Exists])
            }}
    end;
handle_create_rules(Namespace, clientid, #{body := Body}) when is_list(Body) ->
    case ensure_rules_is_valid(Namespace, <<"clientid">>, clientid, Body) of
        ok ->
            lists:foreach(
                fun(#{<<"clientid">> := ClientId, <<"rules">> := Rules}) ->
                    emqx_authz_mnesia:store_rules(Namespace, {clientid, ClientId}, Rules)
                end,
                Body
            ),
            {204};
        {error, {ClientId, too_many_rules}} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    binfmt(
                        <<"The rules length of Client '~ts' exceeds the maximum limit.">>,
                        [ClientId]
                    )
            }};
        {error, {already_exists, Exists}} ->
            {409, #{
                code => <<"ALREADY_EXISTS">>,
                message => binfmt("Client '~ts' already exist", [Exists])
            }}
    end;
handle_create_rules(Namespace, all, #{body := #{<<"rules">> := Rules}}) ->
    case ensure_rules_len(Rules) of
        ok ->
            emqx_authz_mnesia:store_rules(Namespace, all, Rules),
            {204};
        _ ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    <<"The length of rules exceeds the maximum limit.">>
            }}
    end.

handle_get_user(Namespace, #{bindings := #{username := Username}} = _Req) ->
    case emqx_authz_mnesia:get_rules(Namespace, {username, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                username => Username,
                rules => format_rules(Rules)
            }}
    end;
handle_get_user(Namespace, #{bindings := #{clientid := ClientId}}) ->
    case emqx_authz_mnesia:get_rules(Namespace, {clientid, ClientId}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                clientid => ClientId,
                rules => format_rules(Rules)
            }}
    end.

handle_update_user(Namespace, #{
    bindings := #{username := Username},
    body := #{<<"username">> := Username, <<"rules">> := Rules}
}) ->
    case ensure_rules_len(Rules) of
        ok ->
            emqx_authz_mnesia:store_rules(Namespace, {username, Username}, Rules),
            {204};
        {error, too_many_rules} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    binfmt(
                        <<"The rules length exceeds the maximum limit.">>,
                        []
                    )
            }}
    end;
handle_update_user(Namespace, #{
    bindings := #{clientid := ClientId},
    body := #{<<"clientid">> := ClientId, <<"rules">> := Rules}
}) ->
    case ensure_rules_len(Rules) of
        ok ->
            emqx_authz_mnesia:store_rules(Namespace, {clientid, ClientId}, Rules),
            {204};
        {error, too_many_rules} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    binfmt(
                        <<"The rules length exceeds the maximum limit.">>,
                        []
                    )
            }}
    end.

handle_delete_rules(Namespace, #{bindings := #{username := Username}}) ->
    case emqx_authz_mnesia:get_rules(Namespace, {username, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Username Not Found">>}};
        {ok, _Rules} ->
            emqx_authz_mnesia:delete_rules(Namespace, {username, Username}),
            {204}
    end;
handle_delete_rules(Namespace, #{bindings := #{clientid := ClientId}}) ->
    case emqx_authz_mnesia:get_rules(Namespace, {clientid, ClientId}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"ClientID Not Found">>}};
        {ok, _Rules} ->
            emqx_authz_mnesia:delete_rules(Namespace, {clientid, ClientId}),
            {204}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

swagger_with_example({Ref, TypeP}, {_Name, _Type} = Example) ->
    emqx_dashboard_swagger:schema_with_examples(
        case TypeP of
            ?TYPE_REF -> ref(?MODULE, Ref);
            ?TYPE_ARRAY -> array(ref(?MODULE, Ref))
        end,
        rules_example(Example)
    ).

rules_example({ExampleName, ExampleType}) ->
    {Summary, Example} =
        case ExampleName of
            username -> {<<"Username">>, ?USERNAME_RULES_EXAMPLE};
            clientid -> {<<"ClientID">>, ?CLIENTID_RULES_EXAMPLE};
            all -> {<<"All">>, ?ALL_RULES_EXAMPLE}
        end,
    Value =
        case ExampleType of
            ?PAGE_QUERY_EXAMPLE ->
                #{
                    data => [Example],
                    meta => ?META_EXAMPLE
                };
            ?PUT_MAP_EXAMPLE ->
                Example;
            ?POST_ARRAY_EXAMPLE ->
                [Example]
        end,
    #{
        'password_based:built_in_database' => #{
            summary => Summary,
            value => Value
        }
    }.

ensure_rules_len(Rules) ->
    emqx_authz_api_sources:with_source(
        ?AUTHZ_TYPE_BIN,
        fun(#{<<"max_rules">> := MaxLen}) ->
            ensure_rules_len(Rules, MaxLen)
        end
    ).

ensure_rules_len(Rules, MaxLen) ->
    case erlang:length(Rules) =< MaxLen of
        true ->
            ok;
        _ ->
            {error, too_many_rules}
    end.

ensure_rules_is_valid(Namespace, Key, Type, Cfgs) ->
    MaxLen = emqx_authz_api_sources:with_source(
        ?AUTHZ_TYPE_BIN,
        fun(#{<<"max_rules">> := MaxLen}) ->
            MaxLen
        end
    ),
    ensure_rules_is_valid(Namespace, Key, Type, MaxLen, Cfgs).

ensure_rules_is_valid(Namespace, Key, Type, MaxLen, [Cfg | Cfgs]) ->
    #{Key := Id, <<"rules">> := Rules} = Cfg,
    case emqx_authz_mnesia:get_rules(Namespace, {Type, Id}) of
        not_found ->
            case ensure_rules_len(Rules, MaxLen) of
                ok ->
                    ensure_rules_is_valid(Namespace, Key, Type, MaxLen, Cfgs);
                {error, Reason} ->
                    {error, {Id, Reason}}
            end;
        _ ->
            {error, {already_exists, Id}}
    end;
ensure_rules_is_valid(_Namespace, _Key, _Type, _MaxLen, []) ->
    ok.

binfmt(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).

with_namespace(Req, Fn) ->
    case get_namespace(Req) of
        {ok, Namespace} ->
            Fn(Namespace);
        {error, not_authorized} ->
            {403, <<"User not authorized to operate on requested namespace">>}
    end.

get_namespace(#{query_string := QueryString} = Req) ->
    ActorNamespace = emqx_dashboard:get_namespace(Req),
    case maps:get(<<"ns">>, QueryString, ActorNamespace) of
        QSNamespace when QSNamespace /= ActorNamespace andalso ActorNamespace /= ?global_ns ->
            {error, not_authorized};
        QSNamespace ->
            {ok, QSNamespace}
    end.

table(?global_ns) -> ?ACL_TABLE;
table(_Namespace) -> ?ACL_NS_TABLE.
