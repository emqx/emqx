%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia).

-behaviour(minirest_api).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/1, mk/2, ref/1, ref/2, array/1, enum/1]).

-define(QUERY_USERNAME_FUN, {?MODULE, query_username}).
-define(QUERY_CLIENTID_FUN, {?MODULE, query_clientid}).

-define(ACL_USERNAME_QSCHEMA, [{<<"like_username">>, binary}]).
-define(ACL_CLIENTID_QSCHEMA, [{<<"like_clientid">>, binary}]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

%% operation funs
-export([
    users/2,
    clients/2,
    user/2,
    client/2,
    all/2,
    purge/2
]).

%% query funs
-export([
    query_username/4,
    query_clientid/4
]).

-export([format_result/1]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

-define(TYPE_REF, ref).
-define(TYPE_ARRAY, array).
-define(PAGE_QUERY_EXAMPLE, example_in_data).
-define(PUT_MAP_EXAMPLE, in_put_requestBody).
-define(POST_ARRAY_EXAMPLE, in_post_requestBody).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/sources/built_in_database/username",
        "/authorization/sources/built_in_database/clientid",
        "/authorization/sources/built_in_database/username/:username",
        "/authorization/sources/built_in_database/clientid/:clientid",
        "/authorization/sources/built_in_database/all",
        "/authorization/sources/built_in_database/purge-all"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources/built_in_database/username") ->
    #{
        'operationId' => users,
        get =>
            #{
                tags => [<<"authorization">>],
                description => <<"Show the list of record for username">>,
                parameters =>
                    [
                        ref(emqx_dashboard_swagger, page),
                        ref(emqx_dashboard_swagger, limit),
                        {like_username,
                            mk(binary(), #{
                                in => query,
                                required => false,
                                desc => <<"Fuzzy search `username` as substring">>
                            })}
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
                description => <<"Add new records for username">>,
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_ARRAY},
                    {username, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username or bad rule schema">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/clientid") ->
    #{
        'operationId' => clients,
        get =>
            #{
                tags => [<<"authorization">>],
                description => <<"Show the list of record for clientid">>,
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
                                    desc => <<"Fuzzy search `clientid` as substring">>
                                }
                            )}
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
                description => <<"Add new records for clientid">>,
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_ARRAY},
                    {clientid, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid or bad rule schema">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/username/:username") ->
    #{
        'operationId' => user,
        get =>
            #{
                tags => [<<"authorization">>],
                description => <<"Get record info for username">>,
                parameters => [ref(username)],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_username, ?TYPE_REF},
                            {username, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Not Found">>
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => <<"Set record for username">>,
                parameters => [ref(username)],
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_REF},
                    {username, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username or bad rule schema">>
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => <<"Delete one record for username">>,
                parameters => [ref(username)],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Username Not Found">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/clientid/:clientid") ->
    #{
        'operationId' => client,
        get =>
            #{
                tags => [<<"authorization">>],
                description => <<"Get record info for clientid">>,
                parameters => [ref(clientid)],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_clientid, ?TYPE_REF},
                            {clientid, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Not Found">>
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => <<"Set record for clientid">>,
                parameters => [ref(clientid)],
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_REF},
                    {clientid, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid or bad rule schema">>
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => <<"Delete one record for clientid">>,
                parameters => [ref(clientid)],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"ClientID Not Found">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/all") ->
    #{
        'operationId' => all,
        get =>
            #{
                tags => [<<"authorization">>],
                description => <<"Show the list of rules for all">>,
                responses =>
                    #{200 => swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE})}
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => <<
                    "Create/Update the list of rules for all. "
                    "Set a empty list to clean up rules"
                >>,
                'requestBody' =>
                    swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE}),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad rule schema">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/purge-all") ->
    #{
        'operationId' => purge,
        delete =>
            #{
                tags => [<<"authorization">>],
                description => <<"Purge all records">>,
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad Request">>
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
                    desc => <<"Rule on specific topic">>,
                    example => <<"test/topic/1">>
                }
            )},
        {permission,
            mk(
                enum([allow, deny]),
                #{
                    desc => <<"Permission">>,
                    required => true,
                    example => allow
                }
            )},
        {action,
            mk(
                enum([publish, subscribe, all]),
                #{
                    required => true,
                    example => publish,
                    desc => <<"Authorized action">>
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
                    desc => <<"ClientID">>,
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
                    desc => <<"Username">>,
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
        {meta, ref(meta)}
    ];
fields(rules_for_clientid) ->
    fields(rules) ++
        fields(clientid);
fields(clientid_response_data) ->
    [
        {data, mk(array(ref(rules_for_clientid)), #{})},
        {meta, ref(meta)}
    ];
fields(rules) ->
    [{rules, mk(array(ref(rule_item)))}];
fields(meta) ->
    emqx_dashboard_swagger:fields(page) ++
        emqx_dashboard_swagger:fields(limit) ++
        [{count, mk(integer(), #{example => 1})}].

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

users(get, #{query_string := QueryString}) ->
    Response = emqx_mgmt_api:node_query(
        node(),
        QueryString,
        ?ACL_TABLE,
        ?ACL_USERNAME_QSCHEMA,
        ?QUERY_USERNAME_FUN
    ),
    emqx_mgmt_util:generate_response(Response);
users(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(
        fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
            emqx_authz_mnesia:store_rules({username, Username}, format_rules(Rules))
        end,
        Body
    ),
    {204}.

clients(get, #{query_string := QueryString}) ->
    Response = emqx_mgmt_api:node_query(
        node(),
        QueryString,
        ?ACL_TABLE,
        ?ACL_CLIENTID_QSCHEMA,
        ?QUERY_CLIENTID_FUN
    ),
    emqx_mgmt_util:generate_response(Response);
clients(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(
        fun(#{<<"clientid">> := ClientID, <<"rules">> := Rules}) ->
            emqx_authz_mnesia:store_rules({clientid, ClientID}, format_rules(Rules))
        end,
        Body
    ),
    {204}.

user(get, #{bindings := #{username := Username}}) ->
    case emqx_authz_mnesia:get_rules({username, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                username => Username,
                rules => [
                    #{
                        topic => Topic,
                        action => Action,
                        permission => Permission
                    }
                 || {Permission, Action, Topic} <- Rules
                ]
            }}
    end;
user(put, #{
    bindings := #{username := Username},
    body := #{<<"username">> := Username, <<"rules">> := Rules}
}) ->
    emqx_authz_mnesia:store_rules({username, Username}, format_rules(Rules)),
    {204};
user(delete, #{bindings := #{username := Username}}) ->
    case emqx_authz_mnesia:get_rules({username, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Username Not Found">>}};
        {ok, _Rules} ->
            emqx_authz_mnesia:delete_rules({username, Username}),
            {204}
    end.

client(get, #{bindings := #{clientid := ClientID}}) ->
    case emqx_authz_mnesia:get_rules({clientid, ClientID}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                clientid => ClientID,
                rules => [
                    #{
                        topic => Topic,
                        action => Action,
                        permission => Permission
                    }
                 || {Permission, Action, Topic} <- Rules
                ]
            }}
    end;
client(put, #{
    bindings := #{clientid := ClientID},
    body := #{<<"clientid">> := ClientID, <<"rules">> := Rules}
}) ->
    emqx_authz_mnesia:store_rules({clientid, ClientID}, format_rules(Rules)),
    {204};
client(delete, #{bindings := #{clientid := ClientID}}) ->
    case emqx_authz_mnesia:get_rules({clientid, ClientID}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"ClientID Not Found">>}};
        {ok, _Rules} ->
            emqx_authz_mnesia:delete_rules({clientid, ClientID}),
            {204}
    end.

all(get, _) ->
    case emqx_authz_mnesia:get_rules(all) of
        not_found ->
            {200, #{rules => []}};
        {ok, Rules} ->
            {200, #{
                rules => [
                    #{
                        topic => Topic,
                        action => Action,
                        permission => Permission
                    }
                 || {Permission, Action, Topic} <- Rules
                ]
            }}
    end;
all(post, #{body := #{<<"rules">> := Rules}}) ->
    emqx_authz_mnesia:store_rules(all, format_rules(Rules)),
    {204}.

purge(delete, _) ->
    case emqx_authz_api_sources:get_raw_source(<<"built_in_database">>) of
        [#{<<"enable">> := false}] ->
            ok = emqx_authz_mnesia:purge_rules(),
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
    end.

%%--------------------------------------------------------------------
%% Query Functions

query_username(Tab, {_QString, []}, Continuation, Limit) ->
    Ms = emqx_authz_mnesia:list_username_rules(),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_result/1
    );
query_username(Tab, {_QString, FuzzyQString}, Continuation, Limit) ->
    Ms = emqx_authz_mnesia:list_username_rules(),
    FuzzyFilterFun = fuzzy_filter_fun(FuzzyQString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_result/1
    ).

query_clientid(Tab, {_QString, []}, Continuation, Limit) ->
    Ms = emqx_authz_mnesia:list_clientid_rules(),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_result/1
    );
query_clientid(Tab, {_QString, FuzzyQString}, Continuation, Limit) ->
    Ms = emqx_authz_mnesia:list_clientid_rules(),
    FuzzyFilterFun = fuzzy_filter_fun(FuzzyQString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_result/1
    ).

%%--------------------------------------------------------------------
%% Match funcs

%% Fuzzy username funcs
fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

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

%% format rule from api
format_rules(Rules) when is_list(Rules) ->
    lists:foldl(
        fun(
            #{
                <<"topic">> := Topic,
                <<"action">> := Action,
                <<"permission">> := Permission
            },
            AccIn
        ) when
            ?PUBSUB(Action) andalso
                ?ALLOW_DENY(Permission)
        ->
            AccIn ++ [{atom(Permission), atom(Action), Topic}]
        end,
        [],
        Rules
    ).

%% format result from mnesia tab
format_result([{username, Username}, {rules, Rules}]) ->
    #{
        username => Username,
        rules => [
            #{
                topic => Topic,
                action => Action,
                permission => Permission
            }
         || {Permission, Action, Topic} <- Rules
        ]
    };
format_result([{clientid, ClientID}, {rules, Rules}]) ->
    #{
        clientid => ClientID,
        rules => [
            #{
                topic => Topic,
                action => Action,
                permission => Permission
            }
         || {Permission, Action, Topic} <- Rules
        ]
    }.
atom(B) when is_binary(B) ->
    try
        binary_to_existing_atom(B, utf8)
    catch
        _Error:_Expection -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.

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
