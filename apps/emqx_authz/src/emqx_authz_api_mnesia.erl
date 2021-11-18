%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("typerefl/include/types.hrl").

-define(FORMAT_USERNAME_FUN, {?MODULE, format_by_username}).
-define(FORMAT_CLIENTID_FUN, {?MODULE, format_by_clientid}).

-export([ api_spec/0
        , paths/0
        , schema/1
        , fields/1
        ]).

%% operation funs
-export([ users/2
        , clients/2
        , user/2
        , client/2
        , all/2
        , purge/2
        ]).

-export([ format_by_username/1
        , format_by_clientid/1]).

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
    [ "/authorization/sources/built-in-database/username"
    , "/authorization/sources/built-in-database/clientid"
    , "/authorization/sources/built-in-database/username/:username"
    , "/authorization/sources/built-in-database/clientid/:clientid"
    , "/authorization/sources/built-in-database/all"
    , "/authorization/sources/built-in-database/purge-all"].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources/built-in-database/username") ->
    #{
        'operationId' => users,
        get => #{
            tags => [<<"authorization">>],
            description => <<"Show the list of record for username">>,
            parameters => [ hoconsc:ref(emqx_dashboard_swagger, page)
                          , hoconsc:ref(emqx_dashboard_swagger, limit)],
            responses => #{
                200 => swagger_with_example( {username_response_data, ?TYPE_REF}
                                           , {username, ?PAGE_QUERY_EXAMPLE})
            }
        },
        post => #{
            tags => [<<"authorization">>],
            description => <<"Add new records for username">>,
            'requestBody' => swagger_with_example( {rules_for_username, ?TYPE_ARRAY}
                                                 , {username, ?POST_ARRAY_EXAMPLE}),
            responses => #{
                204 => <<"Created">>,
                400 => emqx_dashboard_swagger:error_codes( [?BAD_REQUEST]
                                                         , <<"Bad username or bad rule schema">>)
            }
        }
    };
schema("/authorization/sources/built-in-database/clientid") ->
    #{
        'operationId' => clients,
        get => #{
            tags => [<<"authorization">>],
            description => <<"Show the list of record for clientid">>,
            parameters => [ hoconsc:ref(emqx_dashboard_swagger, page)
                          , hoconsc:ref(emqx_dashboard_swagger, limit)],
            responses => #{
                200 => swagger_with_example( {clientid_response_data, ?TYPE_REF}
                                           , {clientid, ?PAGE_QUERY_EXAMPLE})
            }
        },
        post => #{
            tags => [<<"authorization">>],
            description => <<"Add new records for clientid">>,
            'requestBody' => swagger_with_example( {rules_for_clientid, ?TYPE_ARRAY}
                                                 , {clientid, ?POST_ARRAY_EXAMPLE}),
            responses => #{
                204 => <<"Created">>,
                400 => emqx_dashboard_swagger:error_codes( [?BAD_REQUEST]
                                                         , <<"Bad clientid or bad rule schema">>)
            }
        }
    };
schema("/authorization/sources/built-in-database/username/:username") ->
    #{
        'operationId' => user,
        get => #{
            tags => [<<"authorization">>],
            description => <<"Get record info for username">>,
            parameters => [hoconsc:ref(username)],
            responses => #{
                200 => swagger_with_example( {rules_for_username, ?TYPE_REF}
                                           , {username, ?PUT_MAP_EXAMPLE}),
                404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authorization">>],
            description => <<"Set record for username">>,
            parameters => [hoconsc:ref(username)],
            'requestBody' => swagger_with_example( {rules_for_username, ?TYPE_REF}
                                                 , {username, ?PUT_MAP_EXAMPLE}),
            responses => #{
                204 => <<"Updated">>,
                400 => emqx_dashboard_swagger:error_codes( [?BAD_REQUEST]
                                                         , <<"Bad username or bad rule schema">>)
            }
        },
        delete => #{
            tags => [<<"authorization">>],
            description => <<"Delete one record for username">>,
            parameters => [hoconsc:ref(username)],
            responses => #{
                204 => <<"Deleted">>,
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad username">>)
            }
        }
    };
schema("/authorization/sources/built-in-database/clientid/:clientid") ->
    #{
        'operationId' => client,
        get => #{
            tags => [<<"authorization">>],
            description => <<"Get record info for clientid">>,
            parameters => [hoconsc:ref(clientid)],
            responses => #{
                200 => swagger_with_example( {rules_for_clientid, ?TYPE_REF}
                                           , {clientid, ?PUT_MAP_EXAMPLE}),
                404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authorization">>],
            description => <<"Set record for clientid">>,
            parameters => [hoconsc:ref(clientid)],
            'requestBody' => swagger_with_example( {rules_for_clientid, ?TYPE_REF}
                                                 , {clientid, ?PUT_MAP_EXAMPLE}),
            responses => #{
                204 => <<"Updated">>,
                400 => emqx_dashboard_swagger:error_codes(
                         [?BAD_REQUEST], <<"Bad clientid or bad rule schema">>)
            }
        },
        delete => #{
            tags => [<<"authorization">>],
            description => <<"Delete one record for clientid">>,
            parameters => [hoconsc:ref(clientid)],
            responses => #{
                204 => <<"Deleted">>,
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad clientid">>)
            }
        }
    };
schema("/authorization/sources/built-in-database/all") ->
    #{
        'operationId' => all,
        get => #{
            tags => [<<"authorization">>],
            description => <<"Show the list of rules for all">>,
            responses => #{
                200 => swagger_with_example({rules_for_all, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE})
            }
        },
        put => #{
            tags => [<<"authorization">>],
            description => <<"Set the list of rules for all">>,
            'requestBody' =>
                swagger_with_example({rules_for_all, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE}),
            responses => #{
                204 => <<"Created">>,
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad rule schema">>)
            }
        }
    };
schema("/authorization/sources/built-in-database/purge-all") ->
    #{
        'operationId' => purge,
        delete => #{
            tags => [<<"authorization">>],
            description => <<"Purge all records">>,
            responses => #{
                204 => <<"Deleted">>,
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
            }
        }
    }.

fields(rule_item) ->
    [ {topic,      hoconsc:mk( string()
                             , #{ required => true
                                , desc => <<"Rule on specific topic">>
                                , example => <<"test/topic/1">>})}
    , {permission, hoconsc:mk( hoconsc:enum([allow, deny])
                             , #{desc => <<"Permission">>, required => true, example => allow})}
    , {action,     hoconsc:mk( hoconsc:enum([publish, subscribe, all])
                             , #{ required => true, example => publish
                                , desc => <<"Authorized action">> })} ];
fields(clientid) ->
    [ {clientid, hoconsc:mk( binary()
                           , #{ in => path, required => true
                              , desc => <<"ClientID">>, example => <<"client1">>})}
    ];
fields(username) ->
    [ {username, hoconsc:mk( binary()
                           , #{ in => path, required => true
                              , desc => <<"Username">>, example => <<"user1">>})}
    ];
fields(rules_for_username) ->
    [ {rules, hoconsc:mk(hoconsc:array(hoconsc:ref(rule_item)), #{})}
    ] ++ fields(username);
fields(username_response_data) ->
    [ {data, hoconsc:mk(hoconsc:array(hoconsc:ref(rules_for_username)), #{})}
    , {meta, hoconsc:ref(meta)}
    ];
fields(rules_for_clientid) ->
    [ {rules, hoconsc:mk(hoconsc:array(hoconsc:ref(rule_item)), #{})}
    ] ++ fields(clientid);
fields(clientid_response_data) ->
    [ {data, hoconsc:mk(hoconsc:array(hoconsc:ref(rules_for_clientid)), #{})}
    , {meta, hoconsc:ref(meta)}
    ];
fields(rules_for_all) ->
    [ {rules, hoconsc:mk(hoconsc:array(hoconsc:ref(rule_item)), #{})}
    ];
fields(meta) ->
    emqx_dashboard_swagger:fields(page)
        ++ emqx_dashboard_swagger:fields(limit)
        ++ [{count, hoconsc:mk(integer(), #{example => 1})}].

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

users(get, #{query_string := PageParams}) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}, Rules}) ->
                          [{username, Username}, {rules, Rules}]
                  end),
    {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, PageParams, ?FORMAT_USERNAME_FUN)};
users(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
                      mria:dirty_write(#emqx_acl{
                                          who = {?ACL_TABLE_USERNAME, Username},
                                          rules = format_rules(Rules)
                                         })
                  end, Body),
    {204}.

clients(get, #{query_string := PageParams}) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}, Rules}) ->
                          [{clientid, Clientid}, {rules, Rules}]
                  end),
    {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, PageParams, ?FORMAT_CLIENTID_FUN)};
clients(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"clientid">> := Clientid, <<"rules">> := Rules}) ->
                      mria:dirty_write(#emqx_acl{
                                          who = {?ACL_TABLE_CLIENTID, Clientid},
                                          rules = format_rules(Rules)
                                         })
                  end, Body),
    {204}.

user(get, #{bindings := #{username := Username}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_USERNAME, Username}, rules = Rules}] ->
            {200, #{username => Username,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
user(put, #{bindings := #{username := Username},
              body := #{<<"username">> := Username, <<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = {?ACL_TABLE_USERNAME, Username},
                        rules = format_rules(Rules)
                       }),
    {204};
user(delete, #{bindings := #{username := Username}}) ->
    mria:dirty_delete({?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}}),
    {204}.

client(get, #{bindings := #{clientid := Clientid}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_CLIENTID, Clientid}, rules = Rules}] ->
            {200, #{clientid => Clientid,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
client(put, #{bindings := #{clientid := Clientid},
              body := #{<<"clientid">> := Clientid, <<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = {?ACL_TABLE_CLIENTID, Clientid},
                        rules = format_rules(Rules)
                       }),
    {204};
client(delete, #{bindings := #{clientid := Clientid}}) ->
    mria:dirty_delete({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}}),
    {204}.

all(get, _) ->
    case mnesia:dirty_read(?ACL_TABLE, ?ACL_TABLE_ALL) of
        [] ->
            {200, #{rules => []}};
        [#emqx_acl{who = ?ACL_TABLE_ALL, rules = Rules}] ->
            {200, #{rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
all(put, #{body := #{<<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = ?ACL_TABLE_ALL,
                        rules = format_rules(Rules)
                       }),
    {204}.

purge(delete, _) ->
    case emqx_authz_api_sources:get_raw_source(<<"built-in-database">>) of
        [#{<<"enable">> := false}] ->
            ok = lists:foreach(fun(Key) ->
                                   ok = mria:dirty_delete(?ACL_TABLE, Key)
                               end, mnesia:dirty_all_keys(?ACL_TABLE)),
            {204};
        [#{<<"enable">> := true}] ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message =>
                        <<"'built-in-database' type source must be disabled before purge.">>}};
        [] ->
            {404, #{code => <<"BAD_REQUEST">>,
                    message => <<"'built-in-database' type source is not found.">>
                   }}
    end.

format_rules(Rules) when is_list(Rules) ->
    lists:foldl(fun(#{<<"topic">> := Topic,
                      <<"action">> := Action,
                      <<"permission">> := Permission
                     }, AccIn) when ?PUBSUB(Action)
                            andalso ?ALLOW_DENY(Permission) ->
                   AccIn ++ [{ atom(Permission), atom(Action), Topic }]
                end, [], Rules).

format_by_username([{username, Username}, {rules, Rules}]) ->
    #{username => Username,
      rules => [ #{topic => Topic,
                   action => Action,
                   permission => Permission
                  } || {Permission, Action, Topic} <- Rules]
     }.
format_by_clientid([{clientid, Clientid}, {rules, Rules}]) ->
    #{clientid => Clientid,
      rules => [ #{topic => Topic,
                   action => Action,
                   permission => Permission
                  } || {Permission, Action, Topic} <- Rules]
     }.
atom(B) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
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
          ?TYPE_REF -> hoconsc:ref(?MODULE, Ref);
          ?TYPE_ARRAY -> hoconsc:array(hoconsc:ref(?MODULE, Ref))
      end,
      rules_example(Example)).

rules_example({ExampleName, ExampleType}) ->
    {Summary, Example} =
        case ExampleName of
            username -> {<<"Username">>, ?USERNAME_RULES_EXAMPLE};
            clientid -> {<<"ClientID">>, ?CLIENTID_RULES_EXAMPLE};
            all      -> {<<"All">>,      ?ALL_RULES_EXAMPLE}
        end,
    Value =
        case ExampleType of
            ?PAGE_QUERY_EXAMPLE -> #{
                data => [Example],
                meta => ?META_EXAMPLE
            };
            ?PUT_MAP_EXAMPLE ->
                Example;
            ?POST_ARRAY_EXAMPLE ->
                [Example]
        end,
    #{
        'password-based:built-in-database' => #{
            summary => Summary,
            value   => Value
        }
    }.
