%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
-module(emqx_gateway_api_authn).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(INTERNAL_ERROR, 'INTERNAL_SERVER_ERROR').

-import(hoconsc, [mk/2, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-import(emqx_gateway_http,
        [ return_http_error/2
        , with_gateway/2
        , with_authn/2
        , checks/2
        ]).

%% minirest/dashbaord_swagger behaviour callbacks
-export([ api_spec/0
        , paths/0
        , schema/1
        ]).

%% http handlers
-export([ authn/2
        , users/2
        , users_insta/2
        , import_users/2
        ]).

%% internal export for emqx_gateway_api_listeners module
-export([schema_authn/0]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [ "/gateway/:name/authentication"
    , "/gateway/:name/authentication/users"
    , "/gateway/:name/authentication/users/:uid"
    , "/gateway/:name/authentication/import_users"
    ].

%%--------------------------------------------------------------------
%% http handlers

authn(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        try
            emqx_gateway_http:authn(GwName)
        of
            Authn -> {200, Authn}
        catch
            error : {config_not_found, _} ->
                {204}
        end
    end);

authn(put, #{bindings := #{name := Name0},
             body := Body}) ->
    with_gateway(Name0, fun(GwName, _) ->
        %% TODO: return the authn instances?
        ok = emqx_gateway_http:update_authn(GwName, Body),
        {204}
    end);

authn(post, #{bindings := #{name := Name0},
              body := Body}) ->
    with_gateway(Name0, fun(GwName, _) ->
        %% TODO: return the authn instances?
        ok = emqx_gateway_http:add_authn(GwName, Body),
        {204}
    end);

authn(delete, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:remove_authn(GwName),
        {204}
    end).

users(get, #{bindings := #{name := Name0}, query_string := Qs}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        emqx_authn_api:list_users(ChainName, AuthId, page_pramas(Qs))
    end);
users(post, #{bindings := #{name := Name0},
              body := Body}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        emqx_authn_api:add_user(ChainName, AuthId, Body)
    end).

users_insta(get, #{bindings := #{name := Name0, uid := UserId}}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        emqx_authn_api:find_user(ChainName, AuthId, UserId)
    end);
users_insta(put, #{bindings := #{name := Name0, uid := UserId},
                   body := Body}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        emqx_authn_api:update_user(ChainName, AuthId, UserId, Body)
    end);
users_insta(delete, #{bindings := #{name := Name0, uid := UserId}}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        emqx_authn_api:delete_user(ChainName, AuthId, UserId)
    end).

import_users(post, #{bindings := #{name := Name0},
                     body := Body}) ->
    with_authn(Name0, fun(_GwName, #{id := AuthId,
                                     chain_name := ChainName}) ->
        case maps:get(<<"filename">>, Body, undefined) of
            undefined ->
                emqx_authn_api:serialize_error({missing_parameter, filename});
            Filename ->
                case emqx_authentication:import_users(
                       ChainName, AuthId, Filename) of
                    ok -> {204};
                    {error, Reason} ->
                        emqx_authn_api:serialize_error(Reason)
                end
        end
    end).

%%--------------------------------------------------------------------
%% Utils

page_pramas(Qs) ->
    maps:with([<<"page">>, <<"limit">>], Qs).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------


schema("/gateway/:name/authentication") ->
    #{ 'operationId' => authn,
       get =>
         #{ description => <<"Get the gateway authentication">>
          , parameters => params_gateway_name_in_path()
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                    <<"Ineternal Server Error">>)
               , 200 => schema_authn()
               , 204 => <<"Authentication does not initiated">>
               }
          },
       put =>
         #{ description => <<"Update authentication for the gateway">>
          , parameters => params_gateway_name_in_path()
          , 'requestBody' => schema_authn()
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                   <<"Ineternal Server Error">>)
               , 204 => <<"Updated">> %% XXX: ??? return the updated object
               }
          },
       post =>
         #{ description => <<"Add authentication for the gateway">>
          , parameters => params_gateway_name_in_path()
          , 'requestBody' => schema_authn()
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                   <<"Ineternal Server Error">>)
               , 204 => <<"Added">>
               }
          },
       delete =>
         #{ description => <<"Remove the gateway authentication">>
          , parameters => params_gateway_name_in_path()
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                   <<"Ineternal Server Error">>)
               , 204 => <<"Deleted">>
              }
          }
     };
schema("/gateway/:name/authentication/users") ->
    #{ 'operationId' => users
     , get =>
         #{ description => <<"Get the users for the authentication">>
          , parameters => params_gateway_name_in_path() ++
                          params_paging_in_qs()
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                   <<"Ineternal Server Error">>)
               , 200 => emqx_dashboard_swagger:schema_with_example(
                          ref(emqx_authn_api, response_user),
                          emqx_authn_api:response_user_examples())
              }
          },
       post =>
         #{ description => <<"Add user for the authentication">>
          , parameters => params_gateway_name_in_path()
          , 'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                               ref(emqx_authn_api, request_user_create),
                               emqx_authn_api:request_user_create_examples())
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                   <<"Ineternal Server Error">>)
               , 201 => emqx_dashboard_swagger:schema_with_example(
                          ref(emqx_authn_api, response_user),
                          emqx_authn_api:response_user_examples())
              }
          }
     };
schema("/gateway/:name/authentication/users/:uid") ->
    #{ 'operationId' => users_insta
      , get =>
          #{ description => <<"Get user info from the gateway "
                              "authentication">>
           , parameters => params_gateway_name_in_path() ++
                           params_userid_in_path()
           , responses =>
               #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
                , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
                , 500 => error_codes([?INTERNAL_ERROR],
                                     <<"Ineternal Server Error">>)
                , 200 => emqx_dashboard_swagger:schema_with_example(
                           ref(emqx_authn_api, response_user),
                           emqx_authn_api:response_user_examples())
                }
           },
        put =>
          #{ description => <<"Update the user info for the gateway "
                              "authentication">>
           , parameters => params_gateway_name_in_path() ++
                           params_userid_in_path()
           , 'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                                ref(emqx_authn_api, request_user_update),
                                emqx_authn_api:request_user_update_examples())
           , responses =>
               #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
                , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
                , 500 => error_codes([?INTERNAL_ERROR],
                                     <<"Ineternal Server Error">>)
                , 200 => emqx_dashboard_swagger:schema_with_example(
                           ref(emqx_authn_api, response_user),
                           emqx_authn_api:response_user_examples())
                }
           },
        delete =>
          #{ description => <<"Delete the user for the gateway "
                              "authentication">>
           , parameters => params_gateway_name_in_path() ++
                           params_userid_in_path()
           , responses =>
               #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
                , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
                , 500 => error_codes([?INTERNAL_ERROR],
                                     <<"Ineternal Server Error">>)
                , 204 => <<"User Deleted">>
                }
           }
     };
schema("/gateway/:name/authentication/import_users") ->
    #{ 'operationId' => import_users
     , post =>
         #{ description => <<"Import users into the gateway authentication">>
          , parameters => params_gateway_name_in_path()
          , 'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                             ref(emqx_authn_api, request_import_users),
                             emqx_authn_api:request_import_users_examples()
                            )
          , responses =>
              #{ 400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
               , 404 => error_codes([?NOT_FOUND], <<"Not Found">>)
               , 500 => error_codes([?INTERNAL_ERROR],
                                     <<"Ineternal Server Error">>)
               %% XXX: Put a hint message into 204 return ?
               , 204 => <<"Imported">>
              }
          }
     }.

%%--------------------------------------------------------------------
%% params defines

params_gateway_name_in_path() ->
    [{name,
      mk(binary(),
         #{ in => path
          , desc => <<"Gateway Name">>
          })}
    ].

params_userid_in_path() ->
    [{uid, mk(binary(),
              #{ in => path
               , desc => <<"User ID">>
               })}
    ].

params_paging_in_qs() ->
    [{page, mk(integer(),
               #{ in => query
                , nullable => true
                , desc => <<"Page Index">>
                })},
     {limit, mk(integer(),
                #{ in => query
                 , nullable => true
                 , desc => <<"Page Limit">>
                 })}
    ].

%%--------------------------------------------------------------------
%% schemas

schema_authn() ->
    emqx_dashboard_swagger:schema_with_examples(
      emqx_authn_schema:authenticator_type(),
      emqx_authn_api:authenticator_examples()
     ).
