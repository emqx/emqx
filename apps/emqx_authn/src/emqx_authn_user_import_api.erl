%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_user_import_api).

-behaviour(minirest_api).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(emqx_dashboard_swagger, [error_codes/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

% Swagger

-define(API_TAGS_GLOBAL, [
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY,
    <<"authentication config(global)">>
]).
-define(API_TAGS_SINGLE, [
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY,
    <<"authentication config(single listener)">>
]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    authenticator_import_users/2,
    listener_authenticator_import_users/2
]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/authentication/:id/import_users",
        "/listeners/:listener_id/authentication/:id/import_users"
    ].

schema("/authentication/:id/import_users") ->
    #{
        'operationId' => authenticator_import_users,
        post => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_import_users_post),
            parameters => [emqx_authn_api:param_auth_id()],
            'requestBody' => #{
                content => #{
                    'multipart/form-data' => #{
                        schema => #{
                            filename => file
                        }
                    }
                }
            },
            responses => #{
                204 => <<"Users imported">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id/import_users") ->
    #{
        'operationId' => listener_authenticator_import_users,
        post => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_import_users_post),
            parameters => [emqx_authn_api:param_listener_id(), emqx_authn_api:param_auth_id()],
            'requestBody' => #{
                content => #{
                    'multipart/form-data' => #{
                        schema => #{
                            filename => file
                        }
                    }
                }
            },
            responses => #{
                204 => <<"Users imported">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    }.

authenticator_import_users(
    post,
    #{
        bindings := #{id := AuthenticatorID},
        body := #{<<"filename">> := #{type := _} = File}
    }
) ->
    [{FileName, FileData}] = maps:to_list(maps:without([type], File)),
    case emqx_authentication:import_users(?GLOBAL, AuthenticatorID, {FileName, FileData}) of
        ok -> {204};
        {error, Reason} -> emqx_authn_api:serialize_error(Reason)
    end;
authenticator_import_users(post, #{bindings := #{id := _}, body := _}) ->
    emqx_authn_api:serialize_error({missing_parameter, filename}).

listener_authenticator_import_users(
    post,
    #{
        bindings := #{listener_id := ListenerID, id := AuthenticatorID},
        body := #{<<"filename">> := #{type := _} = File}
    }
) ->
    [{FileName, FileData}] = maps:to_list(maps:without([type], File)),
    emqx_authn_api:with_chain(
        ListenerID,
        fun(ChainName) ->
            case
                emqx_authentication:import_users(ChainName, AuthenticatorID, {FileName, FileData})
            of
                ok -> {204};
                {error, Reason} -> emqx_authn_api:serialize_error(Reason)
            end
        end
    );
listener_authenticator_import_users(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    emqx_authn_api:serialize_error({missing_parameter, filename}).
