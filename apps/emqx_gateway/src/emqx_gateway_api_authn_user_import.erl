%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_api_authn_user_import).

-behaviour(minirest_api).

-include("emqx_gateway_http.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-import(emqx_dashboard_swagger, [error_codes/2]).
-import(hoconsc, [mk/2, ref/2]).
-import(
    emqx_gateway_http,
    [
        with_authn/2,
        with_listener_authn/3
    ]
).

%% minirest/dashboard_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1
]).

%% http handlers
-export([
    import_users/2,
    import_listener_users/2
]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/gateway/:name/authentication/import_users",
        "/gateway/:name/listeners/:id/authentication/import_users"
    ].

%%--------------------------------------------------------------------
%% http handlers

import_users(post, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        case maps:get(<<"filename">>, Body, undefined) of
            undefined ->
                emqx_authn_api:serialize_error({missing_parameter, filename});
            File ->
                [{FileName, FileData}] = maps:to_list(maps:without([type], File)),
                case
                    emqx_authentication:import_users(
                        ChainName, AuthId, {FileName, FileData}
                    )
                of
                    ok -> {204};
                    {error, Reason} -> emqx_authn_api:serialize_error(Reason)
                end
        end
    end).

import_listener_users(post, #{
    bindings := #{name := Name0, id := Id},
    body := Body
}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            case maps:get(<<"filename">>, Body, undefined) of
                undefined ->
                    emqx_authn_api:serialize_error({missing_parameter, filename});
                File ->
                    [{FileName, FileData}] = maps:to_list(maps:without([type], File)),
                    case
                        emqx_authentication:import_users(
                            ChainName, AuthId, {FileName, FileData}
                        )
                    of
                        ok -> {204};
                        {error, Reason} -> emqx_authn_api:serialize_error(Reason)
                    end
            end
        end
    ).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateway/:name/authentication/import_users") ->
    #{
        'operationId' => import_users,
        post =>
            #{
                desc => ?DESC(emqx_gateway_api_authn, import_users),
                parameters => params_gateway_name_in_path(),
                'requestBody' => #{
                    content => #{
                        'multipart/form-data' => #{
                            schema => #{
                                filename => file
                            }
                        }
                    }
                },
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Imported">>})
            }
    };
schema("/gateway/:name/listeners/:id/authentication/import_users") ->
    #{
        'operationId' => import_listener_users,
        post =>
            #{
                desc => ?DESC(emqx_gateway_api_listeners, import_users),
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => #{
                    content => #{
                        'multipart/form-data' => #{
                            schema => #{
                                filename => file
                            }
                        }
                    }
                },
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Imported">>})
            }
    }.

%%--------------------------------------------------------------------
%% params defines
%%--------------------------------------------------------------------

params_gateway_name_in_path() ->
    [
        {name,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(emqx_gateway_api, gateway_name),
                    example => <<"stomp">>
                }
            )}
    ].

params_listener_id_in_path() ->
    [
        {id,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(emqx_gateway_api_listeners, listener_id),
                    example => <<"stomp:tcp:def">>
                }
            )}
    ].
