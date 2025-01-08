%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-import(emqx_dashboard_swagger, [error_codes/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

% Swagger
-define(API_TAGS_GLOBAL, [<<"Authentication">>]).
-define(API_TAGS_SINGLE, [<<"Listener Authentication">>]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    import_result_schema/0
]).

-export([
    authenticator_import_users/2,
    listener_authenticator_import_users/2
]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/authentication/:id/import_users"
        %% hide the deprecated api since 5.1.0
        %% "/listeners/:listener_id/authentication/:id/import_users"
    ].

schema("/authentication/:id/import_users") ->
    #{
        'operationId' => authenticator_import_users,
        post => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_import_users_post),
            parameters => [emqx_authn_api:param_auth_id(), param_password_type()],
            'requestBody' => request_body_schema(),
            responses => #{
                200 => import_result_schema(),
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
            deprecated => true,
            description => ?DESC(listeners_listener_id_authentication_id_import_users_post),
            parameters => [
                emqx_authn_api:param_listener_id(),
                emqx_authn_api:param_auth_id(),
                param_password_type()
            ],
            'requestBody' => request_body_schema(),
            responses => #{
                200 => import_result_schema(),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    }.

request_body_schema() ->
    #{content := Content} = emqx_dashboard_swagger:file_schema(filename),
    Content1 =
        Content#{
            <<"application/json">> => #{
                schema => #{
                    type => object,
                    example => [
                        #{
                            <<"user_id">> => <<"user1">>,
                            <<"password">> => <<"password1">>,
                            <<"is_superuser">> => true
                        },
                        #{
                            <<"user_id">> => <<"user2">>,
                            <<"password">> => <<"password2">>,
                            <<"is_superuser">> => false
                        }
                    ]
                }
            }
        },
    #{
        content => Content1,
        description => <<"Import body">>
    }.

import_result_schema() ->
    [
        {total, hoconsc:mk(integer(), #{description => ?DESC(import_result_total)})},
        {success, hoconsc:mk(integer(), #{description => ?DESC(import_result_success)})},
        {override, hoconsc:mk(integer(), #{description => ?DESC(import_result_override)})},
        {skipped, hoconsc:mk(integer(), #{description => ?DESC(import_result_skipped)})},
        {failed, hoconsc:mk(integer(), #{description => ?DESC(import_result_failed)})}
    ].

authenticator_import_users(
    post,
    Req = #{
        bindings := #{id := AuthenticatorID},
        headers := Headers,
        body := Body
    }
) ->
    PasswordType = password_type(Req),
    Result =
        case maps:get(<<"content-type">>, Headers, undefined) of
            <<"application/json", _/binary>> ->
                emqx_authn_chains:import_users(
                    ?GLOBAL, AuthenticatorID, {PasswordType, prepared_user_list, Body}
                );
            _ ->
                case Body of
                    #{<<"filename">> := #{type := _} = File} ->
                        [{Name, Data}] = maps:to_list(maps:without([type], File)),
                        emqx_authn_chains:import_users(
                            ?GLOBAL, AuthenticatorID, {PasswordType, Name, Data}
                        );
                    _ ->
                        {error, {missing_parameter, filename}}
                end
        end,
    case Result of
        {ok, Result1} -> {200, Result1};
        {error, Reason} -> emqx_authn_api:serialize_error(Reason)
    end.

listener_authenticator_import_users(
    post,
    Req = #{
        bindings := #{listener_id := ListenerID, id := AuthenticatorID},
        headers := Headers,
        body := Body
    }
) ->
    PasswordType = password_type(Req),

    DoImport = fun(FileName, FileData) ->
        emqx_authn_api:with_chain(
            ListenerID,
            fun(ChainName) ->
                case
                    emqx_authn_chains:import_users(
                        ChainName, AuthenticatorID, {PasswordType, FileName, FileData}
                    )
                of
                    {ok, Result} -> {200, Result};
                    {error, Reason} -> emqx_authn_api:serialize_error(Reason)
                end
            end
        )
    end,
    case maps:get(<<"content-type">>, Headers, undefined) of
        <<"application/json", _/binary>> ->
            DoImport(prepared_user_list, Body);
        _ ->
            case Body of
                #{<<"filename">> := #{type := _} = File} ->
                    [{Name, Data}] = maps:to_list(maps:without([type], File)),
                    DoImport(Name, Data);
                _ ->
                    emqx_authn_api:serialize_error({missing_parameter, filename})
            end
    end.

%%--------------------------------------------------------------------
%% helpers

param_password_type() ->
    {type,
        hoconsc:mk(
            binary(),
            #{
                in => query,
                enum => [<<"plain">>, <<"hash">>],
                required => true,
                desc => <<
                    "The import file template type, enum with `plain`,"
                    "`hash`"
                >>,
                example => <<"hash">>
            }
        )}.

password_type(_Req = #{query_string := #{<<"type">> := <<"plain">>}}) ->
    plain;
password_type(_Req = #{query_string := #{<<"type">> := <<"hash">>}}) ->
    hash;
password_type(_) ->
    hash.
