%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(TAGS, [<<"Gateway Authentication">>]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/gateways/:name/authentication/import_users",
        "/gateways/:name/listeners/:id/authentication/import_users"
    ].

%%--------------------------------------------------------------------
%% http handlers

import_users(post, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_authn(
        Name0,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            do_import_users(ChainName, AuthId, Body)
        end
    ).

import_listener_users(post, #{
    bindings := #{name := Name0, id := Id},
    body := Body
}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            do_import_users(ChainName, AuthId, Body)
        end
    ).

do_import_users(ChainName, AuthId, HttpBody) ->
    case maps:get(<<"filename">>, HttpBody, undefined) of
        undefined ->
            emqx_authn_api:serialize_error({missing_parameter, filename});
        File ->
            [{FileName, FileData}] = maps:to_list(maps:without([type], File)),
            case emqx_authn_chains:import_users(ChainName, AuthId, {hash, FileName, FileData}) of
                {ok, Result} -> {200, Result};
                {error, Reason} -> emqx_authn_api:serialize_error(Reason)
            end
    end.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateways/:name/authentication/import_users") ->
    #{
        'operationId' => import_users,
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(emqx_gateway_api_authn, import_users),
                summary => <<"Import users">>,
                parameters => params_gateway_name_in_path(),
                'requestBody' => emqx_dashboard_swagger:file_schema(filename),
                responses =>
                    ?STANDARD_RESP(#{200 => emqx_authn_user_import_api:import_result_schema()})
            }
    };
schema("/gateways/:name/listeners/:id/authentication/import_users") ->
    #{
        'operationId' => import_listener_users,
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(emqx_gateway_api_listeners, import_users),
                summary => <<"Import users">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => emqx_dashboard_swagger:file_schema(filename),
                responses =>
                    ?STANDARD_RESP(#{200 => emqx_authn_user_import_api:import_result_schema()})
            }
    }.

%%--------------------------------------------------------------------
%% params defines
%%--------------------------------------------------------------------

params_gateway_name_in_path() ->
    [
        {name,
            mk(
                hoconsc:enum(emqx_gateway_schema:gateway_names()),
                #{
                    in => path,
                    desc => ?DESC(emqx_gateway_api, gateway_name_in_qs),
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
