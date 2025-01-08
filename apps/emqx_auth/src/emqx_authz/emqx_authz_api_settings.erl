%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_settings).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([settings/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/authorization/settings"].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/settings") ->
    #{
        'operationId' => settings,
        get =>
            #{
                description => ?DESC(authorization_settings_get),
                responses =>
                    #{200 => ref_authz_schema()}
            },
        put =>
            #{
                description => ?DESC(authorization_settings_put),
                'requestBody' => ref_authz_schema(),
                responses =>
                    #{
                        200 => ref_authz_schema(),
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    }.

ref_authz_schema() ->
    emqx_schema:authz_fields().

settings(get, _Params) ->
    {200, authorization_settings()};
settings(put, #{
    body := #{
        <<"no_match">> := NoMatch,
        <<"deny_action">> := DenyAction,
        <<"cache">> := Cache
    }
}) ->
    {ok, _} = emqx_authz_utils:update_config([authorization, no_match], NoMatch),
    {ok, _} = emqx_authz_utils:update_config(
        [authorization, deny_action], DenyAction
    ),
    {ok, _} = emqx_authz_utils:update_config([authorization, cache], Cache),
    {200, authorization_settings()}.

authorization_settings() ->
    C = maps:remove(<<"sources">>, emqx:get_raw_config([authorization], #{})),
    Schema = emqx_hocon:make_schema(emqx_schema:authz_fields()),
    hocon_tconf:make_serializable(Schema, C, #{}).
