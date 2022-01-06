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

-module(emqx_authz_api_settings).

-behaviour(minirest_api).

-export([ api_spec/0
        , settings/2
        ]).

api_spec() ->
    {[settings_api()], []}.

authorization_settings() ->
    maps:remove(<<"sources">>, emqx:get_raw_config([authorization], #{})).

conf_schema() ->
    emqx_mgmt_api_configs:gen_schema(authorization_settings()).

settings_api() ->
    Metadata = #{
        get => #{
            description => "Get authorization settings",
            responses => #{<<"200">> => emqx_mgmt_util:schema(conf_schema())}
        },
        put => #{
            description => "Update authorization settings",
            requestBody => emqx_mgmt_util:schema(conf_schema()),
            responses => #{
                <<"200">> => emqx_mgmt_util:schema(conf_schema()),
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        }
    },
    {"/authorization/settings", Metadata, settings}.

settings(get, _Params) ->
    {200, authorization_settings()};

settings(put, #{body := #{<<"no_match">> := NoMatch,
                          <<"deny_action">> := DenyAction,
                          <<"cache">> := Cache}}) ->
    {ok, _} = emqx_authz_utils:update_config([authorization, no_match], NoMatch),
    {ok, _} = emqx_authz_utils:update_config(
                [authorization, deny_action], DenyAction),
    {ok, _} = emqx_authz_utils:update_config([authorization, cache], Cache),
    ok = emqx_authz_cache:drain_cache(),
    {200, authorization_settings()}.
