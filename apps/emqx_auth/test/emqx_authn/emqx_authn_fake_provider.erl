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

-module(emqx_authn_fake_provider).

-behaviour(emqx_authn_provider).

-include("emqx_authn.hrl").

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1,

    add_user/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{} = Config) ->
    UserTab = ets:new(?MODULE, [set, public]),
    {ok, #{users => UserTab, config => Config}}.

update(Config, _State) ->
    create(Config).

authenticate(Credentials, #{users := UserTab} = _State) ->
    CacheKey = cache_key(Credentials),
    IsValid =
        emqx_auth_cache:with_cache(?AUTHN_CACHE, CacheKey, fun() ->
            {cache,
                lists:any(
                    fun(User) -> are_credentials_matching(Credentials, User) end,
                    ets:tab2list(UserTab)
                )}
        end),
    case IsValid of
        true ->
            {ok, #{is_superuser => true}};
        false ->
            {error, bad_username_or_password}
    end.

destroy(#{users := UserTab}) ->
    true = ets:delete(UserTab),
    ok.

add_user(#{user_id := UserId, password := Password} = User, #{users := UserTab} = _State) ->
    true = ets:insert(UserTab, {UserId, Password}),
    {ok, User}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

cache_key(Credentials) ->
    {
        maps:get(clientid, Credentials, undefined),
        maps:get(username, Credentials, undefined),
        maps:get(password, Credentials, undefined)
    }.

are_credentials_matching(#{username := Username, password := Password}, {Username, Password}) ->
    true;
are_credentials_matching(#{clientid := ClientId, password := Password}, {ClientId, Password}) ->
    true;
are_credentials_matching(_Credentials, _User) ->
    false.
