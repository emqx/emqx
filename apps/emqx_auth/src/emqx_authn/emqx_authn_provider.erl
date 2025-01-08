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

-module(emqx_authn_provider).

-type authenticator_id() :: emqx_authn_chains:authenticator_id().
-type config() :: emqx_authn_chains:config().
-type state() :: emqx_authn_chains:state().
-type extra() :: emqx_authn_chains:extra().
-type user_info() :: emqx_authn_chains:user_info().

-callback create(AuthenticatorID, Config) ->
    {ok, State}
    | {error, term()}
when
    AuthenticatorID :: authenticator_id(), Config :: config(), State :: state().

-callback update(Config, State) ->
    {ok, NewState}
    | {error, term()}
when
    Config :: config(), State :: state(), NewState :: state().

-callback authenticate(Credential, State) ->
    ignore
    | {ok, Extra}
    | {ok, Extra, AuthData}
    | {continue, AuthCache}
    | {continue, AuthData, AuthCache}
    | {error, term()}
when
    Credential :: map(),
    State :: state(),
    Extra :: extra(),
    AuthData :: binary(),
    AuthCache :: map().

-callback destroy(State) ->
    ok
when
    State :: state().

-callback import_users({PasswordType, Filename, FileData}, State) ->
    ok
    | {error, term()}
when
    PasswordType :: plain | hash,
    Filename :: prepared_user_list | binary(),
    FileData :: binary(),
    State :: state().

-callback add_user(UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserInfo :: user_info(), State :: state(), User :: user_info().

-callback delete_user(UserID, State) ->
    ok
    | {error, term()}
when
    UserID :: binary(), State :: state().

-callback update_user(UserID, UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserID :: binary(), UserInfo :: map(), State :: state(), User :: user_info().

-callback lookup_user(UserID, UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserID :: binary(), UserInfo :: map(), State :: state(), User :: user_info().

-callback list_users(State) ->
    {ok, Users}
when
    State :: state(), Users :: [user_info()].

-optional_callbacks([
    import_users/2,
    add_user/2,
    delete_user/2,
    update_user/3,
    lookup_user/3,
    list_users/1
]).
