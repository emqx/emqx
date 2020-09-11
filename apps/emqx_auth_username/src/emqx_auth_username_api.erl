%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_username_api).

-include("emqx_auth_username.hrl").

-import(proplists, [get_value/2]).

-import(minirest,  [return/0, return/1]).

-rest_api(#{name   => list_username,
            method => 'GET',
            path   => "/auth_username",
            func   => list,
            descr  => "List available username in the cluster"
           }).

-rest_api(#{name   => lookup_username,
            method => 'GET',
            path   => "/auth_username/:bin:username",
            func   => lookup,
            descr  => "Lookup username in the cluster"
           }).

-rest_api(#{name   => add_username,
            method => 'POST',
            path   => "/auth_username",
            func   => add,
            descr  => "Add username in the cluster"
           }).

-rest_api(#{name   => update_username,
            method => 'PUT',
            path   => "/auth_username/:bin:username",
            func   => update,
            descr  => "Update username in the cluster"
           }).

-rest_api(#{name   => delete_username,
            method => 'DELETE',
            path   => "/auth_username/:bin:username",
            func   => delete,
            descr  => "Delete username in the cluster"
           }).

-export([ list/2
        , lookup/2
        , add/2
        , update/2
        , delete/2
        ]).

list(_Bindings, _Params) ->
    return({ok, emqx_auth_username:all_users()}).

lookup(#{username := Username}, _Params) ->
    return({ok, format(emqx_auth_username:lookup_user(Username))}).

add(_Bindings, Params) ->
    Username = get_value(<<"username">>, Params),
    Password = get_value(<<"password">>, Params),
    case validate([username, password], [Username, Password]) of
        ok ->
            case emqx_auth_username:add_user(Username, Password) of
                ok  -> return();
                Err -> return(Err)
            end;
        Err -> return(Err)
    end.

update(#{username := Username}, Params) ->
    Password = get_value(<<"password">>, Params),
    case validate([password], [Password]) of
        ok ->
            case emqx_auth_username:update_password(Username, Password) of
                ok  -> return();
                Err -> return(Err)
            end;
        Err -> return(Err)
    end.

delete(#{username := Username}, _) ->
    ok = emqx_auth_username:remove_user(Username),
    return().

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

format([{?APP, Username, Password}]) ->
    #{username => Username,
      password => emqx_auth_username:unwrap_salt(Password)}.

validate([], []) ->
    ok;
validate([K|Keys], [V|Values]) ->
   case validation(K, V) of
       false -> {error, K};
       true  -> validate(Keys, Values)
   end.

validation(username, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
validation(password, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
validation(_, _) ->
    false.
