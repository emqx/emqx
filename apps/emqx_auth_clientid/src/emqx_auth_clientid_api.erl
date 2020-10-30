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

-module(emqx_auth_clientid_api).

-include("emqx_auth_clientid.hrl").

-export([ list/2
        , lookup/2
        , add/2
        , update/2
        , delete/2
        ]).

-import(proplists, [get_value/2]).
-import(minirest,  [return/0, return/1]).

-rest_api(#{name   => list_clientid,
            method => 'GET',
            path   => "/auth_clientid",
            func   => list,
            descr  => "List available clientid in the cluster"
           }).

-rest_api(#{name   => lookup_clientid,
            method => 'GET',
            path   => "/auth_clientid/:bin:clientid",
            func   => lookup,
            descr  => "Lookup clientid in the cluster"
           }).

-rest_api(#{name   => add_clientid,
            method => 'POST',
            path   => "/auth_clientid",
            func   => add,
            descr  => "Add clientid in the cluster"
           }).

-rest_api(#{name   => update_clientid,
            method => 'PUT',
            path   => "/auth_clientid/:bin:clientid",
            func   => update,
            descr  => "Update clientid in the cluster"
           }).

-rest_api(#{name   => delete_clientid,
            method => 'DELETE',
            path   => "/auth_clientid/:bin:clientid",
            func   => delete,
            descr  => "Delete clientid in the cluster"
           }).

list(_Bindings, _Params) ->
    return({ok, emqx_auth_clientid:all_clientids()}).

lookup(#{clientid := ClientId}, _Params) ->
    case emqx_auth_clientid:lookup_clientid(ClientId) of
        [] -> return({error, not_found});
        Auth -> return({ok, format(Auth)})
    end.

add(_Bindings, Params) ->
    ClientId = get_value(<<"clientid">>, Params),
    Password = get_value(<<"password">>, Params),
    case validate([clientid, password], [ClientId, Password]) of
        ok ->
            case emqx_auth_clientid:add_clientid(ClientId, Password) of
                ok -> return();
                Error -> return(Error)
            end;
        Error -> return(Error)
    end.

update(#{clientid := ClientId}, Params) ->
    Password = get_value(<<"password">>, Params),
    case validate([password], [Password]) of
        ok ->
            case emqx_auth_clientid:update_password(ClientId, Password) of
                ok -> return();
                Error -> return(Error)
            end;
        Error -> return(Error)
    end.

delete(#{clientid := ClientId}, _) ->
    case emqx_auth_clientid:remove_clientid(ClientId) of
        ok -> return();
        Error -> return(Error)
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

format([{?APP, ClientId, Password}]) ->
    #{clientid => ClientId,
      password => emqx_auth_clientid:unwrap_salt(Password)}.

validate([], []) ->
    ok;
validate([K|Keys], [V|Values]) ->
    case validation(K, V) of
        false -> {error, K};
        true  -> validate(Keys, Values)
    end.

validation(clientid, V) when is_binary(V)
                        andalso byte_size(V) > 0 ->
    true;
validation(password, V) when is_binary(V)
                        andalso byte_size(V) > 0 ->
    true;
validation(_, _) ->
    false.
