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

-module(emqx_mgmt_api_apps).

-include("emqx_mgmt.hrl").

-import(proplists, [get_value/2]).

-import(minirest, [ return/0
                  , return/1
                  ]).

-rest_api(#{name   => add_app,
            method => 'POST',
            path   => "/apps/",
            func   => add_app,
            descr  => "Add Application"}).

-rest_api(#{name   => del_app,
            method => 'DELETE',
            path   => "/apps/:bin:appid",
            func   => del_app,
            descr  => "Delete Application"}).

-rest_api(#{name   => list_apps,
            method => 'GET',
            path   => "/apps/",
            func   => list_apps,
            descr  => "List Applications"}).

-rest_api(#{name   => lookup_app,
            method => 'GET',
            path   => "/apps/:bin:appid",
            func   => lookup_app,
            descr  => "Lookup Application"}).

-rest_api(#{name   => update_app,
            method => 'PUT',
            path   => "/apps/:bin:appid",
            func   => update_app,
            descr  => "Update Application"}).

-export([ add_app/2
        , del_app/2
        , list_apps/2
        , lookup_app/2
        , update_app/2
        ]).

add_app(_Bindings, Params) ->
    AppId = get_value(<<"app_id">>, Params),
    Name = get_value(<<"name">>, Params),
    Secret = get_value(<<"secret">>, Params),
    Desc = get_value(<<"desc">>, Params),
    Status = get_value(<<"status">>, Params),
    Expired = get_value(<<"expired">>, Params),
    case emqx_mgmt_auth:add_app(AppId, Name, Secret, Desc, Status, Expired) of
        {ok, AppSecret} -> return({ok, #{secret => AppSecret}});
        {error, Reason} -> return({error, ?ERROR2, Reason})
    end.

del_app(#{appid := AppId}, _Params) ->
    case emqx_mgmt_auth:del_app(AppId) of
        ok -> return();
        {error, Reason} -> return({error, ?ERROR2, Reason})
    end.

list_apps(_Bindings, _Params) ->
    return({ok, [format(Apps)|| Apps <- emqx_mgmt_auth:list_apps()]}).

lookup_app(#{appid := AppId}, _Params) ->
    case emqx_mgmt_auth:lookup_app(AppId) of
        {AppId, AppSecret, Name, Desc, Status, Expired} ->
            return({ok, #{app_id => AppId,
                          secret => AppSecret,
                          name => Name,
                          desc => Desc,
                          status => Status,
                          expired => Expired}});
        undefined ->
            return({ok, #{}})
    end.

update_app(#{appid := AppId}, Params) ->
    Name = get_value(<<"name">>, Params),
    Desc = get_value(<<"desc">>, Params),
    Status = get_value(<<"status">>, Params),
    Expired = get_value(<<"expired">>, Params),
    case emqx_mgmt_auth:update_app(AppId, Name, Desc, Status, Expired) of
        ok -> return();
        {error, Reason} -> return({error, ?ERROR2, Reason})
    end.

format({AppId, _AppSecret, Name, Desc, Status, Expired}) ->
    [{app_id, AppId}, {name, Name}, {desc, Desc}, {status, Status}, {expired, Expired}].
