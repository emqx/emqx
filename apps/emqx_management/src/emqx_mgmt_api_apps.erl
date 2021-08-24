%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(minirest_api).

-import(emqx_mgmt_util, [ schema/1
                        , schema/2
                        , object_schema/1
                        , object_schema/2
                        , object_array_schema/2
                        , error_schema/1
                        , error_schema/2
                        , properties/1
                        ]).

-export([api_spec/0]).

-export([ apps/2
        , app/2]).

-define(BAD_APP_ID, 'BAD_APP_ID').
-define(APP_ID_NOT_FOUND, <<"{\"code\": \"BAD_APP_ID\", \"reason\": \"App id not found\"}">>).

api_spec() ->
    {
        [apps_api(), app_api()],
        []
    }.

properties() ->
    properties([
        {app_id, string, <<"App ID">>},
        {secret, string, <<"App Secret">>},
        {name, string, <<"Dsiplay name">>},
        {desc, string, <<"App description">>},
        {status, boolean, <<"Enable or disable">>},
        {expired, integer, <<"Expired time">>}
    ]).

%% not export schema
app_without_secret_schema() ->
    maps:without([secret], properties()).

apps_api() ->
    Metadata = #{
        get => #{
            description => <<"List EMQ X apps">>,
            responses => #{
                <<"200">> =>
                    object_array_schema(app_without_secret_schema(), <<"All apps">>)
                }
        },
        post => #{
            description => <<"EMQ X create apps">>,
            'requestBody' => schema(app),
            responses => #{
                <<"200">> =>
                    schema(app_secret, <<"Create apps">>),
                <<"400">> =>
                    error_schema(<<"App ID already exist">>, [?BAD_APP_ID])
            }
        }
    },
    {"/apps", Metadata, apps}.

app_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X apps">>,
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string}}],
            responses => #{
                <<"404">> =>
                    error_schema(<<"App id not found">>),
                <<"200">> =>
                    object_schema(app_without_secret_schema(), <<"Get App">>)}},
        delete => #{
            description => <<"EMQ X apps">>,
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string}
            }],
            responses => #{
                <<"200">> => schema(<<"Remove app ok">>)}},
        put => #{
            description => <<"EMQ X update apps">>,
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string}
            }],
            'requestBody' => object_schema(app_without_secret_schema()),
            responses => #{
                <<"404">> =>
                    error_schema(<<"App id not found">>, [?BAD_APP_ID]),
                <<"200">> =>
                    object_schema(app_without_secret_schema(), <<"Update ok">>)}}},
    {"/apps/:app_id", Metadata, app}.

%%%==============================================================================================
%% parameters trans
apps(get, _Params) ->
    list(#{});

apps(post, #{body := Data}) ->
    Parameters = #{
        app_id  => maps:get(<<"app_id">>, Data),
        name    => maps:get(<<"name">>, Data),
        secret  => maps:get(<<"secret">>, Data),
        desc    => maps:get(<<"desc">>, Data),
        status  => maps:get(<<"status">>, Data),
        expired => maps:get(<<"expired">>, Data, undefined)
    },
    create(Parameters).

app(get, #{bindings := #{app_id := AppID}}) ->
    lookup(#{app_id => AppID});

app(delete, #{bindings := #{app_id := AppID}}) ->
    delete(#{app_id => AppID});

app(put, #{bindings := #{app_id := AppID}, body := Data}) ->
    Parameters = #{
        app_id  => AppID,
        name    => maps:get(<<"name">>, Data),
        desc    => maps:get(<<"desc">>, Data),
        status  => maps:get(<<"status">>, Data),
        expired => maps:get(<<"expired">>, Data, undefined)
    },
    update(Parameters).


%%%==============================================================================================
%% api apply
list(_) ->
    {200, [format_without_app_secret(Apps) || Apps <- emqx_mgmt_auth:list_apps()]}.

create(#{app_id := AppID, name := Name, secret := Secret,
    desc := Desc, status := Status, expired := Expired}) ->
    case emqx_mgmt_auth:add_app(AppID, Name, Secret, Desc, Status, Expired) of
        {ok, AppSecret} ->
            {200, #{secret => AppSecret}};
        {error, alread_existed} ->
            Message = list_to_binary(io_lib:format("appid ~p already existed", [AppID])),
            {400, #{code => 'BAD_APP_ID', message => Message}};
        {error, Reason} ->
            Response = #{code => 'UNKNOW_ERROR',
                message => list_to_binary(io_lib:format("~p", [Reason]))},
            {500, Response}
    end.

lookup(#{app_id := AppID}) ->
    case emqx_mgmt_auth:lookup_app(AppID) of
        undefined ->
            {404, ?APP_ID_NOT_FOUND};
        App ->
            Response = format_with_app_secret(App),
            {200, Response}
    end.

delete(#{app_id := AppID}) ->
    _ = emqx_mgmt_auth:del_app(AppID),
    {200}.

update(App = #{app_id := AppID, name := Name, desc := Desc, status := Status, expired := Expired}) ->
    case emqx_mgmt_auth:update_app(AppID, Name, Desc, Status, Expired) of
        ok ->
            {200, App};
        {error, not_found} ->
            {404, ?APP_ID_NOT_FOUND};
        {error, Reason} ->
            Response = #{code => 'UNKNOW_ERROR', message => list_to_binary(io_lib:format("~p", [Reason]))},
            {500, Response}
    end.

%%%==============================================================================================
%% format
format_without_app_secret(App) ->
    format_without([secret], App).

format_with_app_secret(App) ->
    format_without([], App).

format_without(List, {AppID, AppSecret, Name, Desc, Status, Expired}) ->
    Data = #{
        app_id => AppID,
        secret => AppSecret,
        name => Name,
        desc => Desc,
        status => Status,
        expired => Expired
    },
    maps:without(List, Data).
