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

-behavior(minirest_api).

-export([api_spec/0]).


-export([ apps/2
        , app/2]).

-define(BAD_APP_ID, 'BAD_APP_ID').
-define(APP_ID_NOT_FOUND, <<"{\"code\": \"BAD_APP_ID\", \"reason\": \"App id not found\"}">>).

api_spec() ->
    {
        [apps_api(), app_api()],
        [app_schema(), app_secret_schema()]
    }.

app_schema() ->
     #{app => #{
         type => object,
         properties => app_properties()}}.

app_properties() ->
    #{
        app_id => #{
            type => string,
            description => <<"App ID">>},
        secret => #{
            type => string,
            description => <<"App Secret">>},
        name => #{
            type => string,
            description => <<"Dsiplay name">>},
        desc => #{
            type => string,
            description => <<"App description">>},
        status => #{
            type => boolean,
            description => <<"Enable or disable">>},
        expired => #{
            type => integer,
            description => <<"Expired time">>}
    }.

app_secret_schema() ->
    #{app_secret => #{
        type => object,
        properties => #{
            secret => #{type => string}}}}.

%% not export schema
app_without_secret_schema() ->
    #{
        type => object,
        properties => maps:without([secret], app_properties())
    }.

apps_api() ->
    Metadata = #{
        get => #{
            description => "List EMQ X apps",
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_array_schema(<<"All apps">>,
                        app_without_secret_schema())}},
        post => #{
            description => "EMQ X create apps",
            'requestBody' => emqx_mgmt_util:request_body_schema(<<"app">>),
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<"Create apps">>, <<"app_secret">>),
                <<"400">> =>
                    emqx_mgmt_util:response_error_schema(<<"App ID already exist">>, [?BAD_APP_ID])}}},
    {"/apps", Metadata, apps}.

app_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X apps",
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string},
                example => <<"admin">>}],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:response_error_schema(<<"App id not found">>),
                <<"200">> =>
                    emqx_mgmt_util:response_schema("Get App", app_without_secret_schema())}},
        delete => #{
            description => "EMQ X apps",
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string},
                example => <<"admin">>}],
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema("Remove app ok")}},
        put => #{
            description => "EMQ X update apps",
            parameters => [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string},
                default => <<"admin">>
            }],
            'requestBody' => emqx_mgmt_util:request_body_schema(app_without_secret_schema()),
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:response_error_schema(<<"App id not found">>, [?BAD_APP_ID]),
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<"Update ok">>, app_without_secret_schema())}}},
    {"/apps/:app_id", Metadata, app}.

%%%==============================================================================================
%% parameters trans
apps(get, _Request) ->
    list(#{});

apps(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Data = emqx_json:decode(Body, [return_maps]),
    Parameters = #{
        app_id  => maps:get(<<"app_id">>, Data),
        name    => maps:get(<<"name">>, Data),
        secret  => maps:get(<<"secret">>, Data),
        desc    => maps:get(<<"desc">>, Data),
        status  => maps:get(<<"status">>, Data),
        expired => maps:get(<<"expired">>, Data, undefined)
    },
    create(Parameters).

app(get, Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    lookup(#{app_id => AppID});

app(delete, Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    delete(#{app_id => AppID});

app(put, Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Data = emqx_json:decode(Body, [return_maps]),
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
    Data = [format_without_app_secret(Apps) || Apps <- emqx_mgmt_auth:list_apps()],
    Response = emqx_json:encode(Data),
    {200, Response}.

create(#{app_id := AppID, name := Name, secret := Secret,
    desc := Desc, status := Status, expired := Expired}) ->
    case emqx_mgmt_auth:add_app(AppID, Name, Secret, Desc, Status, Expired) of
        {ok, AppSecret} ->
            Response = emqx_json:encode(#{secret => AppSecret}),
            {200, Response};
        {error, alread_existed} ->
            Message = list_to_binary(io_lib:format("appid ~p already existed", [AppID])),
            {400, #{code => 'BAD_APP_ID', reason => Message}};
        {error, Reason} ->
            Data = #{code => 'UNKNOW_ERROR',
                reason => list_to_binary(io_lib:format("~p", [Reason]))},
            Response = emqx_json:encode(Data),
            {500, Response}
    end.

lookup(#{app_id := AppID}) ->
    case emqx_mgmt_auth:lookup_app(AppID) of
        undefined ->
            {404, ?APP_ID_NOT_FOUND};
        App ->
            Data = format_with_app_secret(App),
            Response = emqx_json:encode(Data),
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
            Data = #{code => 'UNKNOW_ERROR', reason => list_to_binary(io_lib:format("~p", [Reason]))},
            Response = emqx_json:encode(Data),
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
