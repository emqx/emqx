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

%% API
-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_get/1
        , handle_post/1
        , handle_put/1
        , handle_delete/1]).

-define(APP_ID_NOT_FOUND, <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"App id not found\"}">>).

rest_schema() ->
    [ app_schema()
    , app_without_secret()
    , app_without_id()
    , app_without_id_secret()
    , secret_schema()].

app_schema() ->
    app_without(<<"app">>, []).

app_without_secret() ->
    app_without(<<"app_without_secret">>, [<<"secret">>]).

app_without_id() ->
    app_without(<<"app_without_id">>, [<<"app_id">>]).

app_without_id_secret() ->
    app_without(<<"app_without_id_secret">>, [<<"app_id">>, <<"secret">>]).

app_without(DefinitionName, List) ->
    DefinitionProperties =
        #{
            <<"app_id">>  => #{type => <<"string">>},
            <<"secret">>  => #{type => <<"string">>},
            <<"name">>    => #{type => <<"string">>},
            <<"desc">>    => #{type => <<"string">>},
            <<"status">>  => #{type => <<"boolean">>},
            <<"expired">> => #{type => <<"string">>}
        },
    {DefinitionName, maps:without(List, DefinitionProperties)}.

secret_schema() ->
    DefinitionProperties = #{<<"secret">> => #{type => <<"string">>}},
    {<<"app_secert">>, DefinitionProperties}.

rest_api() ->
    [apps_api(), app_api()].

apps_api() ->
    Metadata = #{
        get =>
            #{tags => ["application"],
            description => "List EMQ X apps",
            operationId => handle_list,
            responses => #{
                <<"200">> => #{
                    content => #{'application/json' => #{schema =>
                        #{type => array,
                          items => cowboy_swagger:schema(<<"app_without_secret">>)}}}}}},
        post =>
            #{tags => ["application"],
            description => "EMQ X create apps",
            operationId => handle_post,
            requestBody => #{
                content => #{'application/json' => #{schema => cowboy_swagger:schema(<<"app">>)}}},
            responses => #{
                <<"200">> => #{description => "Create apps",
                    content => #{
                    'application/json' =>
                    #{schema =>
                        #{type => array, items => cowboy_swagger:schema(<<"app_secert">>)}}}}}}},
    {"/apps", Metadata}.

app_api() ->
    Metadata = #{
        get =>
            #{tags => ["application"],
            description => "EMQ X apps",
            operationId => handle_get,
            parameters =>
            [#{
                name => app_id,
                in => path,
                required => true,
                schema => #{type => string, example => <<"admin">>}
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"App id not found">>),
                <<"200">> => #{content => #{'application/json' =>
                        #{schema => cowboy_swagger:schema(<<"app_without_secret">>)}}}}},
        delete =>
            #{tags => ["application"],
            description => "EMQ X apps",
            operationId => handle_delete,
            parameters =>
                [#{
                    name => app_id,
                    in => path,
                    required => true,
                    schema => #{type => string, example => <<"admin">>}
                }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"App id not found">>),
                <<"200">> => #{description => "Delete app ok"}}},
        put =>
            #{tags => ["application"],
            description => "EMQ X update apps",
            operationId => handle_put,
            parameters =>
                [#{
                    name => app_id,
                    in => path,
                    required => true,
                    schema => #{type => string, example => <<"admin">>}
                }],
            requestBody => #{content => #{'application/json' =>
                #{schema => cowboy_swagger:schema(<<"app_without_id_secret">>)}}},
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"App id not found">>),
                <<"200">> => #{description => "Update apps"}}}},
    {"/apps/:app_id", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(_Request) ->
    list(#{}).

handle_post(Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Data = emqx_json:decode(Body, [return_maps]),
    Parameters = #{
        app_id  => maps:get(<<"app_id">>, Data),
        name    => maps:get(<<"name">>, Data),
        secret  => maps:get(<<"secret">>, Data),
        desc    => maps:get(<<"desc">>, Data),
        status  => maps:get(<<"status">>, Data),
        expired => maps:get(<<"expired">>, Data)
    },
    create(Parameters).

handle_get(Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    lookup(#{app_id => AppID}).

handle_delete(Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    delete(#{app_id => AppID}).

handle_put(Request) ->
    AppID = cowboy_req:binding(app_id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Data = emqx_json:decode(Body, [return_maps]),
    Parameters = #{
        app_id  => AppID,
        name    => maps:get(<<"name">>, Data),
        desc    => maps:get(<<"desc">>, Data),
        status  => maps:get(<<"status">>, Data),
        expired => maps:get(<<"expired">>, Data)
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
        {error, Reason} ->
            Data = #{code => 'UNKNOW_ERROR', reason => list_to_binary(io_lib:format("~p", [Reason]))},
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
    case emqx_mgmt_auth:del_app(AppID) of
        ok ->
            {200};
        {error, Reason} ->
            Data = #{code => 'UNKNOW_ERROR', reason => list_to_binary(io_lib:format("~p", [Reason]))},
            Response = emqx_json:encode(Data),
            {500, Response}
    end.

update(#{app_id := AppID, name := Name, desc := Desc, status := Status, expired := Expired}) ->
    case emqx_mgmt_auth:update_app(AppID, Name, Desc, Status, Expired) of
        ok ->
            {200};
        {error, not_found} ->
            {404, ?APP_ID_NOT_FOUND};
        {error, Reason} ->
            Data = #{code => 'UNKNOW_ERROR', reason => list_to_binary(io_lib:format("~p", [Reason]))},
            Response = emqx_json:encode(Data),
            {500, Response}
    end.

%%%==============================================================================================
%% api apply
format_without_app_secret(App) ->
    format_without([secret], App).

format_with_app_secret(App) ->
    format_without([], App).

format_without(List, {AppID, AppSecret, Name, Desc, Status, Expired}) ->
    Data =
        #{app_id => AppID
        , secret => AppSecret
        , name => Name
        , desc => Desc
        , status => Status
        , expired => Expired},
    maps:without(List, Data).
