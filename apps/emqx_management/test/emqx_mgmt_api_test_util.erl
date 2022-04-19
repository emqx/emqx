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
-module(emqx_mgmt_api_test_util).
-compile(export_all).
-compile(nowarn_export_all).

-define(SERVER, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

init_suite() ->
    init_suite([]).

init_suite(Apps) ->
    mria:start(),
    application:load(emqx_management),
    emqx_common_test_helpers:start_apps(Apps ++ [emqx_dashboard], fun set_special_configs/1).

end_suite() ->
    end_suite([]).

end_suite(Apps) ->
    application:unload(emqx_management),
    emqx_common_test_helpers:stop_apps(Apps ++ [emqx_dashboard]),
    emqx_config:delete_override_conf_files(),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_App) ->
    ok.

request_api(Method, Url) ->
    request_api(Method, Url, [], auth_header_(), []).

request_api(Method, Url, AuthOrHeaders) ->
    request_api(Method, Url, [], AuthOrHeaders, []).

request_api(Method, Url, QueryParams, AuthOrHeaders) ->
    request_api(Method, Url, QueryParams, AuthOrHeaders, []).

request_api(Method, Url, QueryParams, AuthOrHeaders, []) when
    (Method =:= options) orelse
        (Method =:= get) orelse
        (Method =:= put) orelse
        (Method =:= head) orelse
        (Method =:= delete) orelse
        (Method =:= trace)
->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(Method, {NewUrl, build_http_header(AuthOrHeaders)});
request_api(Method, Url, QueryParams, AuthOrHeaders, Body) when
    (Method =:= post) orelse
        (Method =:= patch) orelse
        (Method =:= put) orelse
        (Method =:= delete)
->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(
        Method,
        {NewUrl, build_http_header(AuthOrHeaders), "application/json", emqx_json:encode(Body)}
    ).

do_request_api(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _} = Error} ->
            ct:pal("error: ~p~n", [Error]),
            {error, Reason}
    end.

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

build_http_header(X) when is_list(X) ->
    X;
build_http_header(X) ->
    [X].

api_path(Parts) ->
    ?SERVER ++ filename:join([?BASE_PATH | Parts]).

%% Usage:
%% upload_request(<<"site.com/api/upload">>, <<"path/to/file.png">>,
%% <<"upload">>, <<"image/png">>, [], <<"some-token">>)
%%
%% Usage with RequestData:
%% Payload = [{upload_type, <<"user_picture">>}],
%% PayloadContent = jsx:encode(Payload),
%% RequestData = [
%%     {<<"payload">>, PayloadContent}
%% ]
%% upload_request(<<"site.com/api/upload">>, <<"path/to/file.png">>,
%% <<"upload">>, <<"image/png">>, RequestData, <<"some-token">>)
-spec upload_request(URL, FilePath, Name, MimeType, RequestData, AuthorizationToken) ->
    {ok, binary()} | {error, list()}
when
    URL :: binary(),
    FilePath :: binary(),
    Name :: binary(),
    MimeType :: binary(),
    RequestData :: list(),
    AuthorizationToken :: binary().
upload_request(URL, FilePath, Name, MimeType, RequestData, AuthorizationToken) ->
    Method = post,
    Filename = filename:basename(FilePath),
    {ok, Data} = file:read_file(FilePath),
    Boundary = emqx_guid:to_base62(emqx_guid:gen()),
    RequestBody = format_multipart_formdata(
        Data,
        RequestData,
        Name,
        [Filename],
        MimeType,
        Boundary
    ),
    ContentType = "multipart/form-data; boundary=" ++ binary_to_list(Boundary),
    ContentLength = integer_to_list(length(binary_to_list(RequestBody))),
    Headers = [
        {"Content-Length", ContentLength},
        case AuthorizationToken =/= undefined of
            true -> {"Authorization", "Bearer " ++ binary_to_list(AuthorizationToken)};
            false -> {}
        end
    ],
    HTTPOptions = [],
    Options = [{body_format, binary}],
    inets:start(),
    httpc:request(Method, {URL, Headers, ContentType, RequestBody}, HTTPOptions, Options).

-spec format_multipart_formdata(Data, Params, Name, FileNames, MimeType, Boundary) ->
    binary()
when
    Data :: binary(),
    Params :: list(),
    Name :: binary(),
    FileNames :: list(),
    MimeType :: binary(),
    Boundary :: binary().
format_multipart_formdata(Data, Params, Name, FileNames, MimeType, Boundary) ->
    StartBoundary = erlang:iolist_to_binary([<<"--">>, Boundary]),
    LineSeparator = <<"\r\n">>,
    WithParams = lists:foldl(
        fun({Key, Value}, Acc) ->
            erlang:iolist_to_binary([
                Acc,
                StartBoundary,
                LineSeparator,
                <<"Content-Disposition: form-data; name=\"">>,
                Key,
                <<"\"">>,
                LineSeparator,
                LineSeparator,
                Value,
                LineSeparator
            ])
        end,
        <<"">>,
        Params
    ),
    WithPaths = lists:foldl(
        fun(FileName, Acc) ->
            erlang:iolist_to_binary([
                Acc,
                StartBoundary,
                LineSeparator,
                <<"Content-Disposition: form-data; name=\"">>,
                Name,
                <<"\"; filename=\"">>,
                FileName,
                <<"\"">>,
                LineSeparator,
                <<"Content-Type: ">>,
                MimeType,
                LineSeparator,
                LineSeparator,
                Data,
                LineSeparator
            ])
        end,
        WithParams,
        FileNames
    ),
    erlang:iolist_to_binary([WithPaths, StartBoundary, <<"--">>, LineSeparator]).
