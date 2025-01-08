%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    init_suite(Apps, fun set_special_configs/1, #{}).

init_suite(Apps, SetConfigs) when is_function(SetConfigs) ->
    init_suite(Apps, SetConfigs, #{}).

init_suite(Apps, SetConfigs, Opts) ->
    emqx_common_test_helpers:start_apps(
        Apps ++ [emqx_management, emqx_dashboard], SetConfigs, Opts
    ),
    _ = emqx_common_test_http:create_default_app(),
    ok.

end_suite() ->
    end_suite([]).

end_suite(Apps) ->
    emqx_common_test_http:delete_default_app(),
    emqx_common_test_helpers:stop_apps(Apps ++ [emqx_management, emqx_dashboard]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_App) ->
    ok.

-spec emqx_dashboard() -> emqx_cth_suite:appspec().
emqx_dashboard() ->
    emqx_dashboard(
        "dashboard {\n"
        "           listeners.http { enable = true, bind = 18083}, \n"
        "           password_expired_time = \"86400s\"\n"
        "}"
    ).

emqx_dashboard(Config) ->
    {emqx_dashboard, #{
        config => Config,
        before_start => fun() ->
            {ok, _} = emqx_common_test_http:create_default_app()
        end,
        after_start => fun() ->
            true = emqx_dashboard_listener:is_ready(infinity)
        end
    }}.

%% there is no difference between the 'request' and 'request_api'
%% the 'request' is only to be compatible with the 'emqx_dashboard_api_test_helpers:request'
request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    request_api_with_body(Method, Url, Body).

uri(Parts) ->
    emqx_dashboard_api_test_helpers:uri(Parts).

uri(Host, Parts) ->
    emqx_dashboard_api_test_helpers:uri(Host, Parts).

%% compatible_mode will return as same as 'emqx_dashboard_api_test_helpers:request'
request_api_with_body(Method, Url, Body) ->
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    request_api(Method, Url, [], auth_header_(), Body, Opts).

request_api(Method, Url) ->
    request_api(Method, Url, auth_header_()).

request_api(Method, Url, AuthOrHeaders) ->
    request_api(Method, Url, [], AuthOrHeaders, [], #{}).

request_api(Method, Url, QueryParams, AuthOrHeaders) ->
    request_api(Method, Url, QueryParams, AuthOrHeaders, [], #{}).

request_api(Method, Url, QueryParams, AuthOrHeaders, Body) ->
    request_api(Method, Url, QueryParams, AuthOrHeaders, Body, #{}).

request_api(Method, Url, QueryParams, [], Body, Opts) ->
    request_api(Method, Url, QueryParams, auth_header_(), Body, Opts);
request_api(Method, Url, QueryParams, AuthOrHeaders, [], Opts) when
    (Method =:= options) orelse
        (Method =:= get) orelse
        (Method =:= put) orelse
        (Method =:= head) orelse
        (Method =:= delete) orelse
        (Method =:= trace)
->
    NewUrl =
        case QueryParams of
            [] -> Url;
            _ -> Url ++ "?" ++ build_query_string(QueryParams)
        end,
    do_request_api(Method, {NewUrl, build_http_header(AuthOrHeaders)}, Opts);
request_api(Method, Url, QueryParams, AuthOrHeaders, Body0, Opts) when
    (Method =:= post) orelse
        (Method =:= patch) orelse
        (Method =:= put) orelse
        (Method =:= delete)
->
    ContentType = maps:get('content-type', Opts, "application/json"),
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    Body =
        case Body0 of
            {raw, B} -> B;
            _ -> emqx_utils_json:encode(Body0)
        end,
    do_request_api(
        Method,
        {NewUrl, build_http_header(AuthOrHeaders), ContentType, Body},
        maps:remove('content-type', Opts)
    ).

do_request_api(Method, Request, Opts) ->
    ReturnAll = maps:get(return_all, Opts, false),
    CompatibleMode = maps:get(compatible_mode, Opts, false),
    HttpcReqOpts = maps:get(httpc_req_opts, Opts, []),
    ct:pal("~p: ~p~nOpts: ~p", [Method, Request, Opts]),
    case httpc:request(Method, Request, [], HttpcReqOpts) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{_, Code, _}, _Headers, Body}} when CompatibleMode ->
            {ok, Code, Body};
        {ok, {{"HTTP/1.1", Code, _} = Reason, Headers, Body}} when
            Code >= 200 andalso Code =< 299 andalso ReturnAll
        ->
            {ok, {Reason, Headers, Body}};
        {ok, {{"HTTP/1.1", Code, _}, _, Body}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Body};
        {ok, {Reason, Headers, Body}} when ReturnAll ->
            {error, {Reason, Headers, Body}};
        {ok, {Reason, _Headers, _Body}} ->
            {error, Reason}
    end.

simplify_result(Res) ->
    case Res of
        {error, {{_, Status, _}, _, Body}} ->
            {Status, Body};
        {ok, {{_, Status, _}, _, Body}} ->
            {Status, Body}
    end.

auth_header_() ->
    emqx_common_test_http:default_auth_header().

build_query_string(Query = #{}) ->
    build_query_string(maps:to_list(Query));
build_query_string(Query = [{_, _} | _]) ->
    uri_string:compose_query([{emqx_utils_conv:bin(K), V} || {K, V} <- Query]);
build_query_string(QueryString) ->
    unicode:characters_to_list(QueryString).

build_http_header(X) when is_list(X) ->
    X;
build_http_header(X) ->
    [X].

default_server() ->
    ?SERVER.

api_path(Parts) ->
    join_http_path([?SERVER, ?BASE_PATH | Parts]).

api_path(Host, Parts) ->
    join_http_path([Host, ?BASE_PATH | Parts]).

api_path_without_base_path(Parts) ->
    join_http_path([?SERVER | Parts]).

join_http_path([]) ->
    [];
join_http_path([Part | Rest]) ->
    lists:foldl(fun(P, Acc) -> emqx_bridge_http_connector:join_paths(Acc, P) end, Part, Rest).

%% Usage:
%% upload_request(<<"site.com/api/upload">>, <<"path/to/file.png">>,
%% <<"upload">>, <<"image/png">>, [], <<"some-token">>)
%%
%% Usage with RequestData:
%% Payload = [{upload_type, <<"user_picture">>}],
%% PayloadContent = emqx_utils_json:encode(Payload),
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
    Boundary = emqx_utils:rand_id(32),
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
        case AuthorizationToken of
            _ when is_tuple(AuthorizationToken) ->
                AuthorizationToken;
            _ when is_binary(AuthorizationToken) ->
                {"Authorization", "Bearer " ++ binary_to_list(AuthorizationToken)};
            _ ->
                {}
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

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

simple_request(Method, Path, Params) ->
    AuthHeader = auth_header_(),
    simple_request(Method, Path, Params, AuthHeader).

simple_request(Method, Path, Params, AuthHeader) ->
    Opts = #{return_all => true},
    case request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {{_, Status, _}, _Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {Status, Body};
        {error, {{_, Status, _}, _Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {Status, Body}
    end.
