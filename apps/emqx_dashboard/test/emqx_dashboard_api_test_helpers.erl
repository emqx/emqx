%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_api_test_helpers).

-export([
    set_default_config/0,
    set_default_config/1,
    request/2,
    request/3,
    request/4,
    multipart_formdata_request/3,
    multipart_formdata_request/4,
    uri/0,
    uri/1
]).

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

set_default_config() ->
    set_default_config(<<"admin">>).

set_default_config(DefaultUsername) ->
    Config = #{
        listeners => #{
            http => #{
                enable => true,
                bind => 18083,
                inet6 => false,
                ipv6_v6only => false,
                max_connections => 512,
                num_acceptors => 4,
                send_timeout => 5000,
                backlog => 512
            }
        },
        default_username => DefaultUsername,
        default_password => <<"public">>,
        i18n_lang => en
    },
    emqx_config:put([dashboard], Config),
    I18nFile = filename:join([
        code:priv_dir(emqx_dashboard),
        "i18n.conf"
    ]),
    application:set_env(emqx_dashboard, i18n_file, I18nFile),
    ok.

request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    request(<<"admin">>, Method, Url, Body).

request(Username, Method, Url, Body) ->
    Request =
        case Body of
            [] when
                Method =:= get orelse Method =:= put orelse
                    Method =:= head orelse Method =:= delete orelse
                    Method =:= trace
            ->
                {Url, [auth_header(Username)]};
            _ ->
                {Url, [auth_header(Username)], "application/json", jsx:encode(Body)}
        end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [E || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

auth_header(Username) ->
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

multipart_formdata_request(Url, Fields, Files) ->
    multipart_formdata_request(Url, <<"admin">>, Fields, Files).

multipart_formdata_request(Url, Username, Fields, Files) ->
    Boundary =
        "------------" ++ integer_to_list(rand:uniform(99999999999999999)) ++
            integer_to_list(erlang:system_time(millisecond)),
    Body = format_multipart_formdata(Boundary, Fields, Files),
    ContentType = lists:concat(["multipart/form-data; boundary=", Boundary]),
    Headers =
        [
            auth_header(Username),
            {"Content-Length", integer_to_list(length(Body))}
        ],
    case httpc:request(post, {Url, Headers, ContentType, Body}, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

format_multipart_formdata(Boundary, Fields, Files) ->
    FieldParts = lists:map(
        fun({FieldName, FieldContent}) ->
            [
                lists:concat(["--", Boundary]),
                lists:concat([
                    "Content-Disposition: form-data; name=\"", atom_to_list(FieldName), "\""
                ]),
                "",
                to_list(FieldContent)
            ]
        end,
        Fields
    ),
    FieldParts2 = lists:append(FieldParts),
    FileParts = lists:map(
        fun({FieldName, FileName, FileContent}) ->
            [
                lists:concat(["--", Boundary]),
                lists:concat([
                    "Content-Disposition: form-data; name=\"",
                    atom_to_list(FieldName),
                    "\"; filename=\"",
                    FileName,
                    "\""
                ]),
                lists:concat(["Content-Type: ", "application/octet-stream"]),
                "",
                to_list(FileContent)
            ]
        end,
        Files
    ),
    FileParts2 = lists:append(FileParts),
    EndingParts = [lists:concat(["--", Boundary, "--"]), ""],
    Parts = lists:append([FieldParts2, FileParts2, EndingParts]),
    string:join(Parts, "\r\n").

to_list(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_list(Str) when is_list(Str) -> Str.
