%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ft_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    fields/1,
    roots/0
]).

%% API callbacks
-export([
    '/file_transfer/files'/2,
    '/file_transfer/file'/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).

namespace() -> "file_transfer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/file_transfer/files",
        "/file_transfer/file"
    ].

schema("/file_transfer/files") ->
    #{
        'operationId' => '/file_transfer/files',
        get => #{
            tags => [<<"file_transfer">>],
            summary => <<"List all uploaded files">>,
            description => ?DESC("file_list"),
            responses => #{
                200 => <<"Operation success">>,
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    };
schema("/file_transfer/file") ->
    #{
        'operationId' => '/file_transfer/file',
        get => #{
            tags => [<<"file_transfer">>],
            summary => <<"Download a particular file">>,
            description => ?DESC("file_get"),
            parameters => [
                ref(file_node),
                ref(file_clientid),
                ref(file_id)
            ],
            responses => #{
                200 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Not found">>),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    }.

'/file_transfer/files'(get, #{}) ->
    case emqx_ft_storage:ready_transfers() of
        {ok, Transfers} ->
            FormattedTransfers = lists:map(
                fun({Id, Info}) ->
                    #{id => Id, info => format_file_info(Info)}
                end,
                Transfers
            ),
            {200, #{<<"files">> => FormattedTransfers}};
        {error, _} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
    end.

'/file_transfer/file'(get, #{query_string := Query}) ->
    case emqx_ft_storage:get_ready_transfer(Query) of
        {ok, FileData} ->
            {200, #{<<"content-type">> => <<"application/data">>}, FileData};
        {error, enoent} ->
            {404, error_msg('NOT_FOUND', <<"Not found">>)};
        {error, _} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
    end.

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

-spec fields(hocon_schema:name()) -> hocon_schema:fields().
fields(file_node) ->
    Desc = <<"File Node">>,
    Meta = #{
        in => query, desc => Desc, example => <<"emqx@127.0.0.1">>, required => false
    },
    [{node, hoconsc:mk(binary(), Meta)}];
fields(file_clientid) ->
    Desc = <<"File ClientId">>,
    Meta = #{
        in => query, desc => Desc, example => <<"client1">>, required => false
    },
    [{clientid, hoconsc:mk(binary(), Meta)}];
fields(file_id) ->
    Desc = <<"File">>,
    Meta = #{
        in => query, desc => Desc, example => <<"file1">>, required => false
    },
    [{fileid, hoconsc:mk(binary(), Meta)}].

roots() ->
    [
        file_node,
        file_clientid,
        file_id
    ].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

format_file_info(#{path := Path, size := Size, timestamp := Timestamp}) ->
    #{
        path => Path,
        size => Size,
        timestamp => format_datetime(Timestamp)
    }.

format_datetime({{Year, Month, Day}, {Hour, Minute, Second}}) ->
    iolist_to_binary(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w", [
            Year, Month, Day, Hour, Minute, Second
        ])
    ).
