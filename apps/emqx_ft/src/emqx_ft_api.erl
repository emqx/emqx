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
    roots/0
]).

%% API callbacks
-export([
    '/file_transfer/files'/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).

namespace() -> "file_transfer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/file_transfer/files"
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
    }.

'/file_transfer/files'(get, #{}) ->
    case emqx_ft_storage:exports() of
        {ok, Transfers} ->
            {200, #{<<"files">> => lists:map(fun format_export_info/1, Transfers)}};
        {error, _} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
    end.

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

roots() ->
    [].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

format_export_info(
    Info = #{
        name := Name,
        size := Size,
        uri := URI,
        timestamp := Timestamp,
        transfer := {ClientId, FileId}
    }
) ->
    Res = #{
        name => iolist_to_binary(Name),
        size => Size,
        timestamp => format_timestamp(Timestamp),
        clientid => ClientId,
        fileid => FileId,
        uri => iolist_to_binary(URI)
    },
    case Info of
        #{meta := Meta} ->
            Res#{metadata => emqx_ft:encode_filemeta(Meta)};
        #{} ->
            Res
    end.

format_timestamp(Timestamp) ->
    iolist_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, second}])).
