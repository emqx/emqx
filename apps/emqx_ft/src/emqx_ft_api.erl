%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_ft_api.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    roots/0,
    fields/1
]).

%% Minirest filter for checking if file transfer is enabled
-export([check_ft_enabled/2]).

%% API callbacks
-export([
    '/file_transfer/files'/2,
    '/file_transfer/files/:clientid/:fileid'/2,
    '/file_transfer'/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).

-define(SCHEMA_CONFIG, ref(emqx_ft_schema, file_transfer)).

namespace() -> "file_transfer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/file_transfer/files",
        "/file_transfer/files/:clientid/:fileid",
        "/file_transfer"
    ].

schema("/file_transfer/files") ->
    #{
        'operationId' => '/file_transfer/files',
        filter => fun ?MODULE:check_ft_enabled/2,
        get => #{
            tags => ?TAGS,
            summary => <<"List all uploaded files">>,
            description => ?DESC("file_list"),
            parameters => [
                ref(following),
                ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => <<"Operation success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], <<"Invalid cursor">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], error_desc('SERVICE_UNAVAILABLE')
                )
            }
        }
    };
schema("/file_transfer/files/:clientid/:fileid") ->
    #{
        'operationId' => '/file_transfer/files/:clientid/:fileid',
        filter => fun ?MODULE:check_ft_enabled/2,
        get => #{
            tags => ?TAGS,
            summary => <<"List files uploaded in a specific transfer">>,
            description => ?DESC("file_list_transfer"),
            parameters => [
                ref(client_id),
                ref(file_id)
            ],
            responses => #{
                200 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['FILES_NOT_FOUND'], error_desc('FILES_NOT_FOUND')
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], error_desc('SERVICE_UNAVAILABLE')
                )
            }
        }
    };
schema("/file_transfer") ->
    #{
        'operationId' => '/file_transfer',
        get => #{
            tags => ?TAGS,
            summary => <<"Get current File Transfer configuration">>,
            description => ?DESC("file_transfer_get_config"),
            responses => #{
                200 => ?SCHEMA_CONFIG
            }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update File Transfer configuration">>,
            description => ?DESC("file_transfer_update_config"),
            'requestBody' => ?SCHEMA_CONFIG,
            responses => #{
                200 => ?SCHEMA_CONFIG,
                400 => emqx_dashboard_swagger:error_codes(
                    ['UPDATE_FAILED', 'INVALID_CONFIG'],
                    error_desc('INVALID_CONFIG')
                )
            }
        }
    }.

check_ft_enabled(Params, _Meta) ->
    case emqx_ft_conf:enabled() of
        true ->
            {ok, Params};
        false ->
            {503, error_msg('SERVICE_UNAVAILABLE')}
    end.

'/file_transfer/files'(get, #{
    query_string := QueryString
}) ->
    try
        Limit = limit(QueryString),
        Query =
            case maps:get(<<"following">>, QueryString, undefined) of
                undefined ->
                    #{limit => Limit};
                Cursor ->
                    #{limit => Limit, following => Cursor}
            end,
        case emqx_ft_storage:files(Query) of
            {ok, Page} ->
                {200, format_page(Page)};
            {error, _} ->
                {503, error_msg('SERVICE_UNAVAILABLE')}
        end
    catch
        error:{badarg, cursor} ->
            {400, error_msg('BAD_REQUEST', <<"Invalid cursor">>)}
    end.

'/file_transfer/files/:clientid/:fileid'(get, #{
    bindings := #{clientid := ClientId, fileid := FileId}
}) ->
    Transfer = {ClientId, FileId},
    case emqx_ft_storage:files(#{transfer => Transfer}) of
        {ok, Page} ->
            {200, format_page(Page)};
        {error, [{_Node, enoent} | _]} ->
            {404, error_msg('FILES_NOT_FOUND')};
        {error, _} ->
            {503, error_msg('SERVICE_UNAVAILABLE')}
    end.

%% Forward /file_transfer to /configs/file_transfer
'/file_transfer'(Method, Data) ->
    emqx_mgmt_api_configs:request_config([<<"file_transfer">>], Method, Data).

format_page(#{items := Files, cursor := Cursor}) ->
    #{
        <<"files">> => lists:map(fun format_file_info/1, Files),
        <<"cursor">> => Cursor
    };
format_page(#{items := Files}) ->
    #{
        <<"files">> => lists:map(fun format_file_info/1, Files)
    }.

error_msg(Code) ->
    #{code => Code, message => error_desc(Code)}.

error_msg(Code, Msg) ->
    #{code => Code, message => Msg}.

error_desc('FILES_NOT_FOUND') ->
    <<"Files requested for this transfer could not be found">>;
error_desc('INVALID_CONFIG') ->
    <<"Provided configuration is invalid">>;
error_desc('SERVICE_UNAVAILABLE') ->
    <<"Service unavailable">>.

roots() ->
    [].

-spec fields(hocon_schema:name()) -> [hocon_schema:field()].
fields(client_id) ->
    [
        {clientid,
            mk(binary(), #{
                in => path,
                desc => <<"MQTT Client ID">>,
                required => true
            })}
    ];
fields(file_id) ->
    [
        {fileid,
            mk(binary(), #{
                in => path,
                desc => <<"File ID">>,
                required => true
            })}
    ];
fields(following) ->
    [
        {following,
            mk(binary(), #{
                in => query,
                desc => <<"Cursor to start listing files from">>,
                required => false
            })}
    ].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

format_file_info(
    Info = #{
        name := Name,
        size := Size,
        uri := URI,
        timestamp := Timestamp,
        transfer := {ClientId, FileId}
    }
) ->
    Res = #{
        name => format_name(Name),
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
    emqx_utils_calendar:epoch_to_rfc3339(Timestamp, second).

format_name(NameBin) when is_binary(NameBin) ->
    NameBin;
format_name(Name) when is_list(Name) ->
    iolist_to_binary(Name).

limit(QueryString) ->
    maps:get(<<"limit">>, QueryString, emqx_mgmt:default_row_limit()).
