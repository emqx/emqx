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

-export([
    mk_file_uri/3
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
    % TODO
    % This is conceptually another API, because this logic is inherent only to the
    % local filesystem exporter. Ideally, we won't even publish it if `emqx_ft` is
    % configured with another exporter. Accordingly, things that look too specific
    % for this module (i.e. `parse_filepath/1`) should go away in another API module.
    #{
        'operationId' => '/file_transfer/file',
        get => #{
            tags => [<<"file_transfer">>],
            summary => <<"Download a particular file">>,
            description => ?DESC("file_get"),
            parameters => [
                ref(file_node),
                ref(file_ref)
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
    case emqx_ft_storage:exports() of
        {ok, Transfers} ->
            {200, #{<<"files">> => lists:map(fun format_export_info/1, Transfers)}};
        {error, _} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
    end.

'/file_transfer/file'(get, #{query_string := Query}) ->
    try
        Node = parse_node(maps:get(<<"node">>, Query)),
        Filepath = parse_filepath(maps:get(<<"fileref">>, Query)),
        case emqx_ft_storage_fs_proto_v1:read_export_file(Node, Filepath, self()) of
            {ok, ReaderPid} ->
                FileData = emqx_ft_storage_fs_reader:table(ReaderPid),
                {200,
                    #{
                        <<"content-type">> => <<"application/data">>,
                        <<"content-disposition">> => <<"attachment">>
                    },
                    FileData};
            {error, enoent} ->
                {404, error_msg('NOT_FOUND', <<"Not found">>)};
            {error, Error} ->
                ?SLOG(warning, #{msg => "get_ready_transfer_fail", error => Error}),
                {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
        end
    catch
        throw:{invalid, Param} ->
            {404,
                error_msg(
                    'NOT_FOUND',
                    iolist_to_binary(["Invalid query parameter: ", Param])
                )};
        error:{erpc, noconnection} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"Service unavailable">>)}
    end.

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

-spec fields(hocon_schema:name()) -> hocon_schema:fields().
fields(file_ref) ->
    [
        {fileref,
            hoconsc:mk(binary(), #{
                in => query,
                desc => <<"File reference">>,
                example => <<"file1">>,
                required => true
            })}
    ];
fields(file_node) ->
    [
        {node,
            hoconsc:mk(binary(), #{
                in => query,
                desc => <<"Node under which the file is located">>,
                example => atom_to_list(node()),
                required => true
            })}
    ].

roots() ->
    [
        file_node,
        file_ref
    ].

mk_file_uri(_Options, Node, Filepath) ->
    % TODO: qualify with `?BASE_PATH`
    [
        "/file_transfer/file?",
        uri_string:compose_query([
            {"node", atom_to_list(Node)},
            {"fileref", Filepath}
        ])
    ].

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

parse_node(NodeBin) ->
    case emqx_misc:safe_to_existing_atom(NodeBin) of
        {ok, Node} ->
            Node;
        {error, _} ->
            throw({invalid, NodeBin})
    end.

parse_filepath(PathBin) ->
    case filename:pathtype(PathBin) of
        relative ->
            ok;
        absolute ->
            throw({invalid, PathBin})
    end,
    PathComponents = filename:split(PathBin),
    case lists:any(fun is_special_component/1, PathComponents) of
        false ->
            filename:join(PathComponents);
        true ->
            throw({invalid, PathBin})
    end.

is_special_component(<<".", _/binary>>) ->
    true;
is_special_component([$. | _]) ->
    true;
is_special_component(_) ->
    false.
