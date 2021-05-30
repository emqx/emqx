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

-module(emqx_mgmt_api_data).

-include_lib("emqx/include/emqx.hrl").

-include_lib("kernel/include/file.hrl").

-include("emqx_mgmt.hrl").

-rest_api(#{name   => export,
            method => 'POST',
            path   => "/data/export",
            func   => export,
            descr  => "Export data"}).

-rest_api(#{name   => list_exported,
            method => 'GET',
            path   => "/data/export",
            func   => list_exported,
            descr  => "List exported file"}).

-rest_api(#{name   => import,
            method => 'POST',
            path   => "/data/import",
            func   => import,
            descr  => "Import data"}).

-rest_api(#{name   => download,
            method => 'GET',
            path   => "/data/file/:filename",
            func   => download,
            descr  => "Download data file to local"}).

-rest_api(#{name   => upload,
            method => 'POST',
            path   => "/data/file",
            func   => upload,
            descr  => "Upload data file from local"}).

-rest_api(#{name   => delete,
            method => 'DELETE',
            path   => "/data/file/:filename",
            func   => delete,
            descr  => "Delete data file"}).

-export([ export/2
        , list_exported/2
        , import/2
        , download/2
        , upload/2
        , delete/2
        ]).

-export([ get_list_exported/0
        , do_import/1
        ]).

export(_Bindings, _Params) ->
    case emqx_mgmt_data_backup:export() of
        {ok, File = #{filename := Filename}} ->
            minirest:return({ok, File#{filename => filename:basename(Filename)}});
        Return -> minirest:return(Return)
    end.

list_exported(_Bindings, _Params) ->
    List = [ rpc:call(Node, ?MODULE, get_list_exported, []) || Node <- ekka_mnesia:running_nodes() ],
    NList = lists:map(fun({_, FileInfo}) -> FileInfo end, lists:keysort(1, lists:append(List))),
    minirest:return({ok, NList}).

get_list_exported() ->
    Dir = emqx:get_env(data_dir),
    {ok, Files} = file:list_dir_all(Dir),
    lists:foldl(
        fun(File, Acc) ->
            case filename:extension(File) =:= ".json" of
                true ->
                    FullFile = filename:join([Dir, File]),
                    case file:read_file_info(FullFile) of
                        {ok, #file_info{size = Size, ctime = CTime = {{Y, M, D}, {H, MM, S}}}} ->
                            CreatedAt = io_lib:format("~p-~p-~p ~p:~p:~p", [Y, M, D, H, MM, S]),
                            Seconds = calendar:datetime_to_gregorian_seconds(CTime),
                            [{Seconds, [{filename, list_to_binary(File)},
                                        {size, Size},
                                        {created_at, list_to_binary(CreatedAt)},
                                        {node, node()}
                                       ]} | Acc];
                        {error, Reason} ->
                            logger:error("Read file info of ~s failed with: ~p", [File, Reason]),
                            Acc
                    end;
                false -> Acc
            end
        end, [], Files).

import(_Bindings, Params) ->
    case proplists:get_value(<<"filename">>, Params) of
        undefined ->
            Result = import_content(Params),
            minirest:return(Result);
        Filename ->
            case proplists:get_value(<<"node">>, Params) of
                undefined ->
                    Result = do_import(Filename),
                    minirest:return(Result);
                Node ->
                    case lists:member(Node,
                          [ erlang:atom_to_binary(N, utf8) || N <- ekka_mnesia:running_nodes() ]
                         ) of
                        true -> minirest:return(rpc:call(erlang:binary_to_atom(Node, utf8), ?MODULE, do_import, [Filename]));
                        false -> minirest:return({error, no_existent_node})
                    end
            end
    end.

do_import(Filename) ->
    FullFilename = fullname(Filename),
    emqx_mgmt_data_backup:import(FullFilename, "{}").

download(#{filename := Filename}, _Params) ->
    FullFilename = fullname(Filename),
    case file:read_file(FullFilename) of
        {ok, Bin} ->
            {ok, #{filename => list_to_binary(Filename),
                   file => Bin}};
        {error, Reason} ->
            minirest:return({error, Reason})
    end.

upload(Bindings, Params) ->
    do_upload(Bindings, maps:from_list(Params)).

do_upload(_Bindings, #{<<"filename">> := Filename,
                       <<"file">> := Bin}) ->
    FullFilename = fullname(Filename),
    case file:write_file(FullFilename, Bin) of
        ok ->
            minirest:return({ok, [{node, node()}]});
        {error, Reason} ->
            minirest:return({error, Reason})
    end;
do_upload(Bindings, Params = #{<<"file">> := _}) ->
    do_upload(Bindings, Params#{<<"filename">> => tmp_filename()});
do_upload(_Bindings, _Params) ->
    minirest:return({error, missing_required_params}).

delete(#{filename := Filename}, _Params) ->
    FullFilename = fullname(Filename),
    case file:delete(FullFilename) of
        ok ->
            minirest:return();
        {error, Reason} ->
            minirest:return({error, Reason})
    end.

import_content(Content) ->
    File = dump_to_tmp_file(Content),
    do_import(File).

dump_to_tmp_file(Content) ->
    Bin = emqx_json:encode(Content),
    Filename = tmp_filename(),
    ok = file:write_file(fullname(Filename), Bin),
    Filename.

fullname(Name) ->
    filename:join(emqx:get_env(data_dir), Name).

tmp_filename() ->
    Seconds = erlang:system_time(second),
    {{Y, M, D}, {H, MM, S}} = emqx_mgmt_util:datetime(Seconds),
    io_lib:format("emqx-export-~p-~p-~p-~p-~p-~p.json", [Y, M, D, H, MM, S]).
