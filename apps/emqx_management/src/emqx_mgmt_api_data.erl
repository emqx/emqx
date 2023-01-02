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
            minirest:return({ok, File#{filename => unicode:characters_to_binary(filename:basename(Filename))}});
        Return -> minirest:return(Return)
    end.

list_exported(_Bindings, _Params) ->
    List = [rpc:call(Node, ?MODULE, get_list_exported, []) || Node <- ekka_mnesia:running_nodes()],
    NList = lists:map(fun({_, FileInfo}) -> FileInfo end, lists:keysort(1, lists:append(List))),
    minirest:return({ok, NList}).

get_list_exported() ->
    emqx_mgmt_data_backup:list_backup_file().

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
                        true ->
                            N = erlang:binary_to_atom(Node, utf8),
                            case rpc:call(N, ?MODULE, do_import, [Filename]) of
                                {badrpc, Reason} ->
                                    minirest:return({error, Reason});
                                Res ->
                                    minirest:return(Res)
                            end;
                        false -> minirest:return({error, no_existent_node})
                    end
            end
    end.

do_import(Filename) ->
    emqx_mgmt_data_backup:import(Filename, "{}").

download(#{filename := Filename0}, _Params) ->
    Filename = filename_decode(Filename0),
    case emqx_mgmt_data_backup:read_backup_file(Filename) of
        {ok, Res} ->
            {ok, Res};
        {error, Reason} ->
            minirest:return({error, Reason})
    end.

upload(Bindings, Params) ->
    do_upload(Bindings, maps:from_list(Params)).

do_upload(_Bindings, #{<<"filename">> := Filename,
                       <<"file">> := Bin}) ->
    case emqx_mgmt_data_backup:upload_backup_file(Filename, Bin) of
        ok ->
            minirest:return({ok, [{node, node()}]});
        {error, Reason} ->
            minirest:return({error, Reason})
    end;
do_upload(Bindings, Params = #{<<"file">> := _}) ->
    do_upload(Bindings, Params#{<<"filename">> => tmp_filename()});
do_upload(_Bindings, _Params) ->
    minirest:return({error, missing_required_params}).

delete(#{filename := Filename0}, _Params) ->
    Filename = filename_decode(Filename0),
    case emqx_mgmt_data_backup:delete_backup_file(Filename) of
        ok ->
            minirest:return();
        {error, Reason} ->
            minirest:return({error, Reason})
    end.

import_content(Content) ->
    Bin = emqx_json:encode(Content),
    Filename = tmp_filename(),
    case emqx_mgmt_data_backup:upload_backup_file(Filename, Bin) of
        ok ->
            do_import(Filename);
        {error, Reason} ->
            {error, Reason}
    end.

tmp_filename() ->
    Seconds = erlang:system_time(second),
    {{Y, M, D}, {H, MM, S}} = emqx_mgmt_util:datetime(Seconds),
    iolist_to_binary(io_lib:format("emqx-export-~p-~p-~p-~p-~p-~p.json", [Y, M, D, H, MM, S])).

filename_decode(Filename) ->
    uri_string:percent_decode(Filename).
