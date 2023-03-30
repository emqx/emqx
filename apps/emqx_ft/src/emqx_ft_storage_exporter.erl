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

%% Filesystem storage exporter
%%
%% This is conceptually a part of the Filesystem storage backend that defines
%% how and where complete tranfers are assembled into files and stored.

-module(emqx_ft_storage_exporter).

%% Export API
-export([start_export/3]).
-export([write/2]).
-export([complete/1]).
-export([discard/1]).

%% Listing API
-export([list/1]).
% TODO
% -export([list/2]).

-export([exporter/1]).

-export_type([export/0]).

-type storage() :: emxt_ft_storage_fs:storage().
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().

-type exporter_conf() :: map().
-type export_st() :: term().
-opaque export() :: {module(), export_st()}.

-callback start_export(exporter_conf(), transfer(), filemeta()) ->
    {ok, export_st()} | {error, _Reason}.

%% Exprter must discard the export itself in case of error
-callback write(ExportSt :: export_st(), iodata()) ->
    {ok, ExportSt :: export_st()} | {error, _Reason}.

-callback complete(ExportSt :: export_st()) ->
    ok | {error, _Reason}.

-callback discard(ExportSt :: export_st()) ->
    ok | {error, _Reason}.

-callback list(storage()) ->
    {ok, [emqx_ft_storage:file_info()]} | {error, _Reason}.

%%

-spec start_export(storage(), transfer(), filemeta()) ->
    {ok, export()} | {error, _Reason}.
start_export(Storage, Transfer, Filemeta) ->
    {ExporterMod, ExporterConf} = exporter(Storage),
    case ExporterMod:start_export(ExporterConf, Transfer, Filemeta) of
        {ok, ExportSt} ->
            {ok, {ExporterMod, ExportSt}};
        {error, _} = Error ->
            Error
    end.

-spec write(export(), iodata()) ->
    {ok, export()} | {error, _Reason}.
write({ExporterMod, ExportSt}, Content) ->
    case ExporterMod:write(ExportSt, Content) of
        {ok, ExportStNext} ->
            {ok, {ExporterMod, ExportStNext}};
        {error, _} = Error ->
            Error
    end.

-spec complete(export()) ->
    ok | {error, _Reason}.
complete({ExporterMod, ExportSt}) ->
    ExporterMod:complete(ExportSt).

-spec discard(export()) ->
    ok | {error, _Reason}.
discard({ExporterMod, ExportSt}) ->
    ExporterMod:discard(ExportSt).

-spec list(storage()) ->
    {ok, [emqx_ft_storage:file_info()]} | {error, _Reason}.
list(Storage) ->
    {ExporterMod, ExporterOpts} = exporter(Storage),
    ExporterMod:list(ExporterOpts).

-spec exporter(storage()) -> {module(), _ExporterOptions}.
exporter(Storage) ->
    case maps:get(exporter, Storage) of
        #{type := local} = Options ->
            {emqx_ft_storage_exporter_fs, without_type(Options)};
        #{type := s3} = Options ->
            {emqx_ft_storage_exporter_s3, without_type(Options)}
    end.

-spec without_type(exporter_conf()) -> exporter_conf().
without_type(#{type := _} = Options) ->
    maps:without([type], Options).
