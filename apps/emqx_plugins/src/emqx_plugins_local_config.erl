%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_local_config).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(MAX_KEEP_BACKUP_CONFIGS, 10).

-export([
    update/2,
    backup_and_update/2,
    copy_default/1
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec update(name_vsn(), map()) -> ok.
update(NameVsn, Config) ->
    HoconBin = hocon_pp:do(Config, #{}),
    Path = emqx_plugins_fs:config_file_path(NameVsn),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, HoconBin).

%% @doc Backup the current config to a file with a timestamp suffix and
%% then save the new config to the config file.
-spec backup_and_update(name_vsn(), map()) -> ok.
backup_and_update(NameVsn, Config) ->
    HoconBin = hocon_pp:do(Config, #{}),
    %% this may fail, but we don't care
    %% e.g. read-only file system
    Path = emqx_plugins_fs:config_file_path(NameVsn),
    _ = filelib:ensure_dir(Path),
    TmpFile = Path ++ ".tmp",
    case file:write_file(TmpFile, HoconBin) of
        ok ->
            backup_and_replace(Path, TmpFile);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_save_plugin_conf_file",
                hint =>
                    "The updated cluster config is not saved on this node, please check the file system.",
                filename => TmpFile,
                reason => Reason
            }),
            %% e.g. read-only, it's not the end of the world
            ok
    end.

-spec copy_default(name_vsn()) -> ok | {error, term()}.
copy_default(NameVsn) ->
    Source = emqx_plugins_fs:default_config_file_path(NameVsn),
    Destination = emqx_plugins_fs:config_file_path(NameVsn),
    case filelib:is_regular(Source) of
        true ->
            copy(NameVsn, Source, Destination);
        false ->
            ?SLOG(warning, #{
                msg => "failed_to_copy_plugin_default_hocon_config",
                source => Source,
                destination => Destination,
                reason => no_source_file
            }),
            {error, no_source_file}
    end.

copy(NameVsn, Source, Destination) ->
    maybe
        ok ?= ensure_destination_absent(Destination),
        ok ?= filelib:ensure_dir(Destination),
        {ok, _} ?= file:copy(Source, Destination),
        ok
    else
        {error, destination_present} ->
            ?SLOG(debug, #{msg => "plugin_config_file_already_existed", name_vsn => NameVsn}),
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_copy_plugin_default_hocon_config",
                source => Source,
                destination => Destination,
                reason => Reason
            }),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_destination_absent(Destination) ->
    case filelib:is_regular(Destination) of
        false ->
            ok;
        true ->
            {error, destination_present}
    end.

backup_and_replace(Path, TmpPath) ->
    Backup = Path ++ "." ++ emqx_utils_calendar:now_time(millisecond) ++ ".bak",
    case file:rename(Path, Backup) of
        ok ->
            ok = file:rename(TmpPath, Path),
            ok = prune_backup_files(Path);
        {error, enoent} ->
            %% not created yet
            ok = file:rename(TmpPath, Path);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_backup_plugin_conf_file",
                filename => Backup,
                reason => Reason
            }),
            ok
    end.

prune_backup_files(Path) ->
    Files0 = filelib:wildcard(Path ++ ".*"),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = lists:filter(fun(F) -> re:run(F, Re) =/= nomatch end, Files0),
    Sorted = lists:reverse(lists:sort(Files)),
    {_Keeps, Deletes} = lists:split(min(?MAX_KEEP_BACKUP_CONFIGS, length(Sorted)), Sorted),
    lists:foreach(
        fun(F) ->
            case file:delete(F) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_delete_backup_plugin_conf_file",
                        filename => F,
                        reason => Reason
                    }),
                    ok
            end
        end,
        Deletes
    ).
