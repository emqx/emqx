%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_info).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    read/1,
    read/2
]).

-type t() :: #{
    %% Mandatory fields
    name := binary(),
    rel_vsn := binary(),
    rel_apps := [binary()],
    description := binary(),
    running_status := running | stopped | loaded,
    config_status := enabled | disabled | not_configured,
    %% Optional fields
    with_config_schema => boolean(),
    hidden => boolean(),
    date => binary(),
    md5sum => binary(),
    git_ref => binary(),
    repo => binary(),
    authors => [binary()],
    builder => #{
        name => binary(),
        contact => binary(),
        website => binary()
    },
    functionality => [binary()],
    compatibility => #{emqx => binary()},
    built_on_otp_release => binary(),
    metadata_vsn => binary()
}.

-type read_options() :: #{
    fill_readme => boolean()
}.

-export_type([t/0, read_options/0]).

-spec read(name_vsn()) -> {ok, t()} | {error, term()}.
read(NameVsn) ->
    read(NameVsn, #{}).

-spec read(name_vsn(), read_options()) -> {ok, t()} | {error, term()}.
read(NameVsn, Options) ->
    maybe
        {ok, Info0} ?= emqx_plugins_fs:read_info(NameVsn),
        %% TODO
        %% Take only known keys from the info map
        Info1 = emqx_utils_maps:unsafe_atom_key_map(Info0),
        ok ?= check_plugin(Info1, NameVsn),
        Info2 = populate_plugin_readme(NameVsn, Options, Info1),
        Info3 = populate_plugin_package_info(NameVsn, Info2),
        Info = populate_plugin_status(NameVsn, Info3),
        {ok, Info}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

populate_plugin_package_info(NameVsn, Info) ->
    Info#{md5sum => emqx_plugins_fs:read_md5sum(NameVsn)}.

populate_plugin_readme(NameVsn, #{fill_readme := true}, Info) ->
    Info#{readme => emqx_plugins_fs:read_readme(NameVsn)};
populate_plugin_readme(_NameVsn, _Options, Info) ->
    Info.

populate_plugin_status(NameVsn, Info) ->
    RunningSt = emqx_plugins_apps:running_status(NameVsn),
    Configured = lists:filtermap(
        fun(#{name_vsn := Nv, enable := St}) ->
            case bin(Nv) =:= bin(NameVsn) of
                true -> {true, St};
                false -> false
            end
        end,
        configured()
    ),
    ConfSt =
        case Configured of
            [] -> not_configured;
            [true] -> enabled;
            [false] -> disabled
        end,
    Info#{
        running_status => RunningSt,
        config_status => ConfSt
    }.

check_plugin(
    #{
        name := Name,
        rel_vsn := Vsn,
        rel_apps := Apps,
        description := _
    },
    NameVsn
) ->
    case bin(NameVsn) =:= bin([Name, "-", Vsn]) of
        true ->
            try
                %% assert
                [_ | _] = Apps,
                %% validate if the list is all <app>-<vsn> strings
                lists:foreach(
                    fun(App) -> _ = emqx_plugins_utils:parse_name_vsn(App) end, Apps
                )
            catch
                _:_ ->
                    {error, #{
                        msg => "bad_rel_apps",
                        rel_apps => Apps,
                        hint => "A non-empty string list of app_name-app_vsn format"
                    }}
            end;
        false ->
            {error, #{
                msg => "name_vsn_mismatch",
                name_vsn => NameVsn,
                path => emqx_plugins_fs:info_file_path(NameVsn),
                name => Name,
                rel_vsn => Vsn
            }}
    end;
check_plugin(PluginInfo, NameVsn) ->
    {error, #{
        msg => "bad_info_file_content",
        mandatory_fields => [rel_vsn, name, rel_apps, description],
        name_vsn => NameVsn,
        info => PluginInfo,
        path => emqx_plugins_fs:info_file_path(NameVsn)
    }}.

configured() ->
    emqx_conf:get([?CONF_ROOT, states]).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.
