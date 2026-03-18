%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_info).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    health_status => #{
        status := ok | error,
        message => binary()
    },
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
    fill_readme => boolean(),
    health_check => boolean()
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
        Info4 = populate_plugin_status(NameVsn, Info3),
        Info = populate_plugin_health_status(NameVsn, Options, Info4),
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
    RunningSt = emqx_plugins_apps:running_status(Info),
    ConfSt = configured_status(NameVsn, configured()),
    Info#{
        running_status => RunningSt,
        config_status => ConfSt
    }.

populate_plugin_health_status(
    NameVsn, #{health_check := true} = _Options, #{running_status := running} = Info
) ->
    case emqx_plugins_apps:on_health_check(NameVsn, #{}) of
        ok ->
            Info#{health_status => #{status => ok, message => <<"">>}};
        {error, Reason} ->
            Info#{
                health_status => #{
                    status => error, message => emqx_utils:readable_error_msg(Reason)
                }
            }
    end;
populate_plugin_health_status(_NameVsn, _Options, Info) ->
    Info.

check_plugin(
    #{
        name := Name,
        rel_vsn := Vsn,
        rel_apps := Apps,
        description := _
    },
    NameVsn
) ->
    case emqx_plugins_utils:bin(NameVsn) =:= emqx_plugins_utils:bin([Name, "-", Vsn]) of
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
    configured(emqx_conf:get([?CONF_ROOT, states])).

configured(States) ->
    lists:map(fun emqx_plugins_utils:normalize_state_item/1, States).

-doc """
Return the effective configuration status for a plugin version.

If the version has no entry in `plugins.states`, the result is `not_configured`.
If it is configured but not enabled, the result is `disabled`.
If it is enabled, the result is still `disabled` when a newer version of the same
plugin is also enabled, because only the latest enabled version is treated as
effectively enabled.
""".
configured_status(NameVsn, Configured) ->
    NameVsnBin = emqx_plugins_utils:bin(NameVsn),
    ExactStates = [
        Enabled
     || #{name_vsn := NV, enable := Enabled} <- Configured,
        emqx_plugins_utils:bin(NV) =:= NameVsnBin
    ],
    case lists:member(true, ExactStates) of
        true ->
            enabled_status(NameVsnBin, Configured);
        false when ExactStates =:= [] ->
            not_configured;
        false ->
            disabled
    end.

enabled_status(NameVsn, Configured) ->
    Name = emqx_plugins_utils:plugin_name(NameVsn),
    EnabledVersions = [
        emqx_plugins_utils:bin(NV)
     || #{name_vsn := NV, enable := true} <- Configured,
        emqx_plugins_utils:plugin_name(NV) =:= Name
    ],
    case EnabledVersions of
        [] ->
            disabled;
        [NameVsn] ->
            enabled;
        [_ | _] ->
            case lists:foldl(fun emqx_plugins_utils:latest_name_vsn/2, NameVsn, EnabledVersions) of
                NameVsn -> enabled;
                _Other -> disabled
            end
    end.

-ifdef(TEST).

configured_status_test_() ->
    [
        ?_assertEqual(not_configured, configured_status(<<"demo-1.0.0">>, [])),
        ?_assertEqual(
            disabled,
            configured_status(<<"demo-1.0.0">>, [#{name_vsn => <<"demo-1.0.0">>, enable => false}])
        ),
        ?_assertEqual(
            enabled,
            configured_status(<<"demo-2.0.0">>, [
                #{name_vsn => <<"demo-1.0.0">>, enable => false},
                #{name_vsn => <<"demo-2.0.0">>, enable => true}
            ])
        ),
        ?_assertEqual(
            disabled,
            configured_status(<<"demo-1.0.0">>, [
                #{name_vsn => <<"demo-1.0.0">>, enable => true},
                #{name_vsn => <<"demo-2.0.0">>, enable => true}
            ])
        ),
        ?_assertEqual(
            #{name_vsn => <<"demo-1.0.0">>, enable => true},
            emqx_plugins_utils:normalize_state_item(
                #{<<"name_vsn">> => <<"demo-1.0.0">>, <<"enable">> => true}
            )
        )
    ].

configured_case_test() ->
    States = [#{<<"name_vsn">> => <<"demo-1.0.0">>, <<"enable">> => true}],
    ?assertEqual(
        [#{name_vsn => <<"demo-1.0.0">>, enable => true}],
        configured(States)
    ).
-endif.
