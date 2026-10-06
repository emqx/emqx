%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_pinned).

-moduledoc """
Node-local plugins listed in `node.pinned_plugins`.

A pinned plugin starts from this node's `install_dir`, is always enabled, and
takes no part in cluster plugin sync. This node ignores every `plugins.states`
entry whose plugin name is pinned, whatever its version.
""".

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    list/0,
    find/1,
    is_pinned/1,
    is_pinned_name_vsn/1,
    is_other_version/1,
    refusal/1
]).

%% Config source
-export([
    read_config/1,
    override_file_path/1
]).

%% Alarms
-export([
    raise_alarm/2,
    clear_alarm/1
]).

-define(ALARM_PREFIX, "pinned_plugin_unavailable:").

%%--------------------------------------------------------------------
%% Pinned list
%%--------------------------------------------------------------------

-doc "Return the name-vsns in `node.pinned_plugins`, in start order.".
-spec list() -> [binary()].
list() ->
    emqx:get_config([node, pinned_plugins], []).

-doc "Return the pinned name-vsn that has the same plugin name as `NameVsn`.".
-spec find(name_vsn()) -> {ok, binary()} | false.
find(NameVsn) ->
    Name = name(NameVsn),
    case [P || P <- list(), name(P) =:= Name] of
        [Pinned | _] -> {ok, Pinned};
        [] -> false
    end.

-doc "Return `true` when this node pins the plugin name of `NameVsn`, in any version.".
-spec is_pinned(name_vsn()) -> boolean().
is_pinned(NameVsn) ->
    find(NameVsn) =/= false.

-doc "Return `true` when `NameVsn` is exactly a pinned name-vsn.".
-spec is_pinned_name_vsn(name_vsn()) -> boolean().
is_pinned_name_vsn(NameVsn) ->
    lists:member(emqx_plugins_utils:bin(NameVsn), list()).

-doc """
Return `true` when this node pins the plugin name of `NameVsn` at another
version. This node never runs such a version.
""".
-spec is_other_version(name_vsn()) -> boolean().
is_other_version(NameVsn) ->
    is_pinned(NameVsn) andalso not is_pinned_name_vsn(NameVsn).

-doc "Return the error for a lifecycle operation that a pinned plugin does not allow.".
-spec refusal(name_vsn()) -> map().
refusal(NameVsn) ->
    {ok, Pinned} = find(NameVsn),
    #{
        kind => pinned,
        msg => "plugin_pinned",
        name_vsn => emqx_plugins_utils:bin(NameVsn),
        pinned => Pinned,
        hint =>
            <<
                "This node lists the plugin in node.pinned_plugins. "
                "Edit node.pinned_plugins and restart the node to change it."
            >>
    }.

%%--------------------------------------------------------------------
%% Config source
%%--------------------------------------------------------------------

-doc """
Read the config of a pinned plugin.

A config saved through the API is stored in `data/plugins/<name>/config.hocon`.
When that file exists, it is the whole config. Otherwise the source is the
package default `priv/config.hocon`, with `etc/plugins/<name>.hocon` merged on
top of it when that file exists. No peer node is read.
""".
-spec read_config(name_vsn()) -> {ok, map()} | {error, term()}.
read_config(NameVsn) ->
    case emqx_plugins_local_config:read(NameVsn) of
        {ok, Config} ->
            {ok, Config};
        {error, #{reason := {enoent, _}}} ->
            read_image_config(NameVsn);
        {error, Reason} ->
            {error, #{
                kind => invalid_config,
                msg => "bad_pinned_plugin_config_file",
                name_vsn => emqx_plugins_utils:bin(NameVsn),
                path => emqx_plugins_fs:config_file_path(NameVsn),
                reason => Reason
            }}
    end.

read_image_config(NameVsn) ->
    maybe
        {ok, Default} ?= read_default(NameVsn),
        {ok, Override} ?= read_override(NameVsn),
        {ok, emqx_utils_maps:deep_merge(Default, Override)}
    end.

-spec override_file_path(name_vsn()) -> string().
override_file_path(NameVsn) ->
    emqx:etc_file(filename:join(["plugins", binary_to_list(name(NameVsn)) ++ ".hocon"])).

read_default(NameVsn) ->
    case emqx_plugins_fs:read_default_hocon(NameVsn) of
        {ok, Config} -> {ok, Config};
        {error, #{reason := {enoent, _}}} -> {ok, #{}};
        {error, _} = Error -> Error
    end.

read_override(NameVsn) ->
    Path = override_file_path(NameVsn),
    case filelib:is_regular(Path) of
        false ->
            {ok, #{}};
        true ->
            case hocon:load(Path, #{format => richmap}) of
                {ok, RichMap} ->
                    {ok, hocon_maps:ensure_plain(RichMap)};
                {error, Reason} ->
                    {error, #{
                        kind => invalid_config,
                        msg => "bad_pinned_plugin_config_file",
                        name_vsn => emqx_plugins_utils:bin(NameVsn),
                        path => Path,
                        reason => Reason
                    }}
            end
    end.

%%--------------------------------------------------------------------
%% Alarms
%%--------------------------------------------------------------------

-doc "Log an error and raise an alarm for a pinned plugin that failed to install or start.".
-spec raise_alarm(name_vsn(), term()) -> ok.
raise_alarm(NameVsn, Reason) ->
    NameVsnBin = emqx_plugins_utils:bin(NameVsn),
    ?SLOG(error, #{
        msg => "pinned_plugin_unavailable",
        name_vsn => NameVsnBin,
        reason => Reason
    }),
    Message = iolist_to_binary([
        "Pinned plugin ",
        NameVsnBin,
        " is not running. Check the plugin package in install_dir and the node logs."
    ]),
    _ = emqx_alarm:safe_activate(alarm_name(NameVsnBin), #{name_vsn => NameVsnBin}, Message),
    ok.

-spec clear_alarm(name_vsn()) -> ok.
clear_alarm(NameVsn) ->
    _ = emqx_alarm:ensure_deactivated(alarm_name(emqx_plugins_utils:bin(NameVsn))),
    ok.

alarm_name(NameVsn) ->
    <<?ALARM_PREFIX, NameVsn/binary>>.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% The plugin name is the part before the first dash. It is taken as a binary,
%% so an arbitrary name from an API request does not create an atom.
name(NameVsn) ->
    hd(binary:split(emqx_plugins_utils:bin(NameVsn), <<"-">>)).
