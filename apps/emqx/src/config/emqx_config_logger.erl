%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_config_logger).

-behaviour(emqx_config_handler).

%% API
-export([tr_handlers/1, tr_level/1]).
-export([add_handler/0, remove_handler/0, refresh_config/0]).
-export([post_config_update/5]).
-export([filter_audit/2]).

-include("logger.hrl").

-define(LOG, [log]).

add_handler() ->
    ok = emqx_config_handler:add_handler(?LOG, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?LOG),
    ok.

%% refresh logger config when booting, the cluster config may have changed after node start.
%% Kernel's app env is confirmed before the node starts,
%% but we only copy cluster.conf from other node after this node starts,
%% so we need to refresh the logger config after this node starts.
%% It will not affect the logger config when cluster.conf is unchanged.
refresh_config() ->
    %% read the checked config
    LogConfig = emqx:get_config(?LOG, undefined),
    do_refresh_config(#{log => LogConfig}).

%% this call is shared between initial config refresh at boot
%% and dynamic config update from HTTP API
do_refresh_config(Conf) ->
    Handlers = tr_handlers(Conf),
    ok = update_log_handlers(Handlers),
    Level = tr_level(Conf),
    ok = maybe_update_log_level(Level),
    ok.

%% always refresh config when the override config is changed
post_config_update(?LOG, _Req, NewConf, _OldConf, _AppEnvs) ->
    do_refresh_config(#{log => NewConf}).

maybe_update_log_level(NewLevel) ->
    OldLevel = emqx_logger:get_primary_log_level(),
    case OldLevel =:= NewLevel of
        true ->
            %% no change
            ok;
        false ->
            ok = emqx_logger:set_primary_log_level(NewLevel),
            %% also update kernel's logger_level for troubleshooting
            %% what is actually in effect is the logger's primary log level
            ok = application:set_env(kernel, logger_level, NewLevel),
            log_to_console("Config override: log level is set to '~p'~n", [NewLevel])
    end.

log_to_console(Fmt, Args) ->
    io:format(standard_error, Fmt, Args).

update_log_handlers(NewHandlers) ->
    OldHandlers = application:get_env(kernel, logger, []),
    NewHandlersIds = lists:map(fun({handler, Id, _Mod, _Conf}) -> Id end, NewHandlers),
    OldHandlersIds = lists:map(fun({handler, Id, _Mod, _Conf}) -> Id end, OldHandlers),
    Removes = lists:map(fun(Id) -> {removed, Id} end, OldHandlersIds -- NewHandlersIds),
    MapFn = fun({handler, Id, Mod, Conf} = Handler) ->
        case lists:keyfind(Id, 2, OldHandlers) of
            {handler, Id, Mod, Conf} ->
                %% no change
                false;
            {handler, Id, _Mod, _Conf} ->
                {true, {updated, Handler}};
            false ->
                {true, {enabled, Handler}}
        end
    end,
    AddsAndUpdates = lists:filtermap(MapFn, NewHandlers),
    lists:foreach(fun update_log_handler/1, Removes ++ AddsAndUpdates),
    ok = application:set_env(kernel, logger, NewHandlers),
    ok.

%% Don't remove audit log handler here, we need record this removed action into audit log file.
%% we will remove audit log handler after audit log is record in emqx_audit:log/3.
update_log_handler({removed, ?AUDIT_HANDLER}) ->
    ok;
update_log_handler({removed, Id}) ->
    log_to_console("Config override: ~s is removed~n", [id_for_log(Id)]),
    logger:remove_handler(Id);
update_log_handler({Action, {handler, Id, Mod, Conf}}) ->
    log_to_console("Config override: ~s is ~p~n", [id_for_log(Id), Action]),
    % may return {error, {not_found, Id}}
    _ = logger:remove_handler(Id),
    case logger:add_handler(Id, Mod, Conf) of
        ok ->
            ok;
        %% Don't crash here, otherwise the cluster rpc will retry the wrong handler forever.
        {error, Reason} ->
            log_to_console(
                "Config override: ~s is ~p, but failed to add handler: ~p~n",
                [id_for_log(Id), Action, Reason]
            )
    end,
    ok.

id_for_log(console) -> "log.console";
id_for_log(Other) -> "log.file." ++ atom_to_list(Other).

atom(Id) when is_binary(Id) -> binary_to_atom(Id, utf8);
atom(Id) when is_atom(Id) -> Id.

%% @doc Translate raw config to app-env compatible log handler configs list.
tr_handlers(Conf) ->
    %% mute the default handler
    tr_console_handler(Conf) ++
        tr_file_handlers(Conf).

%% For the default logger that outputs to console
tr_console_handler(Conf) ->
    case conf_get("log.console.enable", Conf) of
        true ->
            ConsoleConf = conf_get("log.console", Conf),
            [
                {handler, console, logger_std_h, #{
                    level => conf_get("log.console.level", Conf),
                    config => (log_handler_conf(ConsoleConf))#{type => standard_io},
                    formatter => log_formatter(console, ConsoleConf),
                    filters => log_filter(console, ConsoleConf)
                }}
            ];
        false ->
            []
    end.

%% For the file logger
tr_file_handlers(Conf) ->
    Files = logger_file_handlers(Conf),
    Audits = logger_audit_handler(Conf),
    Handlers = Audits ++ Files,
    lists:map(fun tr_file_handler/1, Handlers).

tr_file_handler({HandlerName, SubConf}) ->
    FilePath = conf_get("path", SubConf),
    RotationCount = conf_get("rotation_count", SubConf),
    RotationSize = conf_get("rotation_size", SubConf),
    Type =
        case RotationSize of
            infinity -> halt;
            _ -> wrap
        end,
    HandlerConf = log_handler_conf(SubConf),
    {handler, atom(HandlerName), logger_disk_log_h, #{
        level => conf_get("level", SubConf),
        config => HandlerConf#{
            type => Type,
            file => emqx_schema:naive_env_interpolation(FilePath),
            max_no_files => RotationCount,
            max_no_bytes => RotationSize
        },
        formatter => log_formatter(HandlerName, SubConf),
        filters => log_filter(HandlerName, SubConf),
        filesync_repeat_interval => no_repeat
    }}.

logger_audit_handler(Conf) ->
    Handlers = [{?AUDIT_HANDLER, conf_get("log.audit", Conf, #{})}],
    logger_handlers(Handlers).

logger_file_handlers(Conf) ->
    Handlers = maps:to_list(conf_get("log.file", Conf, #{})),
    logger_handlers(Handlers).

logger_handlers(Handlers) ->
    keep_only_enabeld(Handlers).

keep_only_enabeld(Handlers) ->
    lists:filter(
        fun({_Name, Handler}) ->
            conf_get("enable", Handler, false)
        end,
        Handlers
    ).

conf_get(Key, Conf) -> emqx_schema:conf_get(Key, Conf).
conf_get(Key, Conf, Default) -> emqx_schema:conf_get(Key, Conf, Default).

log_handler_conf(Conf) ->
    SycModeQlen = conf_get("sync_mode_qlen", Conf),
    DropModeQlen = conf_get("drop_mode_qlen", Conf),
    FlushQlen = conf_get("flush_qlen", Conf),
    Overkill = conf_get("overload_kill", Conf),
    BurstLimit = conf_get("burst_limit", Conf),
    #{
        sync_mode_qlen => SycModeQlen,
        drop_mode_qlen => DropModeQlen,
        flush_qlen => FlushQlen,
        overload_kill_enable => conf_get("enable", Overkill),
        overload_kill_qlen => conf_get("qlen", Overkill),
        overload_kill_mem_size => conf_get("mem_size", Overkill),
        overload_kill_restart_after => conf_get("restart_after", Overkill),
        burst_limit_enable => conf_get("enable", BurstLimit),
        burst_limit_max_count => conf_get("max_count", BurstLimit),
        burst_limit_window_time => conf_get("window_time", BurstLimit)
    }.

log_formatter(HandlerName, Conf) ->
    CharsLimit =
        case conf_get("chars_limit", Conf) of
            unlimited -> unlimited;
            V when V > 0 -> V
        end,
    TimeOffSet =
        case conf_get("time_offset", Conf) of
            "system" -> "";
            "utc" -> 0;
            OffSetStr -> OffSetStr
        end,
    SingleLine = conf_get("single_line", Conf),
    Depth = conf_get("max_depth", Conf),
    Format =
        case HandlerName of
            ?AUDIT_HANDLER ->
                json;
            _ ->
                conf_get("formatter", Conf)
        end,
    TsFormat = timestamp_format(Conf),
    WithMfa = conf_get("with_mfa", Conf),
    PayloadEncode = conf_get("payload_encode", Conf, text),
    do_formatter(
        Format, CharsLimit, SingleLine, TimeOffSet, Depth, TsFormat, WithMfa, PayloadEncode
    ).

%% auto | epoch | rfc3339
timestamp_format(Conf) ->
    conf_get("timestamp_format", Conf).

%% helpers
do_formatter(json, CharsLimit, SingleLine, TimeOffSet, Depth, TsFormat, WithMfa, PayloadEncode) ->
    {emqx_logger_jsonfmt, #{
        chars_limit => CharsLimit,
        single_line => SingleLine,
        time_offset => TimeOffSet,
        depth => Depth,
        timestamp_format => TsFormat,
        with_mfa => WithMfa,
        payload_encode => PayloadEncode
    }};
do_formatter(text, CharsLimit, SingleLine, TimeOffSet, Depth, TsFormat, WithMfa, PayloadEncode) ->
    {emqx_logger_textfmt, #{
        template => ["[", level, "] ", msg, "\n"],
        chars_limit => CharsLimit,
        single_line => SingleLine,
        time_offset => TimeOffSet,
        depth => Depth,
        timestamp_format => TsFormat,
        with_mfa => WithMfa,
        payload_encode => PayloadEncode
    }}.

%% Don't record all logger message
%% only use it for ?AUDIT/1
log_filter(?AUDIT_HANDLER, _Conf) ->
    [{filter_audit, {fun ?MODULE:filter_audit/2, stop}}];
log_filter(_, Conf) ->
    case conf_get("supervisor_reports", Conf) of
        error -> [{drop_progress_reports, {fun logger_filters:progress/2, stop}}];
        progress -> []
    end.

filter_audit(_, _) -> stop.

tr_level(Conf) ->
    ConsoleLevel = conf_get("log.console.level", Conf, undefined),
    FileLevels = [conf_get("level", SubConf) || {_, SubConf} <- logger_file_handlers(Conf)],
    case FileLevels ++ [ConsoleLevel || ConsoleLevel =/= undefined] of
        %% warning is the default level we should use
        [] -> warning;
        Levels -> least_severe_log_level(Levels)
    end.

least_severe_log_level(Levels) ->
    hd(sort_log_levels(Levels)).

sort_log_levels(Levels) ->
    lists:sort(
        fun(A, B) ->
            case logger:compare_levels(A, B) of
                R when R == lt; R == eq -> true;
                gt -> false
            end
        end,
        Levels
    ).
