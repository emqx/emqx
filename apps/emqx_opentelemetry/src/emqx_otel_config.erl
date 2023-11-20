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
-module(emqx_otel_config).

-behaviour(emqx_config_handler).

-define(OPTL, [opentelemetry]).

-define(OTEL_EXPORTER, opentelemetry_exporter).
-define(OTEL_LOG_HANDLER, otel_log_handler).
-define(OTEL_LOG_HANDLER_ID, opentelemetry_handler).

-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).
-export([update/1]).
-export([add_otel_log_handler/0, remove_otel_log_handler/0]).
-export([stop_all_otel_apps/0]).
-export([otel_exporter/1]).

update(Config) ->
    case
        emqx_conf:update(
            ?OPTL,
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

add_handler() ->
    ok = emqx_config_handler:add_handler(?OPTL, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?OPTL),
    ok.

post_config_update(?OPTL, _Req, Old, Old, _AppEnvs) ->
    ok;
post_config_update(?OPTL, _Req, New, Old, AppEnvs) ->
    application:set_env(AppEnvs),
    MetricsRes = ensure_otel_metrics(New, Old),
    LogsRes = ensure_otel_logs(New, Old),
    TracesRes = ensure_otel_traces(New, Old),
    _ = maybe_stop_all_otel_apps(New),
    case {MetricsRes, LogsRes, TracesRes} of
        {ok, ok, ok} -> ok;
        Other -> {error, Other}
    end;
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

stop_all_otel_apps() ->
    stop_all_otel_apps(true).

add_otel_log_handler() ->
    ensure_otel_logs(emqx:get_config(?OPTL), #{}).

remove_otel_log_handler() ->
    remove_handler_if_present(?OTEL_LOG_HANDLER_ID).

otel_exporter(ExporterConf) ->
    #{
        endpoint := Endpoint,
        protocol := Proto,
        ssl_options := SSLOpts
    } = ExporterConf,
    {?OTEL_EXPORTER, #{
        endpoint => Endpoint,
        protocol => Proto,
        ssl_options => ssl_opts(Endpoint, SSLOpts)
    }}.

%% Internal functions

ensure_otel_metrics(#{metrics := MetricsConf}, #{metrics := MetricsConf}) ->
    ok;
ensure_otel_metrics(#{metrics := #{enable := true} = MetricsConf}, _Old) ->
    _ = emqx_otel_metrics:stop_otel(),
    emqx_otel_metrics:start_otel(MetricsConf);
ensure_otel_metrics(#{metrics := #{enable := false}}, _Old) ->
    emqx_otel_metrics:stop_otel();
ensure_otel_metrics(_, _) ->
    ok.

ensure_otel_logs(#{logs := LogsConf}, #{logs := LogsConf}) ->
    ok;
ensure_otel_logs(#{logs := #{enable := true} = LogsConf}, _OldConf) ->
    ok = remove_handler_if_present(?OTEL_LOG_HANDLER_ID),
    ok = ensure_log_apps(),
    HandlerConf = tr_handler_conf(LogsConf),
    %% NOTE: should primary logger level be updated if it's higher than otel log level?
    logger:add_handler(?OTEL_LOG_HANDLER_ID, ?OTEL_LOG_HANDLER, HandlerConf);
ensure_otel_logs(#{logs := #{enable := false}}, _OldConf) ->
    remove_handler_if_present(?OTEL_LOG_HANDLER_ID).

ensure_otel_traces(#{traces := TracesConf}, #{traces := TracesConf}) ->
    ok;
ensure_otel_traces(#{traces := #{enable := true} = TracesConf}, _OldConf) ->
    emqx_otel_trace:start(TracesConf);
ensure_otel_traces(#{traces := #{enable := false}}, _OldConf) ->
    emqx_otel_trace:stop().

remove_handler_if_present(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, _} ->
            ok = logger:remove_handler(HandlerId);
        _ ->
            ok
    end.

ensure_log_apps() ->
    {ok, _} = application:ensure_all_started(opentelemetry_exporter),
    {ok, _} = application:ensure_all_started(opentelemetry_experimental),
    ok.

maybe_stop_all_otel_apps(#{
    metrics := #{enable := false},
    logs := #{enable := false},
    traces := #{enable := false}
}) ->
    IsShutdown = false,
    stop_all_otel_apps(IsShutdown);
maybe_stop_all_otel_apps(_) ->
    ok.

tr_handler_conf(Conf) ->
    #{
        level := Level,
        max_queue_size := MaxQueueSize,
        exporting_timeout := ExportingTimeout,
        scheduled_delay := ScheduledDelay,
        exporter := ExporterConf
    } = Conf,
    #{
        level => Level,
        config => #{
            max_queue_size => MaxQueueSize,
            exporting_timeout_ms => ExportingTimeout,
            scheduled_delay_ms => ScheduledDelay,
            exporter => otel_exporter(ExporterConf)
        }
    }.

ssl_opts(Endpoint, SSLOpts) ->
    case is_ssl(Endpoint) of
        true ->
            emqx_tls_lib:to_client_opts(SSLOpts#{enable => true});
        false ->
            []
    end.

is_ssl(<<"https://", _/binary>> = _Endpoint) ->
    true;
is_ssl(_Endpoint) ->
    false.

stop_all_otel_apps(IsShutdown) ->
    %% if traces were enabled, it's not safe to stop opentelemetry app,
    %% as there could be not finsihed traces that would crash if spans ETS tables are deleted
    _ =
        case IsShutdown of
            true ->
                _ = application:stop(opentelemetry);
            false ->
                ok
        end,
    _ = application:stop(opentelemetry_experimental),
    _ = application:stop(opentelemetry_experimental_api),
    _ = application:stop(opentelemetry_exporter),
    ok.
