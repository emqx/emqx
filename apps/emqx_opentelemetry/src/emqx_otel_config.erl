%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_config).

-behaviour(emqx_config_handler).

-include_lib("emqx/include/logger.hrl").

-define(OTEL, [opentelemetry]).
-define(CERTS_PATH, filename:join(["opentelemetry", "exporter"])).

-define(OTEL_EXPORTER, opentelemetry_exporter).
-define(OTEL_LOG_HANDLER, otel_log_handler).
-define(OTEL_LOG_HANDLER_ID, opentelemetry_handler).

-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).
-export([update/1]).
-export([add_otel_log_handler/0, remove_otel_log_handler/0]).
-export([otel_exporter/1]).

update(Config) ->
    case
        emqx_conf:update(
            ?OTEL,
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
    ok = emqx_config_handler:add_handler(?OTEL, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?OTEL),
    ok.

pre_config_update(?OTEL, RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update(?OTEL, NewRawConf, _RawConf) ->
    {ok, convert_certs(NewRawConf)}.

post_config_update(?OTEL, _Req, Old, Old, _AppEnvs) ->
    ok;
post_config_update(?OTEL, _Req, New, Old, AppEnvs) ->
    application:set_env(AppEnvs),
    MetricsRes = ensure_otel_metrics(New, Old),
    LogsRes = ensure_otel_logs(New, Old),
    TracesRes = ensure_otel_traces(New, Old),
    case {MetricsRes, LogsRes, TracesRes} of
        {ok, ok, ok} -> ok;
        Other -> {error, Other}
    end;
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

add_otel_log_handler() ->
    ensure_otel_logs(emqx:get_config(?OTEL), #{}).

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

convert_certs(#{<<"exporter">> := ExporterConf} = NewRawConf) ->
    NewRawConf#{<<"exporter">> => convert_exporter_certs(ExporterConf)};
convert_certs(#{exporter := ExporterConf} = NewRawConf) ->
    NewRawConf#{exporter => convert_exporter_certs(ExporterConf)};
convert_certs(NewRawConf) ->
    NewRawConf.

convert_exporter_certs(#{<<"ssl_options">> := SSLOpts} = ExporterConf) ->
    ExporterConf#{<<"ssl_options">> => do_convert_certs(SSLOpts)};
convert_exporter_certs(#{ssl_options := SSLOpts} = ExporterConf) ->
    ExporterConf#{ssl_options => do_convert_certs(SSLOpts)};
convert_exporter_certs(ExporterConf) ->
    ExporterConf.

do_convert_certs(SSLOpts) ->
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(?CERTS_PATH, SSLOpts) of
        {ok, undefined} ->
            SSLOpts;
        {ok, SSLOpts1} ->
            SSLOpts1;
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config", name => "opentelemetry_exporter"}),
            throw({bad_ssl_config, Reason})
    end.

ensure_otel_metrics(
    #{metrics := MetricsConf, exporter := Exporter},
    #{metrics := MetricsConf, exporter := Exporter}
) ->
    ok;
ensure_otel_metrics(#{metrics := #{enable := true}} = Conf, _Old) ->
    ok = emqx_otel_cpu_sup:stop_otel_cpu_sup(),
    _ = emqx_otel_cpu_sup:start_otel_cpu_sup(Conf),
    _ = emqx_otel_metrics:stop_otel(),
    emqx_otel_metrics:start_otel(Conf);
ensure_otel_metrics(#{metrics := #{enable := false}}, _Old) ->
    ok = emqx_otel_cpu_sup:stop_otel_cpu_sup(),
    emqx_otel_metrics:stop_otel();
ensure_otel_metrics(_, _) ->
    ok.

ensure_otel_logs(
    #{logs := LogsConf, exporter := Exporter},
    #{logs := LogsConf, exporter := Exporter}
) ->
    ok;
ensure_otel_logs(#{logs := #{enable := true}} = Conf, _OldConf) ->
    ok = remove_handler_if_present(?OTEL_LOG_HANDLER_ID),
    HandlerConf = tr_handler_conf(Conf),
    %% NOTE: should primary logger level be updated if it's higher than otel log level?
    logger:add_handler(?OTEL_LOG_HANDLER_ID, ?OTEL_LOG_HANDLER, HandlerConf);
ensure_otel_logs(#{logs := #{enable := false}}, _OldConf) ->
    remove_handler_if_present(?OTEL_LOG_HANDLER_ID).

ensure_otel_traces(
    #{traces := TracesConf, exporter := Exporter},
    #{traces := TracesConf, exporter := Exporter}
) ->
    ok;
ensure_otel_traces(#{traces := #{enable := true}} = Conf, _OldConf) ->
    _ = emqx_otel_trace:stop(),
    emqx_otel_trace:start(Conf);
ensure_otel_traces(#{traces := #{enable := false}}, _OldConf) ->
    emqx_otel_trace:stop().

remove_handler_if_present(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, _} ->
            ok = logger:remove_handler(HandlerId);
        _ ->
            ok
    end.

tr_handler_conf(#{logs := LogsConf, exporter := ExporterConf}) ->
    #{
        level := Level,
        max_queue_size := MaxQueueSize,
        exporting_timeout := ExportingTimeout,
        scheduled_delay := ScheduledDelay
    } = LogsConf,
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
            %% force enable ssl
            emqx_tls_lib:to_client_opts(SSLOpts#{enable => true});
        false ->
            []
    end.

is_ssl(<<"https://", _/binary>>) ->
    true;
is_ssl(<<"http://", _/binary>>) ->
    false.
