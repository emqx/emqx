%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_api).

-moduledoc """
REST API handlers for UNS Governance status, metrics, models and validation.
""".

-export([handle/3]).

handle(get, [<<"status">>], _Request) ->
    {ok, 200, #{}, #{
        plugin => <<"emqx_unsgov">>,
        on_mismatch => emqx_unsgov_config:on_mismatch(),
        exempt_topics => emqx_unsgov_config:exempt_topics()
    }};
handle(get, [<<"stats">>], _Request) ->
    Stats = emqx_unsgov_metrics:snapshot(),
    {ok, 200, #{}, Stats};
handle(get, [<<"events">>], _Request) ->
    Events = emqx_unsgov_metrics:recent_events(),
    {ok, 200, #{}, #{data => Events}};
handle(get, [<<"metrics">>], _Request) ->
    Stats = emqx_unsgov_metrics:snapshot(),
    {ok, 200, #{<<"content-type">> => <<"text/plain; version=0.0.4; charset=utf-8">>},
        iolist_to_binary(prometheus_metrics(Stats))};
handle(get, [<<"ui">>], _Request) ->
    {ok, 200,
        #{
            <<"content-type">> => <<"text/html; charset=utf-8">>,
            <<"cache-control">> => <<"no-store, no-cache, must-revalidate">>,
            <<"pragma">> => <<"no-cache">>,
            <<"expires">> => <<"0">>
        },
        ui_html()};
handle(get, [<<"models">>], _Request) ->
    {ok, Entries} = emqx_unsgov_store:list_models(),
    {ok, 200, #{}, #{data => Entries}};
handle(get, [<<"models">>, Id], _Request) ->
    case emqx_unsgov_store:get_model(Id) of
        {ok, Entry} ->
            {ok, 200, #{}, Entry};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(post, [<<"models">>], Request) ->
    Body = maps:get(body, Request, #{}),
    Activate = get_activate_flag(Body),
    Model = get_model_body(Body),
    case emqx_unsgov_store:put_model(Model, Activate) of
        {ok, Entry} ->
            {ok, 200, #{}, Entry};
        {error, Reason} ->
            bad_model(Reason)
    end;
handle(post, [<<"models">>, Id, <<"activate">>], _Request) ->
    case emqx_unsgov_store:activate(Id) of
        ok ->
            {ok, 200, #{}, #{id => Id, active => true}};
        {error, Reason} when Reason =/= not_found ->
            bad_model(Reason);
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(post, [<<"models">>, Id, <<"deactivate">>], _Request) ->
    case emqx_unsgov_store:deactivate(Id) of
        ok ->
            {ok, 200, #{}, #{id => Id, active => false}};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(delete, [<<"models">>, Id], _Request) ->
    case emqx_unsgov_store:delete_model(Id) of
        ok ->
            {ok, 200, #{}, #{id => Id, deleted => true}};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(post, [<<"validate">>, <<"topic">>], Request) ->
    Topic = get_topic(maps:get(body, Request, #{})),
    case Topic of
        <<>> ->
            {error, 400, #{}, #{
                code => <<"BAD_REQUEST">>,
                message => <<"topic is required">>
            }};
        _ ->
            Result = emqx_unsgov_store:validate_topic(Topic),
            {ok, 200, #{}, #{
                topic => Topic,
                result => format_validate_result(Result)
            }}
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

bad_model(Reason) ->
    Reason1 = normalize_reason(Reason),
    case maps:get(cause, Reason1, unknown) of
        conflicting_topic_filter ->
            {error, 409, #{}, #{
                code => <<"CONFLICT">>,
                message => model_error_message(Reason1),
                reason => Reason1
            }};
        _ ->
            {error, 400, #{}, #{
                code => <<"BAD_MODEL">>,
                message => model_error_message(Reason1),
                reason => Reason1
            }}
    end.

normalize_reason(Reason) when is_map(Reason) ->
    Reason;
normalize_reason({Cause, Value}) when is_atom(Cause) ->
    #{cause => Cause, value => Value};
normalize_reason({Cause, Value1, Value2}) when is_atom(Cause) ->
    #{cause => Cause, value => Value1, value2 => Value2};
normalize_reason(Cause) when is_atom(Cause) ->
    #{cause => Cause};
normalize_reason(Other) ->
    #{cause => unknown, detail => iolist_to_binary(io_lib:format("~p", [Other]))}.

model_error_message(#{cause := conflicting_topic_filter, conflict_with := ConflictWith}) ->
    iolist_to_binary(
        io_lib:format("Conflicting topic filter with active model ~ts", [to_bin(ConflictWith)])
    );
model_error_message(#{cause := Cause}) when is_atom(Cause) ->
    iolist_to_binary(io_lib:format("Bad model: ~ts", [Cause]));
model_error_message(_Reason) ->
    <<"Bad model">>.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_list(V) -> unicode:characters_to_binary(V);
to_bin(V) -> iolist_to_binary(io_lib:format("~p", [V])).

get_activate_flag(#{<<"activate">> := V}) ->
    normalize_bool(V);
get_activate_flag(_) ->
    false.

get_model_body(#{<<"model">> := Model}) when is_map(Model) ->
    Model;
get_model_body(Body) ->
    Body.

get_topic(#{<<"topic">> := Topic}) ->
    Topic;
get_topic(_) ->
    <<>>.

format_validate_result({allow, _ModelId}) ->
    #{valid => true};
format_validate_result({deny, _ModelId, Reason}) ->
    #{valid => false, reason => Reason}.

normalize_bool(true) -> true;
normalize_bool(_) -> false.

ui_html() ->
    maybe
        Dir = code:priv_dir(emqx_unsgov),
        true ?= is_list(Dir),
        {ok, Bin} ?= file:read_file(filename:join(Dir, "ui.html")),
        Bin
    else
        _ -> ui_not_available()
    end.

ui_not_available() ->
    <<
        "<!doctype html><html><body><h1>UNS Governance UI unavailable</h1>"
        "<p>Missing priv/ui.html</p></body></html>"
    >>.

prometheus_metrics(Stats) ->
    Counters = [
        {<<"emqx_unsgov_messages_total">>,
            <<"Total messages evaluated by UNS Governance (cluster-aggregated).">>},
        {<<"emqx_unsgov_messages_allowed_total">>,
            <<"Messages allowed by UNS Governance (cluster-aggregated).">>},
        {<<"emqx_unsgov_messages_dropped_total">>,
            <<"Messages dropped by UNS Governance (cluster-aggregated).">>},
        {<<"emqx_unsgov_topic_nomatch_total">>,
            <<"Messages that matched no active model, counted independently from drops (cluster-aggregated).">>},
        {<<"emqx_unsgov_topic_invalid_total">>,
            <<"Messages dropped because topic failed model validation (cluster-aggregated).">>},
        {<<"emqx_unsgov_payload_invalid_total">>,
            <<"Messages dropped because payload failed schema validation (cluster-aggregated).">>},
        {<<"emqx_unsgov_not_endpoint_total">>,
            <<"Messages dropped because topic is a namespace, not an endpoint (cluster-aggregated).">>},
        {<<"emqx_unsgov_exempt_total">>,
            <<"Messages skipped because topic is exempt from governance (cluster-aggregated).">>}
    ],
    Gauges = [
        {<<"emqx_unsgov_uptime_seconds">>,
            <<"UNS Governance plugin uptime in seconds (cluster-aggregated).">>}
    ],
    PerModelCounters = [
        {<<"emqx_unsgov_model_messages_total">>,
            <<"Total messages evaluated per model (cluster-aggregated).">>},
        {<<"emqx_unsgov_model_messages_allowed_total">>,
            <<"Messages allowed per model (cluster-aggregated).">>},
        {<<"emqx_unsgov_model_messages_dropped_total">>,
            <<"Messages dropped per model (cluster-aggregated).">>},
        {<<"emqx_unsgov_model_topic_invalid_total">>,
            <<"Messages with invalid topic per model (cluster-aggregated).">>},
        {<<"emqx_unsgov_model_payload_invalid_total">>,
            <<"Messages with invalid payload per model (cluster-aggregated).">>},
        {<<"emqx_unsgov_model_not_endpoint_total">>,
            <<"Messages targeting non-endpoint per model (cluster-aggregated).">>}
    ],
    PerModel = maps:get(per_model, Stats, #{}),
    [
        [format_help_and_type(Name, Help, <<"counter">>) || {Name, Help} <- Counters],
        [format_metric(Name, maps:get(stat_key(Name), Stats, 0)) || {Name, _} <- Counters],
        [format_help_and_type(Name, Help, <<"gauge">>) || {Name, Help} <- Gauges],
        [format_metric(Name, maps:get(stat_key(Name), Stats, 0)) || {Name, _} <- Gauges],
        [format_help_and_type(Name, Help, <<"counter">>) || {Name, Help} <- PerModelCounters],
        format_per_model_metrics(PerModel)
    ].

format_per_model_metrics(PerModel) ->
    maps:fold(
        fun(ModelId, M, Acc) ->
            EscapedModelId = prom_escape_label(ModelId),
            [
                Acc,
                format_metric_with_model(
                    <<"emqx_unsgov_model_messages_total">>,
                    EscapedModelId,
                    maps:get(messages_total, M, 0)
                ),
                format_metric_with_model(
                    <<"emqx_unsgov_model_messages_allowed_total">>,
                    EscapedModelId,
                    maps:get(messages_allowed, M, 0)
                ),
                format_metric_with_model(
                    <<"emqx_unsgov_model_messages_dropped_total">>,
                    EscapedModelId,
                    maps:get(messages_dropped, M, 0)
                ),
                format_metric_with_model(
                    <<"emqx_unsgov_model_topic_invalid_total">>,
                    EscapedModelId,
                    maps:get(topic_invalid, M, 0)
                ),
                format_metric_with_model(
                    <<"emqx_unsgov_model_payload_invalid_total">>,
                    EscapedModelId,
                    maps:get(payload_invalid, M, 0)
                ),
                format_metric_with_model(
                    <<"emqx_unsgov_model_not_endpoint_total">>,
                    EscapedModelId,
                    maps:get(not_endpoint, M, 0)
                )
            ]
        end,
        [],
        PerModel
    ).

format_help_and_type(Name, Help, Type) ->
    [
        <<"# HELP ">>,
        Name,
        <<" ">>,
        Help,
        <<"\n">>,
        <<"# TYPE ">>,
        Name,
        <<" ">>,
        Type,
        <<"\n">>
    ].

stat_key(<<"emqx_unsgov_messages_total">>) -> messages_total;
stat_key(<<"emqx_unsgov_messages_allowed_total">>) -> messages_allowed;
stat_key(<<"emqx_unsgov_messages_dropped_total">>) -> messages_dropped;
stat_key(<<"emqx_unsgov_topic_nomatch_total">>) -> topic_nomatch;
stat_key(<<"emqx_unsgov_topic_invalid_total">>) -> topic_invalid;
stat_key(<<"emqx_unsgov_payload_invalid_total">>) -> payload_invalid;
stat_key(<<"emqx_unsgov_not_endpoint_total">>) -> not_endpoint;
stat_key(<<"emqx_unsgov_exempt_total">>) -> exempt;
stat_key(<<"emqx_unsgov_uptime_seconds">>) -> uptime_seconds.

format_metric(Name, Value) ->
    [Name, <<" ">>, integer_to_binary(Value), <<"\n">>].

format_metric_with_model(Name, ModelId, Value) ->
    [
        Name,
        <<"{model_id=\"">>,
        ModelId,
        <<"\"} ">>,
        integer_to_binary(Value),
        <<"\n">>
    ].

prom_escape_label(Bin) when is_binary(Bin) ->
    Bin1 = binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
    binary:replace(Bin1, <<"\"">>, <<"\\\"">>, [global]);
prom_escape_label(Other) ->
    prom_escape_label(iolist_to_binary(io_lib:format("~p", [Other]))).
