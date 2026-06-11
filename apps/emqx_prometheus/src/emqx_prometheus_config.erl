%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_config).

-behaviour(emqx_config_handler).

-include("emqx_prometheus.hrl").

-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).
-export([update/1]).
-export([register_collectors/0]).
-export([conf/0, is_push_gateway_server_enabled/1]).
-export([to_recommend_type/1]).
-export([mria_lag_refresh_interval/0]).

update(Config) ->
    case
        emqx_conf:update(
            [prometheus],
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
    ok = emqx_config_handler:add_handler(?PROMETHEUS, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?PROMETHEUS),
    ok.

mria_lag_refresh_interval() ->
    emqx_config:get([prometheus, mria_lag_refresh_interval], 10_000).

%% when we import the config with the old version
%% we need to respect it, and convert to new schema.
pre_config_update(?PROMETHEUS, MergeConf, OriginConf) ->
    OriginType = emqx_prometheus_schema:is_recommend_type(OriginConf),
    MergeType = emqx_prometheus_schema:is_recommend_type(MergeConf),
    {ok,
        case {OriginType, MergeType} of
            {true, false} -> to_recommend_type(MergeConf);
            _ -> MergeConf
        end}.

to_recommend_type(Conf) ->
    #{
        <<"push_gateway">> => to_push_gateway(Conf),
        <<"collectors">> => to_collectors(Conf),
        <<"enable_basic_auth">> => false
    }.

to_push_gateway(Conf) ->
    Init = maps:with([<<"interval">>, <<"headers">>, <<"job_name">>, <<"enable">>], Conf),
    case maps:get(<<"push_gateway_server">>, Conf, "") of
        "" ->
            Init#{<<"enable">> => false};
        Url ->
            Init#{<<"url">> => Url}
    end.

to_collectors(Conf) ->
    lists:foldl(
        fun({From, To}, Acc) ->
            case maps:find(From, Conf) of
                {ok, Value} -> Acc#{To => Value};
                error -> Acc
            end
        end,
        #{},
        [
            {<<"vm_dist_collector">>, <<"vm_dist">>},
            {<<"mnesia_collector">>, <<"mnesia">>},
            {<<"vm_statistics_collector">>, <<"vm_statistics">>},
            {<<"vm_system_info_collector">>, <<"vm_system_info">>},
            {<<"vm_memory_collector">>, <<"vm_memory">>},
            {<<"vm_msacc_collector">>, <<"vm_msacc">>}
        ]
    ).

post_config_update(?PROMETHEUS, _Req, New, Old, _AppEnvs) ->
    _ = register_collectors(New),
    _ = update_push_gateway(New),
    ok = emqx_prometheus_auth:update_latency_metrics(New),
    update_auth(New, Old);
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

register_collectors() ->
    register_collectors(conf()).

register_collectors(Conf) ->
    lists:foreach(
        fun(Registry) ->
            register_collectors(Registry, collectors(Registry, Conf))
        end,
        ?PROMETHEUS_ALL_REGISTRIES
    ).

register_collectors(Registry, Collectors) ->
    PrevCollectors = prometheus_registry:collectors(Registry),
    lists:foreach(
        fun(Collector) ->
            prometheus_registry:deregister_collector(Registry, Collector)
        end,
        PrevCollectors -- Collectors
    ),
    lists:foreach(
        fun(Collector) ->
            prometheus_registry:register_collector(Registry, Collector)
        end,
        Collectors -- PrevCollectors
    ).

collectors(?PROMETHEUS_DEFAULT_REGISTRY, Prometheus) ->
    default_collectors(Prometheus);
collectors(Registry, _Prometheus) ->
    proplists:get_value(Registry, named_collectors(), []).

default_collectors(Prometheus) ->
    [emqx_prometheus] ++ static_collectors() ++ vm_collectors(Prometheus).

static_collectors() ->
    [
        prometheus_boolean,
        prometheus_counter,
        prometheus_gauge,
        prometheus_histogram,
        prometheus_quantile_summary,
        prometheus_summary
    ].

vm_collectors(#{collectors := Collectors}) ->
    enabled_vm_collectors(Collectors);
vm_collectors(Prometheus) ->
    enabled_vm_collectors(legacy_collectors(Prometheus)).

enabled_vm_collectors(Collectors) ->
    [
        Collector
     || {Key, Collector} <- vm_collector_specs(),
        is_collector_enabled(Key, Collectors)
    ].

legacy_collectors(Prometheus) ->
    maps:from_list(
        [
            {Key, maps:get(LegacyKey, Prometheus, disabled)}
         || {LegacyKey, Key} <- legacy_collector_keys()
        ]
    ).

legacy_collector_keys() ->
    [
        {vm_dist_collector, vm_dist},
        {mnesia_collector, mnesia},
        {vm_statistics_collector, vm_statistics},
        {vm_system_info_collector, vm_system_info},
        {vm_memory_collector, vm_memory},
        {vm_msacc_collector, vm_msacc}
    ].

vm_collector_specs() ->
    [
        {vm_dist, prometheus_vm_dist_collector},
        {mnesia, prometheus_mnesia_collector},
        {vm_statistics, prometheus_vm_statistics_collector},
        {vm_system_info, prometheus_vm_system_info_collector},
        {vm_memory, prometheus_vm_memory_collector},
        {vm_msacc, prometheus_vm_msacc_collector}
    ].

is_collector_enabled(Key, Collectors) ->
    maps:get(Key, Collectors, maps:get(atom_to_binary(Key), Collectors, disabled)) =:= enabled.

named_collectors() ->
    [
        {?PROMETHEUS_AUTH_REGISTRY, [?PROMETHEUS_AUTH_COLLECTOR]},
        {?PROMETHEUS_DATA_INTEGRATION_REGISTRY, [?PROMETHEUS_DATA_INTEGRATION_COLLECTOR]},
        {?PROMETHEUS_SCHEMA_VALIDATION_REGISTRY, [?PROMETHEUS_SCHEMA_VALIDATION_COLLECTOR]},
        {?PROMETHEUS_MESSAGE_TRANSFORMATION_REGISTRY, [
            ?PROMETHEUS_MESSAGE_TRANSFORMATION_COLLECTOR
        ]}
    ].

update_push_gateway(Prometheus) ->
    case is_push_gateway_server_enabled(Prometheus) of
        true ->
            case erlang:whereis(?APP) of
                undefined -> emqx_prometheus_sup:start_child(?APP, Prometheus);
                Pid -> emqx_prometheus_sup:update_child(Pid, Prometheus)
            end;
        false ->
            emqx_prometheus_sup:stop_child(?APP)
    end.

update_auth(#{enable_basic_auth := New}, #{enable_basic_auth := Old}) when New =/= Old ->
    emqx_dashboard:regenerate_dispatch_after_config_update(),
    ok;
update_auth(_, _) ->
    ok.

conf() ->
    emqx_config:get(?PROMETHEUS).

is_push_gateway_server_enabled(#{enable := true, push_gateway_server := Url}) ->
    Url =/= "";
is_push_gateway_server_enabled(#{push_gateway := #{url := Url, enable := Enable}}) ->
    Enable andalso Url =/= "";
is_push_gateway_server_enabled(_) ->
    false.
