%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    pre_config_update/3,
    post_config_update/5
]).

-export([
    load_hook/0,
    unload_hook/0
]).

-export([on_message_publish/1]).

-export([
    load/0,
    unload/0,
    lookup/1,
    lookup/2,
    is_exist_v1/2,
    get_metrics/2,
    create/3,
    disable_enable/3,
    remove/2,
    check_deps_and_remove/3,
    list/0,
    reload_hook/1
]).

-export([
    send_message/2,
    send_message/5
]).

-export([config_key_path/0]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%% Data backup
-export([
    import_config/1,
    %% exported for emqx_bridge_v2
    import_config/4
]).

-export([query_opts/1]).

-define(EGRESS_DIR_BRIDGES(T),
    T == webhook;
    T == mysql;
    T == gcp_pubsub;
    T == influxdb_api_v1;
    T == influxdb_api_v2;
    T == kafka_producer;
    T == redis_single;
    T == redis_sentinel;
    T == redis_cluster;
    T == clickhouse;
    T == pgsql;
    T == timescale;
    T == matrix;
    T == tdengine;
    T == dynamo;
    T == rocketmq;
    T == cassandra;
    T == sqlserver;
    T == pulsar_producer;
    T == oracle;
    T == iotdb;
    T == kinesis_producer;
    T == greptimedb;
    T == azure_event_hub_producer;
    T == syskeeper_forwarder
).

-define(ROOT_KEY, bridges).

load() ->
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, Conf}) ->
                    %% fetch opts for `emqx_resource_buffer_worker`
                    ResOpts = emqx_resource:fetch_creation_opts(Conf),
                    safe_load_bridge(Type, Name, Conf, ResOpts)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(Bridges),
        infinity
    ).

unload() ->
    unload_hook(),
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, _Conf}) ->
                    _ = emqx_bridge_resource:stop(Type, Name)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(Bridges),
        infinity
    ).

safe_load_bridge(Type, Name, Conf, Opts) ->
    try
        _Res = emqx_bridge_resource:create(Type, Name, Conf, Opts),
        ?tp(
            emqx_bridge_loaded,
            #{
                type => Type,
                name => Name,
                res => _Res
            }
        )
    catch
        Err:Reason:ST ->
            ?SLOG(error, #{
                msg => "load_bridge_failed",
                type => Type,
                name => Name,
                error => Err,
                reason => Reason,
                stacktrace => ST
            })
    end.

reload_hook(Bridges) ->
    ok = unload_hook(),
    ok = load_hook(Bridges).

load_hook() ->
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
    load_hook(Bridges).

load_hook(Bridges) ->
    lists:foreach(
        fun({Type, Bridge}) ->
            lists:foreach(
                fun({_Name, BridgeConf}) ->
                    do_load_hook(Type, BridgeConf)
                end,
                maps:to_list(Bridge)
            )
        end,
        maps:to_list(Bridges)
    ).

do_load_hook(Type, #{local_topic := LocalTopic}) when
    ?EGRESS_DIR_BRIDGES(Type) andalso is_binary(LocalTopic)
->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_BRIDGE);
do_load_hook(mqtt, #{egress := #{local := #{topic := _}}}) ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_BRIDGE);
do_load_hook(_Type, _Conf) ->
    ok.

unload_hook() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

on_message_publish(Message = #message{topic = Topic, flags = Flags}) ->
    case maps:get(sys, Flags, false) of
        false ->
            send_to_matched_egress_bridges(Topic, Message);
        true ->
            ok
    end,
    {ok, Message}.

send_to_matched_egress_bridges(Topic, Message) ->
    case get_matched_egress_bridges(Topic) of
        [] ->
            ok;
        Ids ->
            {Msg, _} = emqx_rule_events:eventmsg_publish(Message),
            send_to_matched_egress_bridges_loop(Topic, Msg, Ids)
    end.

send_to_matched_egress_bridges_loop(_Topic, _Msg, []) ->
    ok;
send_to_matched_egress_bridges_loop(Topic, Msg, [Id | Ids]) ->
    try send_message(Id, Msg) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "send_message_to_bridge_failed",
                bridge => Id,
                error => Reason
            });
        _ ->
            ok
    catch
        throw:Reason ->
            ?SLOG(error, #{
                msg => "send_message_to_bridge_exception",
                bridge => Id,
                reason => emqx_utils:redact(Reason)
            });
        Err:Reason:ST ->
            ?SLOG(error, #{
                msg => "send_message_to_bridge_exception",
                bridge => Id,
                error => Err,
                reason => emqx_utils:redact(Reason),
                stacktrace => emqx_utils:redact(ST)
            })
    end,
    send_to_matched_egress_bridges_loop(Topic, Msg, Ids).

send_message(BridgeId, Message) ->
    {BridgeV1Type, BridgeName} = emqx_bridge_resource:parse_bridge_id(BridgeId),
    case emqx_bridge_v2:is_bridge_v2_type(BridgeV1Type) of
        true ->
            ActionType = emqx_action_info:bridge_v1_type_to_action_type(BridgeV1Type),
            emqx_bridge_v2:send_message(ActionType, BridgeName, Message, #{});
        false ->
            ResId = emqx_bridge_resource:resource_id(BridgeV1Type, BridgeName),
            send_message(BridgeV1Type, BridgeName, ResId, Message, #{})
    end.

send_message(BridgeType, BridgeName, ResId, Message, QueryOpts0) ->
    case emqx:get_config([?ROOT_KEY, BridgeType, BridgeName], not_found) of
        not_found ->
            {error, bridge_not_found};
        #{enable := true} = Config ->
            QueryOpts = maps:merge(query_opts(Config), QueryOpts0),
            emqx_resource:query(ResId, {send_message, Message}, QueryOpts);
        #{enable := false} ->
            %% race
            {error, bridge_disabled}
    end.

query_opts(Config) ->
    case emqx_utils_maps:deep_get([resource_opts, request_ttl], Config, false) of
        Timeout when is_integer(Timeout) orelse Timeout =:= infinity ->
            %% request_ttl is configured
            #{timeout => Timeout};
        _ ->
            %% emqx_resource has a default value (15s)
            #{}
    end.

config_key_path() ->
    [?ROOT_KEY].

pre_config_update([?ROOT_KEY], RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update([?ROOT_KEY], NewConf, _RawConf) ->
    case multi_validate_bridge_names(NewConf) of
        ok ->
            {ok, convert_certs(NewConf)};
        Error ->
            Error
    end.

post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    Result = perform_bridge_changes([
        #{action => fun emqx_bridge_resource:remove/4, action_name => remove, data => Removed},
        #{
            action => fun emqx_bridge_resource:create/4,
            action_name => create,
            data => Added,
            on_exception_fn => fun emqx_bridge_resource:remove/4
        },
        #{action => fun emqx_bridge_resource:update/4, action_name => update, data => Updated}
    ]),
    ok = unload_hook(),
    ok = load_hook(NewConf),
    ?tp(bridge_post_config_update_done, #{}),
    Result.

list() ->
    BridgeV1Bridges =
        maps:fold(
            fun(Type, NameAndConf, Bridges) ->
                maps:fold(
                    fun(Name, RawConf, Acc) ->
                        case lookup(Type, Name, RawConf) of
                            {error, not_found} -> Acc;
                            {ok, Res} -> [Res | Acc]
                        end
                    end,
                    Bridges,
                    NameAndConf
                )
            end,
            [],
            emqx:get_raw_config([bridges], #{})
        ),
    BridgeV2Bridges =
        emqx_bridge_v2:bridge_v1_list_and_transform(),
    BridgeV1Bridges ++ BridgeV2Bridges.

lookup(Id) ->
    {Type, Name} = emqx_bridge_resource:parse_bridge_id(Id),
    lookup(Type, Name).

is_exist_v1(Type, Name) ->
    emqx_resource:is_exist(emqx_bridge_resource:resource_id(Type, Name)).

lookup(Type, Name) ->
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        true ->
            emqx_bridge_v2:bridge_v1_lookup_and_transform(Type, Name);
        false ->
            RawConf = emqx:get_raw_config([bridges, Type, Name], #{}),
            lookup(Type, Name, RawConf)
    end.

lookup(Type, Name, RawConf) ->
    case emqx_resource:get_instance(emqx_bridge_resource:resource_id(Type, Name)) of
        {error, not_found} ->
            {error, not_found};
        {ok, _, Data} ->
            {ok, #{
                type => Type,
                name => Name,
                resource_data => Data,
                raw_config => maybe_upgrade(Type, RawConf)
            }}
    end.

get_metrics(ActionType, Name) ->
    case emqx_bridge_v2:is_bridge_v2_type(ActionType) of
        true ->
            case emqx_bridge_v2:bridge_v1_is_valid(ActionType, Name) of
                true ->
                    BridgeV2Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(ActionType),
                    try
                        ConfRootKey = emqx_bridge_v2:get_conf_root_key_if_only_one(
                            BridgeV2Type, Name
                        ),
                        emqx_bridge_v2:get_metrics(ConfRootKey, BridgeV2Type, Name)
                    catch
                        error:Reason ->
                            {error, Reason}
                    end;
                false ->
                    {error, not_bridge_v1_compatible}
            end;
        false ->
            emqx_resource:get_metrics(emqx_bridge_resource:resource_id(ActionType, Name))
    end.

maybe_upgrade(mqtt, Config) ->
    emqx_bridge_compatible_config:maybe_upgrade(Config);
maybe_upgrade(webhook, Config) ->
    emqx_bridge_compatible_config:http_maybe_upgrade(Config);
maybe_upgrade(_Other, Config) ->
    Config.

disable_enable(Action, BridgeType0, BridgeName) when
    Action =:= disable; Action =:= enable
->
    BridgeType = upgrade_type(BridgeType0),
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            emqx_bridge_v2:bridge_v1_enable_disable(Action, BridgeType, BridgeName);
        false ->
            emqx_conf:update(
                config_key_path() ++ [BridgeType, BridgeName],
                {Action, BridgeType, BridgeName},
                #{override_to => cluster}
            )
    end.

create(BridgeV1Type, BridgeName, RawConf) ->
    BridgeType = upgrade_type(BridgeV1Type),
    ?SLOG(debug, #{
        bridge_action => create,
        bridge_type => BridgeType,
        bridge_name => BridgeName,
        bridge_raw_config => emqx_utils:redact(RawConf)
    }),
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            emqx_bridge_v2:bridge_v1_split_config_and_create(BridgeV1Type, BridgeName, RawConf);
        false ->
            emqx_conf:update(
                emqx_bridge:config_key_path() ++ [BridgeType, BridgeName],
                RawConf,
                #{override_to => cluster}
            )
    end.

%% NOTE: This function can cause broken references but it is only called from
%% test cases.
-spec remove(atom() | binary(), binary()) -> ok | {error, any()}.
remove(BridgeType0, BridgeName) ->
    BridgeType = upgrade_type(BridgeType0),
    ?SLOG(debug, #{
        bridge_action => remove,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            emqx_bridge_v2:bridge_v1_remove(BridgeType0, BridgeName);
        false ->
            remove_v1(BridgeType, BridgeName)
    end.

remove_v1(BridgeType0, BridgeName) ->
    BridgeType = upgrade_type(BridgeType0),
    case
        emqx_conf:remove(
            emqx_bridge:config_key_path() ++ [BridgeType, BridgeName],
            #{override_to => cluster}
        )
    of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

check_deps_and_remove(BridgeType0, BridgeName, RemoveDeps) ->
    BridgeType = upgrade_type(BridgeType0),
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            emqx_bridge_v2:bridge_v1_check_deps_and_remove(
                BridgeType,
                BridgeName,
                RemoveDeps
            );
        false ->
            do_check_deps_and_remove(BridgeType, BridgeName, RemoveDeps)
    end.

do_check_deps_and_remove(BridgeType, BridgeName, RemoveDeps) ->
    case emqx_bridge_lib:maybe_withdraw_rule_action(BridgeType, BridgeName, RemoveDeps) of
        ok ->
            remove(BridgeType, BridgeName);
        {error, Reason} ->
            {error, Reason}
    end.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

import_config(RawConf) ->
    import_config(RawConf, <<"bridges">>, ?ROOT_KEY, config_key_path()).

%% Used in emqx_bridge_v2
import_config(RawConf, RawConfKey, RootKey, RootKeyPath) ->
    BridgesConf = maps:get(RawConfKey, RawConf, #{}),
    OldBridgesConf = emqx:get_raw_config(RootKeyPath, #{}),
    MergedConf = merge_confs(OldBridgesConf, BridgesConf),
    case emqx_conf:update(RootKeyPath, MergedConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            {ok, #{root_key => RootKey, changed => changed_paths(OldBridgesConf, NewRawConf)}};
        Error ->
            {error, #{root_key => RootKey, reason => Error}}
    end.

merge_confs(OldConf, NewConf) ->
    AllTypes = maps:keys(maps:merge(OldConf, NewConf)),
    lists:foldr(
        fun(Type, Acc) ->
            NewBridges = maps:get(Type, NewConf, #{}),
            OldBridges = maps:get(Type, OldConf, #{}),
            Acc#{Type => maps:merge(OldBridges, NewBridges)}
        end,
        #{},
        AllTypes
    ).

changed_paths(OldRawConf, NewRawConf) ->
    maps:fold(
        fun(Type, Bridges, ChangedAcc) ->
            OldBridges = maps:get(Type, OldRawConf, #{}),
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(Bridges, OldBridges)),
            [[?ROOT_KEY, Type, K] || K <- maps:keys(Changed)] ++ ChangedAcc
        end,
        [],
        NewRawConf
    ).

%%========================================================================================
%% Helper functions
%%========================================================================================

convert_certs(BridgesConf) ->
    maps:map(
        fun(Type, Bridges) ->
            maps:map(
                fun(Name, BridgeConf) ->
                    Path = filename:join([?ROOT_KEY, Type, Name]),
                    case emqx_connector_ssl:convert_certs(Path, BridgeConf) of
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "bad_ssl_config",
                                type => Type,
                                name => Name,
                                reason => Reason
                            }),
                            throw({bad_ssl_config, Reason});
                        {ok, BridgeConf1} ->
                            BridgeConf1
                    end
                end,
                Bridges
            )
        end,
        BridgesConf
    ).

perform_bridge_changes(Tasks) ->
    perform_bridge_changes(Tasks, []).

perform_bridge_changes([], Errors) ->
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end;
perform_bridge_changes([#{action := Action, data := MapConfs} = Task | Tasks], Errors0) ->
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Results = emqx_utils:pmap(
        fun({{Type, Name}, Conf}) ->
            ResOpts = creation_opts(Conf),
            Res =
                try
                    Action(Type, Name, Conf, ResOpts)
                catch
                    Kind:Error:Stacktrace ->
                        ?SLOG(error, #{
                            msg => "bridge_config_update_exception",
                            kind => Kind,
                            error => Error,
                            type => Type,
                            name => Name,
                            stacktrace => Stacktrace
                        }),
                        OnException(Type, Name, Conf, ResOpts),
                        {error, Error}
                end,
            {{Type, Name}, Res}
        end,
        maps:to_list(MapConfs),
        infinity
    ),
    Errs = lists:filter(
        fun
            ({_TypeName, {error, _}}) -> true;
            (_) -> false
        end,
        Results
    ),
    Errors =
        case Errs of
            [] ->
                Errors0;
            _ ->
                #{action_name := ActionName} = Task,
                [#{action => ActionName, errors => Errs} | Errors0]
        end,
    perform_bridge_changes(Tasks, Errors).

creation_opts({_OldConf, Conf}) ->
    emqx_resource:fetch_creation_opts(Conf);
creation_opts(Conf) ->
    emqx_resource:fetch_creation_opts(Conf).

diff_confs(NewConfs, OldConfs) ->
    emqx_utils_maps:diff_maps(
        flatten_confs(NewConfs),
        flatten_confs(OldConfs)
    ).

flatten_confs(Conf0) ->
    maps:from_list(
        lists:flatmap(
            fun({Type, Conf}) ->
                do_flatten_confs(Type, Conf)
            end,
            maps:to_list(Conf0)
        )
    ).

do_flatten_confs(Type, Conf0) ->
    [{{Type, Name}, Conf} || {Name, Conf} <- maps:to_list(Conf0)].

%% TODO: create a topic index for this
get_matched_egress_bridges(Topic) ->
    Bridges = emqx:get_config([bridges], #{}),
    maps:fold(
        fun(BType, Conf, Acc0) ->
            maps:fold(
                fun
                    (BName, #{egress := _} = BConf, Acc1) when BType =:= mqtt ->
                        get_matched_bridge_id(BType, BConf, Topic, BName, Acc1);
                    (_BName, #{ingress := _}, Acc1) when BType =:= mqtt ->
                        %% ignore ingress only bridge
                        Acc1;
                    (BName, BConf, Acc1) ->
                        get_matched_bridge_id(BType, BConf, Topic, BName, Acc1)
                end,
                Acc0,
                Conf
            )
        end,
        [],
        Bridges
    ).

get_matched_bridge_id(_BType, #{enable := false}, _Topic, _BName, Acc) ->
    Acc;
get_matched_bridge_id(BType, Conf, Topic, BName, Acc) when ?EGRESS_DIR_BRIDGES(BType) ->
    case maps:get(local_topic, Conf, undefined) of
        undefined ->
            Acc;
        Filter ->
            do_get_matched_bridge_id(Topic, Filter, BType, BName, Acc)
    end;
get_matched_bridge_id(mqtt, #{egress := #{local := #{topic := Filter}}}, Topic, BName, Acc) ->
    do_get_matched_bridge_id(Topic, Filter, mqtt, BName, Acc);
get_matched_bridge_id(_BType, _Conf, _Topic, _BName, Acc) ->
    Acc.

do_get_matched_bridge_id(Topic, Filter, BType, BName, Acc) ->
    case emqx_topic:match(Topic, Filter) of
        true -> [emqx_bridge_resource:bridge_id(BType, BName) | Acc];
        false -> Acc
    end.

-spec get_basic_usage_info() ->
    #{
        num_bridges => non_neg_integer(),
        count_by_type =>
            #{BridgeType => non_neg_integer()}
    }
when
    BridgeType :: atom().
get_basic_usage_info() ->
    InitialAcc = #{num_bridges => 0, count_by_type => #{}},
    try
        lists:foldl(
            fun
                (#{resource_data := #{config := #{enable := false}}}, Acc) ->
                    Acc;
                (#{type := BridgeType}, Acc) ->
                    NumBridges = maps:get(num_bridges, Acc),
                    CountByType0 = maps:get(count_by_type, Acc),
                    CountByType = maps:update_with(
                        binary_to_atom(BridgeType, utf8),
                        fun(X) -> X + 1 end,
                        1,
                        CountByType0
                    ),
                    Acc#{
                        num_bridges => NumBridges + 1,
                        count_by_type => CountByType
                    }
            end,
            InitialAcc,
            list()
        )
    catch
        %% for instance, when the bridge app is not ready yet.
        _:_ ->
            InitialAcc
    end.

validate_bridge_name(BridgeName) ->
    try
        _ = emqx_resource:validate_name(to_bin(BridgeName)),
        ok
    catch
        throw:Error ->
            {error, Error}
    end.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(B) when is_binary(B) -> B.

upgrade_type(Type) ->
    emqx_bridge_lib:upgrade_type(Type).

multi_validate_bridge_names(Conf) ->
    BridgeTypeAndNames =
        [
            {Type, Name}
         || {Type, NameToConf} <- maps:to_list(Conf),
            {Name, _Conf} <- maps:to_list(NameToConf)
        ],
    BadBridges =
        lists:filtermap(
            fun({Type, Name}) ->
                case validate_bridge_name(Name) of
                    ok -> false;
                    _Error -> {true, #{type => Type, name => Name}}
                end
            end,
            BridgeTypeAndNames
        ),
    case BadBridges of
        [] ->
            ok;
        [_ | _] ->
            {error, #{
                kind => validation_error,
                reason => bad_bridge_names,
                bad_bridges => BadBridges
            }}
    end.
