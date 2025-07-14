%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    pre_config_update/4,
    post_config_update/6
]).

-export([
    create/4,
    disable_enable/4,
    get_metrics/3,
    list/0,
    list/1,
    load/0,
    is_exist/3,
    lookup/3,
    remove/3,
    unload/0
]).

-export([config_key_path/0]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%% Data backup
-export([
    import_config/1
]).

%% Deprecated RPC target (`emqx_connector_proto_v1`).
-deprecated({list, 0, "use list/1 instead"}).

-define(ROOT_KEY, connectors).
-define(ENABLE_OR_DISABLE(A), (A =:= disable orelse A =:= enable)).

load() ->
    GlobalConnectors = emqx:get_config([?ROOT_KEY], #{}),
    do_load(?global_ns, GlobalConnectors),
    NamespacedConnectors = emqx_config:get_root_from_all_namespaces(?ROOT_KEY),
    emqx_utils:pforeach(
        fun({Namespace, ConnectorsRoot}) ->
            do_load(Namespace, ConnectorsRoot)
        end,
        maps:to_list(NamespacedConnectors),
        infinity
    ).

do_load(Namespace, ConnectorsRoot) ->
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, Conf}) ->
                    safe_load_connector(Namespace, Type, Name, Conf)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(ConnectorsRoot),
        infinity
    ).

unload() ->
    GlobalConnectors = emqx:get_config([?ROOT_KEY], #{}),
    do_unload(?global_ns, GlobalConnectors),
    NamespacedConnectors = emqx_config:get_root_from_all_namespaces(?ROOT_KEY),
    emqx_utils:pforeach(
        fun({Namespace, ConnectorsRoot}) ->
            do_unload(Namespace, ConnectorsRoot)
        end,
        maps:to_list(NamespacedConnectors),
        infinity
    ).

do_unload(Namespace, ConnectorsRoot) ->
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, _Conf}) ->
                    _ = emqx_connector_resource:stop(Namespace, Type, Name)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(ConnectorsRoot),
        infinity
    ).

safe_load_connector(Namespace, Type, Name, Conf) ->
    try
        Opts = #{async_start => true},
        _Res = emqx_connector_resource:create(Namespace, Type, Name, Conf, Opts),
        ?tp(
            emqx_connector_loaded,
            #{
                type => Type,
                name => Name,
                namespace => Namespace,
                res => _Res
            }
        )
    catch
        Err:Reason:ST ->
            ?SLOG(error, #{
                msg => "load_connector_failed",
                type => Type,
                name => Name,
                namespace => Namespace,
                error => Err,
                reason => Reason,
                stacktrace => ST
            })
    end.

config_key_path() ->
    [?ROOT_KEY].

pre_config_update([?ROOT_KEY], {async_start, NewConf}, RawConf, ExtraContext) ->
    pre_config_update([?ROOT_KEY], NewConf, RawConf, ExtraContext);
pre_config_update([?ROOT_KEY], RawConf, RawConf, _ExtraContext) ->
    {ok, RawConf};
pre_config_update([?ROOT_KEY], NewConf, _RawConf, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    case multi_validate_connector_names(NewConf) of
        ok -> {ok, convert_certs(Namespace, NewConf)};
        Error -> Error
    end;
pre_config_update([?ROOT_KEY, _Type, _Name], Oper, undefined, _ExtraContext) when
    ?ENABLE_OR_DISABLE(Oper)
->
    {error, connector_not_found};
pre_config_update([?ROOT_KEY, _Type, _Name], Oper, OldConfig, _ExtraContext) when
    ?ENABLE_OR_DISABLE(Oper)
->
    %% to save the 'enable' to the config files
    {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update([?ROOT_KEY, Type, Name] = KeyPath, Conf = #{}, ConfOld, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    case validate_connector_name(Name) of
        ok ->
            CertDir = ssl_cert_dir(Namespace, Type, Name),
            case emqx_connector_ssl:convert_certs(CertDir, Conf) of
                {ok, ConfNew} ->
                    connector_pre_config_update(KeyPath, ConfNew, ConfOld);
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            Error
    end.

connector_pre_config_update([?ROOT_KEY, Type, Name] = KeyPath, ConfNew, ConfOld) ->
    Mod = emqx_connector_info:config_transform_module(Type),
    case Mod =/= undefined andalso erlang:function_exported(Mod, pre_config_update, 4) of
        true ->
            apply(Mod, pre_config_update, [KeyPath, Name, ConfNew, ConfOld]);
        false ->
            {ok, ConfNew}
    end.

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

post_config_update([?ROOT_KEY], Req, NewConf, OldConf, _AppEnv, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    #{added := Added, removed := Removed, changed := Updated} = diff_confs(NewConf, OldConf),
    Opts =
        case Req of
            {async_start, _} ->
                #{async_start => true};
            _ ->
                #{}
        end,
    case ensure_no_channels(Removed, Namespace) of
        ok ->
            perform_connector_changes(Removed, Added, Updated, Namespace, Opts);
        {error, Error} ->
            {error, Error}
    end;
post_config_update([?ROOT_KEY, Type, Name], '$remove', _, _OldConf, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    case emqx_connector_resource:get_channels(Namespace, Type, Name) of
        {ok, []} ->
            ok = emqx_connector_resource:remove(Namespace, Type, Name),
            ?tp(connector_post_config_update_done, #{}),
            ok;
        {error, not_found} ->
            ?tp(connector_post_config_update_done, #{}),
            ok;
        {ok, Channels} ->
            {error, {active_channels, Channels}}
    end;
%% create a new connector
post_config_update([?ROOT_KEY, Type, Name], _Req, NewConf, undefined, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_connector_resource:create(Namespace, Type, Name, NewConf, ResOpts),
    ?tp(connector_post_config_update_done, #{}),
    ok;
%% update an existing connector
post_config_update([?ROOT_KEY, Type, Name], _Req, NewConf, OldConf, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_connector_resource:update(Namespace, Type, Name, {OldConf, NewConf}, ResOpts),
    ?tp(connector_post_config_update_done, #{}),
    ok.

perform_connector_changes(Removed, Added, Updated, Namespace, Opts) ->
    Result = do_perform_connector_changes(
        [
            #{
                action => fun(Namespace1, Type, Name, _Conf, _Opts) ->
                    emqx_connector_resource:remove(Namespace1, Type, Name)
                end,
                action_name => remove,
                namespace => Namespace,
                data => Removed
            },
            #{
                action => fun emqx_connector_resource:create/5,
                action_name => create,
                namespace => Namespace,
                data => Added,
                on_exception_fn => fun(Namespace1, Type, Name, _Conf, _Opts) ->
                    emqx_connector_resource:remove(Namespace1, Type, Name)
                end
            },
            #{
                action => fun emqx_connector_resource:update/5,
                action_name => update,
                namespace => Namespace,
                data => Updated
            }
        ],
        Opts
    ),
    ?tp(connector_post_config_update_done, #{}),
    Result.

%% Deprecated RPC target (`emqx_connector_proto_v1`).
list() ->
    list(?global_ns).

list(Namespace) ->
    maps:fold(
        fun(Type, NameAndConf, Connectors) ->
            maps:fold(
                fun(Name, RawConf, Acc) ->
                    case do_lookup(Namespace, Type, Name, RawConf) of
                        {error, not_found} -> Acc;
                        {ok, Res} -> [Res | Acc]
                    end
                end,
                Connectors,
                NameAndConf
            )
        end,
        [],
        get_raw_config(Namespace, [connectors], #{})
    ).

lookup(Namespace, Type, Name) ->
    RawConf = get_raw_config(Namespace, [connectors, Type, Name], #{}),
    do_lookup(Namespace, Type, Name, RawConf).

do_lookup(Namespace, Type, Name, RawConf) ->
    ConnResId = emqx_connector_resource:resource_id(Namespace, Type, Name),
    case emqx_resource:get_instance(ConnResId) of
        {error, not_found} ->
            {error, not_found};
        {ok, _, Data} ->
            {ok, #{
                type => atom(Type),
                name => atom(Name),
                resource_data => Data,
                raw_config => RawConf
            }}
    end.

is_exist(Namespace, Type, Name) ->
    ConnResId = emqx_connector_resource:resource_id(Namespace, Type, Name),
    emqx_resource:is_exist(ConnResId).

get_metrics(Namespace, Type, Name) ->
    ConnResId = emqx_connector_resource:resource_id(Namespace, Type, Name),
    emqx_resource:get_metrics(ConnResId).

disable_enable(Namespace, Action, ConnectorType, ConnectorName) when ?ENABLE_OR_DISABLE(Action) ->
    emqx_conf:update(
        config_key_path() ++ [ConnectorType, ConnectorName],
        Action,
        with_namespace(#{override_to => cluster}, Namespace)
    ).

create(Namespace, ConnectorType, ConnectorName, RawConf) ->
    ?SLOG(debug, #{
        connector_action => create,
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        connector_raw_config => emqx_utils:redact(RawConf)
    }),
    emqx_conf:update(
        emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
        RawConf,
        with_namespace(#{override_to => cluster}, Namespace)
    ).

remove(Namespace, ConnectorType, ConnectorName) ->
    ?SLOG(debug, #{
        bridge_action => remove,
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        namespace => Namespace
    }),
    case
        emqx_conf:remove(
            emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
            with_namespace(#{override_to => cluster}, Namespace)
        )
    of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

%% TODO: namespace
import_config(RawConf) ->
    RootKeyPath = config_key_path(),
    ConnectorsConf = maps:get(<<"connectors">>, RawConf, #{}),
    OldConnectorsConf = emqx:get_raw_config(RootKeyPath, #{}),
    MergedConf = merge_confs(OldConnectorsConf, ConnectorsConf),
    %% using merge strategy, deletions should not be performed within the post_config_update/5.
    case emqx_conf:update(RootKeyPath, MergedConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            {ok, #{root_key => ?ROOT_KEY, changed => changed_paths(OldConnectorsConf, NewRawConf)}};
        Error ->
            {error, #{root_key => ?ROOT_KEY, reason => Error}}
    end.

merge_confs(OldConf, NewConf) ->
    AllTypes = maps:keys(maps:merge(OldConf, NewConf)),
    lists:foldr(
        fun(Type, Acc) ->
            NewConnectors = maps:get(Type, NewConf, #{}),
            OldConnectors = maps:get(Type, OldConf, #{}),
            Acc#{Type => maps:merge(OldConnectors, NewConnectors)}
        end,
        #{},
        AllTypes
    ).

changed_paths(OldRawConf, NewRawConf) ->
    maps:fold(
        fun(Type, Connectors, ChangedAcc) ->
            OldConnectors = maps:get(Type, OldRawConf, #{}),
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(Connectors, OldConnectors)),
            [[?ROOT_KEY, Type, K] || K <- maps:keys(Changed)] ++ ChangedAcc
        end,
        [],
        NewRawConf
    ).

%%========================================================================================
%% Helper functions
%%========================================================================================

convert_certs(Namespace, ConnectorsConf) ->
    maps:map(
        fun(Type, Connectors) ->
            maps:map(
                fun(Name, ConnectorConf) ->
                    CertDir = ssl_cert_dir(Namespace, Type, Name),
                    case emqx_connector_ssl:convert_certs(CertDir, ConnectorConf) of
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "bad_ssl_config",
                                type => Type,
                                name => Name,
                                reason => Reason
                            }),
                            throw(Reason);
                        {ok, ConnectorConf1} ->
                            ConnectorConf1
                    end
                end,
                Connectors
            )
        end,
        ConnectorsConf
    ).

do_perform_connector_changes(Tasks, Opts) ->
    do_perform_connector_changes(Tasks, [], Opts).

do_perform_connector_changes([], Errors, _Opts) ->
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end;
do_perform_connector_changes([#{action := Action, data := MapConfs} = Task | Tasks], Errors0, Opts) ->
    Namespace = maps:get(namespace, Task),
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Results = emqx_utils:pmap(
        fun({{Type, Name}, Conf}) ->
            ResOpts0 = creation_opts(Conf),
            ResOpts = maps:merge(ResOpts0, Opts),
            Res =
                try
                    Action(Namespace, Type, Name, Conf, ResOpts)
                catch
                    Kind:Error:Stacktrace ->
                        ?SLOG(error, #{
                            msg => "connector_config_update_exception",
                            kind => Kind,
                            error => Error,
                            type => Type,
                            name => Name,
                            namespace => Namespace,
                            stacktrace => Stacktrace
                        }),
                        OnException(Namespace, Type, Name, Conf, ResOpts),
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
    do_perform_connector_changes(Tasks, Errors, Opts).

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

-spec get_basic_usage_info() ->
    #{
        num_connectors => non_neg_integer(),
        count_by_type =>
            #{ConnectorType => non_neg_integer()}
    }
when
    ConnectorType :: atom().
get_basic_usage_info() ->
    InitialAcc = #{num_connectors => 0, count_by_type => #{}},
    try
        lists:foldl(
            fun
                (#{resource_data := #{config := #{enable := false}}}, Acc) ->
                    Acc;
                (#{type := ConnectorType}, Acc) ->
                    NumConnectors = maps:get(num_connectors, Acc),
                    CountByType0 = maps:get(count_by_type, Acc),
                    CountByType = maps:update_with(
                        binary_to_atom(ConnectorType, utf8),
                        fun(X) -> X + 1 end,
                        1,
                        CountByType0
                    ),
                    Acc#{
                        num_connectors => NumConnectors + 1,
                        count_by_type => CountByType
                    }
            end,
            InitialAcc,
            list(?global_ns)
        )
    catch
        %% for instance, when the connector app is not ready yet.
        _:_ ->
            InitialAcc
    end.

ensure_no_channels(Configs, Namespace) ->
    Pipeline =
        lists:map(
            fun({Type, ConnectorName}) ->
                fun(_) ->
                    case emqx_connector_resource:get_channels(Namespace, Type, ConnectorName) of
                        {error, not_found} ->
                            ok;
                        {ok, []} ->
                            ok;
                        {ok, Channels} ->
                            {error, #{
                                reason => "connector_has_active_channels",
                                type => Type,
                                connector_name => ConnectorName,
                                active_channels => Channels
                            }}
                    end
                end
            end,
            maps:keys(Configs)
        ),
    case emqx_utils:pipeline(Pipeline, unused, unused) of
        {ok, _, _} ->
            ok;
        {error, Reason, _State} ->
            {error, Reason}
    end.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(B) when is_binary(B) -> B.

validate_connector_name(ConnectorName) ->
    try
        _ = emqx_resource:validate_name(to_bin(ConnectorName)),
        ok
    catch
        throw:Error ->
            {error, Error}
    end.

multi_validate_connector_names(Conf) ->
    ConnectorTypeAndNames =
        [
            {Type, Name}
         || {Type, NameToConf} <- maps:to_list(Conf),
            {Name, _Conf} <- maps:to_list(NameToConf)
        ],
    BadConnectors =
        lists:filtermap(
            fun({Type, Name}) ->
                case validate_connector_name(Name) of
                    ok -> false;
                    _Error -> {true, #{type => Type, name => Name}}
                end
            end,
            ConnectorTypeAndNames
        ),
    case BadConnectors of
        [] ->
            ok;
        [_ | _] ->
            {error, #{
                kind => validation_error,
                reason => bad_connector_names,
                bad_connectors => BadConnectors
            }}
    end.

ssl_cert_dir(Namespace, Type, Name) when is_binary(Namespace) ->
    filename:join([Namespace, ?ROOT_KEY, Type, Name]);
ssl_cert_dir(?global_ns, Type, Name) ->
    filename:join([?ROOT_KEY, Type, Name]).

get_raw_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_raw_config({Namespace, KeyPath}, Default);
get_raw_config(?global_ns, KeyPath, Default) ->
    emqx:get_raw_config(KeyPath, Default).

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.

atom(B) when is_binary(B) -> binary_to_existing_atom(B, utf8);
atom(A) when is_atom(A) -> A.
