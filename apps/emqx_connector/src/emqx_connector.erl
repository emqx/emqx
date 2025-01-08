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
-module(emqx_connector).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    pre_config_update/3,
    post_config_update/5
]).

-export([
    create/3,
    disable_enable/3,
    get_metrics/2,
    list/0,
    load/0,
    is_exist/2,
    lookup/1,
    lookup/2,
    remove/2,
    unload/0,
    update/3
]).

-export([config_key_path/0]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%% Data backup
-export([
    import_config/1
]).

-define(ROOT_KEY, connectors).
-define(ENABLE_OR_DISABLE(A), (A =:= disable orelse A =:= enable)).

load() ->
    Connectors = emqx:get_config([?ROOT_KEY], #{}),
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, Conf}) ->
                    safe_load_connector(Type, Name, Conf)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(Connectors),
        infinity
    ).

unload() ->
    Connectors = emqx:get_config([?ROOT_KEY], #{}),
    emqx_utils:pforeach(
        fun({Type, NamedConf}) ->
            emqx_utils:pforeach(
                fun({Name, _Conf}) ->
                    _ = emqx_connector_resource:stop(Type, Name)
                end,
                maps:to_list(NamedConf),
                infinity
            )
        end,
        maps:to_list(Connectors),
        infinity
    ).

safe_load_connector(Type, Name, Conf) ->
    try
        _Res = emqx_connector_resource:create(Type, Name, Conf),
        ?tp(
            emqx_connector_loaded,
            #{
                type => Type,
                name => Name,
                res => _Res
            }
        )
    catch
        Err:Reason:ST ->
            ?SLOG(error, #{
                msg => "load_connector_failed",
                type => Type,
                name => Name,
                error => Err,
                reason => Reason,
                stacktrace => ST
            })
    end.

config_key_path() ->
    [?ROOT_KEY].

pre_config_update([?ROOT_KEY], RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update([?ROOT_KEY], NewConf, _RawConf) ->
    case multi_validate_connector_names(NewConf) of
        ok -> {ok, convert_certs(NewConf)};
        Error -> Error
    end;
pre_config_update([?ROOT_KEY, _Type, _Name], Oper, undefined) when
    ?ENABLE_OR_DISABLE(Oper)
->
    {error, connector_not_found};
pre_config_update([?ROOT_KEY, _Type, _Name], Oper, OldConfig) when
    ?ENABLE_OR_DISABLE(Oper)
->
    %% to save the 'enable' to the config files
    {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update([?ROOT_KEY, _Type, Name] = Path, Conf = #{}, ConfOld) ->
    case validate_connector_name(Name) of
        ok ->
            case emqx_connector_ssl:convert_certs(filename:join(Path), Conf) of
                {ok, ConfNew} ->
                    connector_pre_config_update(Path, ConfNew, ConfOld);
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            Error
    end.

connector_pre_config_update([?ROOT_KEY, Type, Name] = Path, ConfNew, ConfOld) ->
    Mod = emqx_connector_info:config_transform_module(Type),
    case Mod =/= undefined andalso erlang:function_exported(Mod, pre_config_update, 4) of
        true ->
            apply(Mod, pre_config_update, [Path, Name, ConfNew, ConfOld]);
        false ->
            {ok, ConfNew}
    end.

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} = diff_confs(NewConf, OldConf),
    case ensure_no_channels(Removed) of
        ok -> perform_connector_changes(Removed, Added, Updated);
        {error, Error} -> {error, Error}
    end;
post_config_update([?ROOT_KEY, Type, Name], '$remove', _, _OldConf, _AppEnvs) ->
    case emqx_connector_resource:get_channels(Type, Name) of
        {ok, []} ->
            ok = emqx_connector_resource:remove(Type, Name),
            ?tp(connector_post_config_update_done, #{}),
            ok;
        {error, not_found} ->
            ?tp(connector_post_config_update_done, #{}),
            ok;
        {ok, Channels} ->
            {error, {active_channels, Channels}}
    end;
%% create a new connector
post_config_update([?ROOT_KEY, Type, Name], _Req, NewConf, undefined, _AppEnvs) ->
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_connector_resource:create(Type, Name, NewConf, ResOpts),
    ?tp(connector_post_config_update_done, #{}),
    ok;
%% update an existing connector
post_config_update([?ROOT_KEY, Type, Name], _Req, NewConf, OldConf, _AppEnvs) ->
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_connector_resource:update(Type, Name, {OldConf, NewConf}, ResOpts),
    ?tp(connector_post_config_update_done, #{}),
    ok.

perform_connector_changes(Removed, Added, Updated) ->
    Result = perform_connector_changes([
        #{action => fun emqx_connector_resource:remove/4, action_name => remove, data => Removed},
        #{
            action => fun emqx_connector_resource:create/4,
            action_name => create,
            data => Added,
            on_exception_fn => fun emqx_connector_resource:remove/4
        },
        #{action => fun emqx_connector_resource:update/4, action_name => update, data => Updated}
    ]),
    ?tp(connector_post_config_update_done, #{}),
    Result.

list() ->
    maps:fold(
        fun(Type, NameAndConf, Connectors) ->
            maps:fold(
                fun(Name, RawConf, Acc) ->
                    case lookup(Type, Name, RawConf) of
                        {error, not_found} -> Acc;
                        {ok, Res} -> [Res | Acc]
                    end
                end,
                Connectors,
                NameAndConf
            )
        end,
        [],
        emqx:get_raw_config([connectors], #{})
    ).

lookup(Id) ->
    {Type, Name} = emqx_connector_resource:parse_connector_id(Id),
    lookup(Type, Name).

lookup(Type, Name) ->
    RawConf = emqx:get_raw_config([connectors, Type, Name], #{}),
    lookup(Type, Name, RawConf).

lookup(Type, Name, RawConf) ->
    case emqx_resource:get_instance(emqx_connector_resource:resource_id(Type, Name)) of
        {error, not_found} ->
            {error, not_found};
        {ok, _, Data} ->
            {ok, #{
                type => Type,
                name => Name,
                resource_data => Data,
                raw_config => RawConf
            }}
    end.

is_exist(Type, Name) ->
    emqx_resource:is_exist(emqx_connector_resource:resource_id(Type, Name)).

get_metrics(Type, Name) ->
    emqx_resource:get_metrics(emqx_connector_resource:resource_id(Type, Name)).

disable_enable(Action, ConnectorType, ConnectorName) when ?ENABLE_OR_DISABLE(Action) ->
    emqx_conf:update(
        config_key_path() ++ [ConnectorType, ConnectorName],
        Action,
        #{override_to => cluster}
    ).

create(ConnectorType, ConnectorName, RawConf) ->
    ?SLOG(debug, #{
        connector_action => create,
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        connector_raw_config => emqx_utils:redact(RawConf)
    }),
    emqx_conf:update(
        emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
        RawConf,
        #{override_to => cluster}
    ).

remove(ConnectorType, ConnectorName) ->
    ?SLOG(debug, #{
        bridge_action => remove,
        connector_type => ConnectorType,
        connector_name => ConnectorName
    }),
    case
        emqx_conf:remove(
            emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
            #{override_to => cluster}
        )
    of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

update(ConnectorType, ConnectorName, RawConf) ->
    ?SLOG(debug, #{
        connector_action => update,
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        connector_raw_config => emqx_utils:redact(RawConf)
    }),
    case lookup(ConnectorType, ConnectorName) of
        {ok, _Conf} ->
            emqx_conf:update(
                emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
                RawConf,
                #{override_to => cluster}
            );
        Error ->
            Error
    end.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

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

convert_certs(ConnectorsConf) ->
    maps:map(
        fun(Type, Connectors) ->
            maps:map(
                fun(Name, ConnectorConf) ->
                    Path = filename:join([?ROOT_KEY, Type, Name]),
                    case emqx_connector_ssl:convert_certs(Path, ConnectorConf) of
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

perform_connector_changes(Tasks) ->
    perform_connector_changes(Tasks, []).

perform_connector_changes([], Errors) ->
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end;
perform_connector_changes([#{action := Action, data := MapConfs} = Task | Tasks], Errors0) ->
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
                            msg => "connector_config_update_exception",
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
    perform_connector_changes(Tasks, Errors).

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
            list()
        )
    catch
        %% for instance, when the connector app is not ready yet.
        _:_ ->
            InitialAcc
    end.

ensure_no_channels(Configs) ->
    Pipeline =
        lists:map(
            fun({Type, ConnectorName}) ->
                fun(_) ->
                    case emqx_connector_resource:get_channels(Type, ConnectorName) of
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
