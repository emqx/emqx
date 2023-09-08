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
-module(emqx_bridge_v2).

-behaviour(emqx_config_handler).
% -behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    load/0,
    unload/0,
    is_bridge_v2_type/1,
    id/2,
    id/3,
    parse_id/1,
    send_message/4,
    bridge_v2_type_to_connector_type/1,
    is_bridge_v2_id/1,
    extract_connector_id_from_bridge_v2_id/1,
    is_bridge_v2_installed_in_connector_state/2,
    get_channels_for_connector/1
]).

%% CRUD API

-export([
    list/0,
    lookup/1,
    lookup/2,
    get_metrics/2,
    config_key_path/0,
    disable_enable/3,
    create/3,
    remove/2,
    health_check/2
]).

%% Config Update Handler API

-export([
    post_config_update/5
]).

-define(ROOT_KEY, bridges_v2).

get_channels_for_connector(ConnectorId) ->
    {ConnectorType, ConnectorName} = emqx_connector_resource:parse_connector_id(ConnectorId),
    RootConf = maps:keys(emqx:get_config([?ROOT_KEY], #{})),
    RelevantBridgeV2Types = [
        Type
     || Type <- RootConf,
        bridge_v2_type_to_connector_type(Type) =:= ConnectorType
    ],
    lists:flatten([
        get_channels_for_connector(ConnectorName, BridgeV2Type)
     || BridgeV2Type <- RelevantBridgeV2Types
    ]).

get_channels_for_connector(ConnectorName, BridgeV2Type) ->
    BridgeV2s = emqx:get_config([?ROOT_KEY, BridgeV2Type], #{}),
    [
        {id(BridgeV2Type, Name, ConnectorName), Conf}
     || {Name, Conf} <- maps:to_list(BridgeV2s),
        bin(ConnectorName) =:= maps:get(connector, Conf, no_name)
    ].

load() ->
    % Bridge_V2s = emqx:get_config([?ROOT_KEY], #{}),
    % lists:foreach(
    %     fun({Type, NamedConf}) ->
    %         lists:foreach(
    %             fun({Name, Conf}) ->
    %                 install_bridge_v2(
    %                     Type,
    %                     Name,
    %                     Conf
    %                 )
    %             end,
    %             maps:to_list(NamedConf)
    %         )
    %     end,
    %     maps:to_list(Bridge_V2s)
    % ),
    ok.

unload() ->
    % Bridge_V2s = emqx:get_config([?ROOT_KEY], #{}),
    % lists:foreach(
    %     fun({Type, NamedConf}) ->
    %         lists:foreach(
    %             fun({Name, Conf}) ->
    %                 uninstall_bridge_v2(
    %                     Type,
    %                     Name,
    %                     Conf
    %                 )
    %             end,
    %             maps:to_list(NamedConf)
    %         )
    %     end,
    %     maps:to_list(Bridge_V2s)
    % ),
    ok.

install_bridge_v2(
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    ok;
install_bridge_v2(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    %% Create metrics for Bridge V2
    ok = emqx_resource:create_metrics(BridgeV2Id),
    %% We might need to create buffer workers for Bridge V2
    case get_query_mode(BridgeV2Type, Config) of
        %% the Bridge V2 has built-in buffer, so there is no need for resource workers
        simple_sync ->
            ok;
        simple_async ->
            ok;
        %% The Bridge V2 is a consumer Bridge V2, so there is no need for resource workers
        no_queries ->
            ok;
        _ ->
            %% start resource workers as the query type requires them
            ok = emqx_resource_buffer_worker_sup:start_workers(BridgeV2Id, CreationOpts)
    end,
    %% If there is a running connector, we need to install the Bridge V2 in it
    ConnectorId = emqx_connector_resource:resource_id(
        bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
    ),
    emqx_resource_manager:add_channel(ConnectorId, BridgeV2Id, Config),
    ok.

uninstall_bridge_v2(
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    %% Already not installed
    ok;
uninstall_bridge_v2(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ok = emqx_resource_buffer_worker_sup:stop_workers(BridgeV2Id, CreationOpts),
    ok = emqx_resource:clear_metrics(BridgeV2Id),
    %% Deinstall from connector
    ConnectorId = emqx_connector_resource:resource_id(
        bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
    ),
    emqx_resource_manager:remove_channel(ConnectorId, BridgeV2Id).

get_query_mode(BridgeV2Type, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ResourceType = emqx_bridge_resource:bridge_to_resource_type(BridgeV2Type),
    emqx_resource:query_mode(ResourceType, Config, CreationOpts).

send_message(BridgeType, BridgeName, Message, QueryOpts0) ->
    case lookup(BridgeType, BridgeName) of
        #{enable := true} = Config ->
            do_send_msg_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config);
        #{enable := false} ->
            {error, bridge_stopped};
        Error ->
            Error
    end.

health_check(BridgeType, BridgeName) ->
    case lookup(BridgeType, BridgeName) of
        #{
            enable := true,
            connector := ConnectorName
        } ->
            ConnectorId = emqx_connector_resource:resource_id(
                bridge_v2_type_to_connector_type(BridgeType), ConnectorName
            ),
            emqx_resource_manager:channel_health_check(
                ConnectorId, id(BridgeType, BridgeName, ConnectorName)
            );
        #{enable := false} ->
            {error, bridge_stopped};
        Error ->
            Error
    end.

% do_send_msg_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config) ->
%     BridgeV2Id = emqx_bridge_v2:id(BridgeType, BridgeName),
%     ConnectorResourceId = emqx_bridge_v2:extract_connector_id_from_bridge_v2_id(BridgeV2Id),
%     try
%         case emqx_resource_manager:maybe_install_bridge_v2(ConnectorResourceId, BridgeV2Id) of
%             ok ->
%                 do_send_msg_after_bridge_v2_installed(
%                   BridgeType,
%                   BridgeName,
%                   BridgeV2Id,
%                   Message,
%                   QueryOpts0,
%                   Config
%                  );
%             InstallError ->
%                 throw(InstallError)
%         end
%     catch
%         Error:Reason:Stack ->
%             Msg = iolist_to_binary(
%                 io_lib:format(
%                     "Failed to install bridge_v2 ~p in connector ~p: ~p",
%                     [BridgeV2Id, ConnectorResourceId, Reason]
%                 )
%             ),
%             ?SLOG(error, #{
%                 msg => Msg,
%                 error => Error,
%                 reason => Reason,
%                 stacktrace => Stack
%             })
%     end.

do_send_msg_with_enabled_config(
    BridgeType, BridgeName, Message, QueryOpts0, Config
) ->
    QueryMode = get_query_mode(BridgeType, Config),
    QueryOpts = maps:merge(
        emqx_bridge:query_opts(Config),
        QueryOpts0#{
            query_mode => QueryMode
        }
    ),
    BridgeV2Id = emqx_bridge_v2:id(BridgeType, BridgeName),
    emqx_resource:query(BridgeV2Id, {BridgeV2Id, Message}, QueryOpts).

parse_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [Type, Name] ->
            {Type, Name};
        [<<"bridge_v2">>, Type, Name | _] ->
            {Type, Name};
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

id(BridgeType, BridgeName) ->
    case lookup(BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id(BridgeType, BridgeName, ConnectorName);
        Error ->
            error(Error)
    end.

id(BridgeType, BridgeName, ConnectorName) ->
    ConnectorType = bin(bridge_v2_type_to_connector_type(BridgeType)),
    <<"bridge_v2:", (bin(BridgeType))/binary, ":", (bin(BridgeName))/binary, ":connector:",
        (bin(ConnectorType))/binary, ":", (bin(ConnectorName))/binary>>.

bridge_v2_type_to_connector_type(kafka) ->
    kafka.

is_bridge_v2_type(kafka) -> true;
is_bridge_v2_type(_) -> false.

is_bridge_v2_id(<<"bridge_v2:", _/binary>>) -> true;
is_bridge_v2_id(_) -> false.

extract_connector_id_from_bridge_v2_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [<<"bridge_v2">>, _Type, _Name, <<"connector">>, ConnectorType, ConnecorName] ->
            <<"connector:", ConnectorType/binary, ":", ConnecorName/binary>>;
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid bridge V2 ID: ~p", [Id]))})
    end.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

%% Basic CRUD Operations

list() ->
    maps:fold(
        fun(Type, NameAndConf, Bridges) ->
            maps:fold(
                fun(Name, RawConf, Acc) ->
                    [
                        #{
                            type => Type,
                            name => Name,
                            raw_config => RawConf
                        }
                        | Acc
                    ]
                end,
                Bridges,
                NameAndConf
            )
        end,
        [],
        emqx:get_raw_config([?ROOT_KEY], #{})
    ).

lookup(Id) ->
    {Type, Name} = parse_id(Id),
    lookup(Type, Name).

lookup(Type, Name) ->
    case emqx:get_config([?ROOT_KEY, Type, Name], not_found) of
        not_found ->
            {error, bridge_not_found};
        Config ->
            Config
    end.

get_metrics(Type, Name) ->
    emqx_resource:get_metrics(id(Type, Name)).

config_key_path() ->
    [?ROOT_KEY].

disable_enable(Action, BridgeType, BridgeName) when
    Action =:= disable; Action =:= enable
->
    emqx_conf:update(
        config_key_path() ++ [BridgeType, BridgeName],
        {Action, BridgeType, BridgeName},
        #{override_to => cluster}
    ).

create(BridgeType, BridgeName, RawConf) ->
    ?SLOG(debug, #{
        brige_action => create,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName,
        bridge_raw_config => emqx_utils:redact(RawConf)
    }),
    emqx_conf:update(
        config_key_path() ++ [BridgeType, BridgeName],
        RawConf,
        #{override_to => cluster}
    ).

remove(BridgeType, BridgeName) ->
    ?SLOG(debug, #{
        brige_action => remove,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    emqx_conf:remove(
        config_key_path() ++ [BridgeType, BridgeName],
        #{override_to => cluster}
    ).

%% This top level handler will be triggered when the bridges_v2 path is updated
%% with calls to emqx_conf:update([bridges_v2], BridgesConf, #{}).
%%
%% A public API that can trigger this is:
%% bin/emqx ctl conf load data/configs/cluster.hocon
post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    %% The config update will be failed if any task in `perform_bridge_changes` failed.
    RemoveFun = fun uninstall_bridge_v2/3,
    CreateFun = fun install_bridge_v2/3,
    UpdateFun = fun(Type, Name, {OldBridgeConf, Conf}) ->
        uninstall_bridge_v2(Type, Name, OldBridgeConf),
        install_bridge_v2(Type, Name, Conf)
    end,
    Result = perform_bridge_changes([
        #{action => RemoveFun, data => Removed},
        #{
            action => CreateFun,
            data => Added,
            on_exception_fn => fun emqx_bridge_resource:remove/4
        },
        #{action => UpdateFun, data => Updated}
    ]),
    ?tp(bridge_post_config_update_done, #{}),
    Result;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], '$remove', _, _OldConf, _AppEnvs) ->
    Conf = emqx:get_config([?ROOT_KEY, BridgeType, BridgeName]),
    ok = uninstall_bridge_v2(BridgeType, BridgeName, Conf),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs) ->
    ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs) ->
    ok = uninstall_bridge_v2(BridgeType, BridgeName, OldConf),
    ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
    ?tp(bridge_post_config_update_done, #{}),
    ok.

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

perform_bridge_changes(Tasks) ->
    perform_bridge_changes(Tasks, ok).

perform_bridge_changes([], Result) ->
    Result;
perform_bridge_changes([#{action := Action, data := MapConfs} = Task | Tasks], Result0) ->
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Result = maps:fold(
        fun
            ({_Type, _Name}, _Conf, {error, Reason}) ->
                {error, Reason};
            %% for update
            ({Type, Name}, {OldConf, Conf}, _) ->
                case Action(Type, Name, {OldConf, Conf}) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
                end;
            ({Type, Name}, Conf, _) ->
                try Action(Type, Name, Conf) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
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
                        OnException(Type, Name, Conf),
                        erlang:raise(Kind, Error, Stacktrace)
                end
        end,
        Result0,
        MapConfs
    ),
    perform_bridge_changes(Tasks, Result).

is_bridge_v2_installed_in_connector_state(Tag, State) when is_map(State) ->
    BridgeV2s = maps:get(installed_bridge_v2s, State, #{}),
    maps:is_key(Tag, BridgeV2s);
is_bridge_v2_installed_in_connector_state(_Tag, _State) ->
    false.
