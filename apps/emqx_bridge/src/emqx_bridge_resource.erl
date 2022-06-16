%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_resource).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    bridge_to_resource_type/1,
    resource_id/1,
    resource_id/2,
    bridge_id/2,
    parse_bridge_id/1
]).

-export([
    create/2,
    create/3,
    create/4,
    recreate/2,
    recreate/3,
    create_dry_run/2,
    remove/1,
    remove/2,
    remove/3,
    update/2,
    update/3,
    stop/2,
    restart/2,
    reset_metrics/1
]).

bridge_to_resource_type(<<"mqtt">>) -> emqx_connector_mqtt;
bridge_to_resource_type(mqtt) -> emqx_connector_mqtt;
bridge_to_resource_type(<<"webhook">>) -> emqx_connector_http;
bridge_to_resource_type(webhook) -> emqx_connector_http.

resource_id(BridgeId) when is_binary(BridgeId) ->
    <<"bridge:", BridgeId/binary>>.

resource_id(BridgeType, BridgeName) ->
    BridgeId = bridge_id(BridgeType, BridgeName),
    resource_id(BridgeId).

bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    case string:split(bin(BridgeId), ":", all) of
        [Type, Name] -> {binary_to_atom(Type, utf8), binary_to_atom(Name, utf8)};
        _ -> error({invalid_bridge_id, BridgeId})
    end.

reset_metrics(ResourceId) ->
    emqx_resource:reset_metrics(ResourceId).

stop(Type, Name) ->
    emqx_resource:stop(resource_id(Type, Name)).

%% we don't provide 'start', as we want an already started bridge to be restarted.
restart(Type, Name) ->
    emqx_resource:restart(resource_id(Type, Name)).

create(BridgeId, Conf) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    create(BridgeType, BridgeName, Conf).

create(Type, Name, Conf) ->
    create(Type, Name, Conf, #{auto_retry_interval => 60000}).

create(Type, Name, Conf, Opts) ->
    ?SLOG(info, #{
        msg => "create bridge",
        type => Type,
        name => Name,
        config => Conf
    }),
    {ok, _Data} = emqx_resource:create_local(
        resource_id(Type, Name),
        <<"emqx_bridge">>,
        bridge_to_resource_type(Type),
        parse_confs(Type, Name, Conf),
        Opts
    ),
    maybe_disable_bridge(Type, Name, Conf).

update(BridgeId, {OldConf, Conf}) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    update(BridgeType, BridgeName, {OldConf, Conf}).

update(Type, Name, {OldConf, Conf}) ->
    %% TODO: sometimes its not necessary to restart the bridge connection.
    %%
    %% - if the connection related configs like `servers` is updated, we should restart/start
    %% or stop bridges according to the change.
    %% - if the connection related configs are not update, only non-connection configs like
    %% the `method` or `headers` of a WebHook is changed, then the bridge can be updated
    %% without restarting the bridge.
    %%
    case emqx_map_lib:if_only_to_toggle_enable(OldConf, Conf) of
        false ->
            ?SLOG(info, #{
                msg => "update bridge",
                type => Type,
                name => Name,
                config => Conf
            }),
            case recreate(Type, Name, Conf) of
                {ok, _} ->
                    maybe_disable_bridge(Type, Name, Conf);
                {error, not_found} ->
                    ?SLOG(warning, #{
                        msg => "updating_a_non-exist_bridge_need_create_a_new_one",
                        type => Type,
                        name => Name,
                        config => Conf
                    }),
                    create(Type, Name, Conf);
                {error, Reason} ->
                    {error, {update_bridge_failed, Reason}}
            end;
        true ->
            %% we don't need to recreate the bridge if this config change is only to
            %% toggole the config 'bridge.{type}.{name}.enable'
            _ =
                case maps:get(enable, Conf, true) of
                    true ->
                        restart(Type, Name);
                    false ->
                        stop(Type, Name)
                end,
            ok
    end.

recreate(Type, Name) ->
    recreate(Type, Name, emqx:get_config([bridges, Type, Name])).

recreate(Type, Name, Conf) ->
    emqx_resource:recreate_local(
        resource_id(Type, Name),
        bridge_to_resource_type(Type),
        parse_confs(Type, Name, Conf),
        #{auto_retry_interval => 60000}
    ).

create_dry_run(Type, Conf) ->
    Conf0 = fill_dry_run_conf(Conf),
    case emqx_resource:check_config(bridge_to_resource_type(Type), Conf0) of
        {ok, Conf1} ->
            TmpPath = iolist_to_binary(["bridges-create-dry-run:", emqx_misc:gen_id(8)]),
            case emqx_connector_ssl:convert_certs(TmpPath, Conf1) of
                {error, Reason} ->
                    {error, Reason};
                {ok, ConfNew} ->
                    Res = emqx_resource:create_dry_run_local(
                        bridge_to_resource_type(Type), ConfNew
                    ),
                    _ = maybe_clear_certs(TmpPath, ConfNew),
                    Res
            end;
        {error, _} = Error ->
            Error
    end.

remove(BridgeId) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    remove(BridgeType, BridgeName, #{}).

remove(Type, Name) ->
    remove(Type, Name, undefined).

%% just for perform_bridge_changes/1
remove(Type, Name, _Conf) ->
    ?SLOG(info, #{msg => "remove_bridge", type => Type, name => Name}),
    case emqx_resource:remove_local(resource_id(Type, Name)) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.

maybe_disable_bridge(Type, Name, Conf) ->
    case maps:get(enable, Conf, true) of
        false -> stop(Type, Name);
        true -> ok
    end.

fill_dry_run_conf(Conf) ->
    Conf#{
        <<"egress">> =>
            #{
                <<"remote_topic">> => <<"t">>,
                <<"remote_qos">> => 0,
                <<"retain">> => true,
                <<"payload">> => <<"val">>
            },
        <<"ingress">> =>
            #{<<"remote_topic">> => <<"t">>}
    }.

maybe_clear_certs(TmpPath, #{ssl := SslConf} = Conf) ->
    %% don't remove the cert files if they are in use
    case is_tmp_path_conf(TmpPath, SslConf) of
        true -> emqx_connector_ssl:clear_certs(TmpPath, Conf);
        false -> ok
    end.

is_tmp_path_conf(TmpPath, #{certfile := Certfile}) ->
    is_tmp_path(TmpPath, Certfile);
is_tmp_path_conf(TmpPath, #{keyfile := Keyfile}) ->
    is_tmp_path(TmpPath, Keyfile);
is_tmp_path_conf(TmpPath, #{cacertfile := CaCertfile}) ->
    is_tmp_path(TmpPath, CaCertfile);
is_tmp_path_conf(_TmpPath, _Conf) ->
    false.

is_tmp_path(TmpPath, File) ->
    string:str(str(File), str(TmpPath)) > 0.

parse_confs(
    webhook,
    _Name,
    #{
        url := Url,
        method := Method,
        body := Body,
        headers := Headers,
        request_timeout := ReqTimeout
    } = Conf
) ->
    {BaseUrl, Path} = parse_url(Url),
    {ok, BaseUrl2} = emqx_http_lib:uri_parse(BaseUrl),
    Conf#{
        base_url => BaseUrl2,
        request =>
            #{
                path => Path,
                method => Method,
                body => Body,
                headers => Headers,
                request_timeout => ReqTimeout
            }
    };
parse_confs(Type, Name, #{connector := ConnId, direction := Direction} = Conf) when
    is_binary(ConnId)
->
    case emqx_connector:parse_connector_id(ConnId) of
        {Type, ConnName} ->
            ConnectorConfs = emqx:get_config([connectors, Type, ConnName]),
            make_resource_confs(
                Direction,
                ConnectorConfs,
                maps:without([connector, direction], Conf),
                Type,
                Name
            );
        {_ConnType, _ConnName} ->
            error({cannot_use_connector_with_different_type, ConnId})
    end;
parse_confs(Type, Name, #{connector := ConnectorConfs, direction := Direction} = Conf) when
    is_map(ConnectorConfs)
->
    make_resource_confs(
        Direction,
        ConnectorConfs,
        maps:without([connector, direction], Conf),
        Type,
        Name
    ).

make_resource_confs(ingress, ConnectorConfs, BridgeConf, Type, Name) ->
    BName = bridge_id(Type, Name),
    ConnectorConfs#{
        ingress => BridgeConf#{hookpoint => <<"$bridges/", BName/binary>>}
    };
make_resource_confs(egress, ConnectorConfs, BridgeConf, _Type, _Name) ->
    ConnectorConfs#{
        egress => BridgeConf
    }.

parse_url(Url) ->
    case string:split(Url, "//", leading) of
        [Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [HostPort, Path] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), Path};
                [HostPort] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), <<>>}
            end;
        [Url] ->
            error({invalid_url, Url})
    end.

str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(Str) when is_list(Str) -> Str.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
