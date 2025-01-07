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
-module(emqx_bridge_resource).

-include("emqx_bridge_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([
    bridge_to_resource_type/1,
    resource_id/1,
    resource_id/2,
    resource_id/3,
    bridge_id/2,
    parse_bridge_id/1,
    parse_bridge_id/2,
    bridge_hookpoint/1,
    bridge_hookpoint_to_bridge_id/1
]).

-export([
    create/2,
    create/3,
    create/4,
    create_dry_run/2,
    recreate/2,
    recreate/3,
    remove/1,
    remove/2,
    remove/4,
    reset_metrics/1,
    restart/2,
    start/2,
    stop/2,
    update/2,
    update/3,
    update/4
]).

-callback connector_config(ParsedConfig) -> ParsedConfig when ParsedConfig :: #{atom() => any()}.
-optional_callbacks([connector_config/1]).

%% bi-directional bridge with producer/consumer or ingress/egress configs
-define(IS_BI_DIR_BRIDGE(TYPE),
    (TYPE) =:= <<"mqtt">>
).
-define(IS_INGRESS_BRIDGE(TYPE),
    (TYPE) =:= <<"kafka_consumer">> orelse
        (TYPE) =:= <<"gcp_pubsub_consumer">> orelse
        ?IS_BI_DIR_BRIDGE(TYPE)
).

-define(ROOT_KEY_ACTIONS, actions).
-define(ROOT_KEY_SOURCES, sources).

-if(?EMQX_RELEASE_EDITION == ee).
bridge_to_resource_type(BridgeType) when is_binary(BridgeType) ->
    bridge_to_resource_type(binary_to_existing_atom(BridgeType, utf8));
bridge_to_resource_type(mqtt) ->
    emqx_bridge_mqtt_connector;
bridge_to_resource_type(webhook) ->
    emqx_bridge_http_connector;
bridge_to_resource_type(BridgeType) ->
    emqx_bridge_enterprise:resource_type(BridgeType).

bridge_impl_module(BridgeType) -> emqx_bridge_enterprise:bridge_impl_module(BridgeType).
-else.
bridge_to_resource_type(BridgeType) when is_binary(BridgeType) ->
    bridge_to_resource_type(binary_to_existing_atom(BridgeType, utf8));
bridge_to_resource_type(mqtt) ->
    emqx_bridge_mqtt_connector;
bridge_to_resource_type(webhook) ->
    emqx_bridge_http_connector.

bridge_impl_module(_BridgeType) -> undefined.
-endif.

resource_id(BridgeId) when is_binary(BridgeId) ->
    resource_id_for_kind(?ROOT_KEY_ACTIONS, BridgeId).

resource_id(BridgeType, BridgeName) ->
    resource_id(?ROOT_KEY_ACTIONS, BridgeType, BridgeName).

resource_id(ConfRootKey, BridgeType, BridgeName) ->
    BridgeId = bridge_id(BridgeType, BridgeName),
    resource_id_for_kind(ConfRootKey, BridgeId).

resource_id_for_kind(ConfRootKey, BridgeId) when is_binary(BridgeId) ->
    case binary:split(BridgeId, <<":">>) of
        [Type, _Name] ->
            case emqx_bridge_v2:is_bridge_v2_type(Type) of
                true ->
                    emqx_bridge_v2:bridge_v1_id_to_connector_resource_id(ConfRootKey, BridgeId);
                false ->
                    <<"bridge:", BridgeId/binary>>
            end;
        _ ->
            invalid_data(<<"should be of pattern {type}:{name}, but got ", BridgeId/binary>>)
    end.

bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    parse_bridge_id(bin(BridgeId), #{atom_name => true}).

-spec parse_bridge_id(binary() | atom(), #{atom_name => boolean()}) ->
    {atom(), atom() | binary()}.
parse_bridge_id(<<"bridge:", ID/binary>>, Opts) ->
    parse_bridge_id(ID, Opts);
parse_bridge_id(BridgeId, Opts) ->
    {Type, Name} = emqx_resource:parse_resource_id(BridgeId, Opts),
    {emqx_bridge_lib:upgrade_type(Type), Name}.

bridge_hookpoint(BridgeId) ->
    <<"$bridges/", (bin(BridgeId))/binary>>.

bridge_hookpoint_to_bridge_id(?BRIDGE_HOOKPOINT(BridgeId)) ->
    {ok, BridgeId};
bridge_hookpoint_to_bridge_id(?SOURCE_HOOKPOINT(BridgeId)) ->
    {ok, BridgeId};
bridge_hookpoint_to_bridge_id(_) ->
    {error, bad_bridge_hookpoint}.

-spec invalid_data(binary()) -> no_return().
invalid_data(Reason) -> throw(#{kind => validation_error, reason => Reason}).

reset_metrics(ResourceId) ->
    %% TODO we should not create atoms here
    {Type, Name} = parse_bridge_id(ResourceId),
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        false ->
            emqx_resource:reset_metrics(ResourceId);
        true ->
            case emqx_bridge_v2:bridge_v1_is_valid(Type, Name) of
                true ->
                    BridgeV2Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(Type),
                    emqx_bridge_v2:reset_metrics(BridgeV2Type, Name);
                false ->
                    {error, not_bridge_v1_compatible}
            end
    end.

restart(Type, Name) ->
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        false ->
            emqx_resource:restart(resource_id(Type, Name));
        true ->
            emqx_bridge_v2:bridge_v1_restart(Type, Name)
    end.

stop(Type, Name) ->
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        false ->
            emqx_resource:stop(resource_id(Type, Name));
        true ->
            emqx_bridge_v2:bridge_v1_stop(Type, Name)
    end.

start(Type, Name) ->
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        false ->
            emqx_resource:start(resource_id(Type, Name));
        true ->
            emqx_bridge_v2:bridge_v1_start(Type, Name)
    end.

create(BridgeId, Conf) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    create(BridgeType, BridgeName, Conf).

create(Type, Name, Conf) ->
    create(Type, Name, Conf, #{}).

create(Type, Name, Conf0, Opts) ->
    ?SLOG(info, #{
        msg => "create_bridge",
        type => Type,
        name => Name,
        config => emqx_utils:redact(Conf0)
    }),
    TypeBin = bin(Type),
    Conf = Conf0#{bridge_type => TypeBin, bridge_name => Name},
    {ok, _Data} = emqx_resource:create_local(
        resource_id(Type, Name),
        <<"bridge">>,
        bridge_to_resource_type(Type),
        parse_confs(TypeBin, Name, Conf),
        parse_opts(Conf, Opts)
    ),
    ok.

update(BridgeId, {OldConf, Conf}) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    update(BridgeType, BridgeName, {OldConf, Conf}).

update(Type, Name, {OldConf, Conf}) ->
    update(Type, Name, {OldConf, Conf}, #{}).

update(Type, Name, {OldConf, Conf}, Opts) ->
    %% TODO: sometimes its not necessary to restart the bridge connection.
    %%
    %% - if the connection related configs like `servers` is updated, we should restart/start
    %% or stop bridges according to the change.
    %% - if the connection related configs are not update, only non-connection configs like
    %% the `method` or `headers` of a WebHook is changed, then the bridge can be updated
    %% without restarting the bridge.
    %%
    case emqx_utils_maps:if_only_to_toggle_enable(OldConf, Conf) of
        false ->
            ?SLOG(info, #{
                msg => "update_bridge",
                type => Type,
                name => Name,
                config => emqx_utils:redact(Conf)
            }),
            case recreate(Type, Name, Conf, Opts) of
                {ok, _} ->
                    ok;
                {error, not_found} ->
                    ?SLOG(warning, #{
                        msg => "updating_a_non_existing_bridge",
                        type => Type,
                        name => Name,
                        config => emqx_utils:redact(Conf)
                    }),
                    create(Type, Name, Conf, Opts);
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
    recreate(Type, Name, Conf, #{}).

recreate(Type, Name, Conf0, Opts) ->
    TypeBin = bin(Type),
    Conf = Conf0#{bridge_type => TypeBin, bridge_name => Name},
    emqx_resource:recreate_local(
        resource_id(Type, Name),
        bridge_to_resource_type(Type),
        parse_confs(TypeBin, Name, Conf),
        parse_opts(Conf, Opts)
    ).

create_dry_run(Type0, Conf0) ->
    Type = emqx_bridge_lib:upgrade_type(Type0),
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        false ->
            create_dry_run_bridge_v1(Type, Conf0);
        true ->
            emqx_bridge_v2:bridge_v1_create_dry_run(Type, Conf0)
    end.

create_dry_run_bridge_v1(Type, Conf0) ->
    TmpName = ?PROBE_ID_NEW(),
    TmpPath = emqx_utils:safe_filename(TmpName),
    %% Already type checked, no need to catch errors
    TypeBin = bin(Type),
    TypeAtom = safe_atom(Type),
    Conf1 = maps:without([<<"name">>], Conf0),
    RawConf = #{<<"bridges">> => #{TypeBin => #{<<"temp_name">> => Conf1}}},
    try
        #{bridges := #{TypeAtom := #{temp_name := Conf2}}} =
            hocon_tconf:check_plain(
                emqx_bridge_schema,
                RawConf,
                #{atom_key => true, required => false}
            ),
        Conf = Conf2#{bridge_type => TypeBin, bridge_name => TmpName},
        case emqx_connector_ssl:convert_certs(TmpPath, Conf) of
            {error, Reason} ->
                {error, Reason};
            {ok, ConfNew} ->
                ParseConf = parse_confs(TypeBin, TmpName, ConfNew),
                emqx_resource:create_dry_run_local(bridge_to_resource_type(Type), ParseConf)
        end
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    after
        _ = file:del_dir_r(emqx_tls_lib:pem_dir(TmpPath))
    end.

remove(BridgeId) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    remove(BridgeType, BridgeName, #{}, #{}).

remove(Type, Name) ->
    remove(Type, Name, #{}, #{}).

%% just for perform_bridge_changes/1
remove(Type, Name, _Conf, _Opts) ->
    %% TODO we need to handle bridge_v2 here
    ?SLOG(info, #{msg => "remove_bridge", type => Type, name => Name}),
    emqx_resource:remove_local(resource_id(Type, Name)).

%% convert bridge configs to what the connector modules want
%% TODO: remove it, if the http_bridge already ported to v2
parse_confs(
    <<"webhook">>,
    _Name,
    #{
        url := Url,
        method := Method,
        headers := Headers,
        max_retries := Retry
    } = Conf
) ->
    Url1 = bin(Url),
    {RequestBase, Path} = parse_url(Url1),
    RequestTTL = emqx_utils_maps:deep_get(
        [resource_opts, request_ttl],
        Conf
    ),
    Conf#{
        request_base => RequestBase,
        request =>
            #{
                path => Path,
                method => Method,
                body => maps:get(body, Conf, undefined),
                headers => Headers,
                request_ttl => RequestTTL,
                max_retries => Retry
            }
    };
parse_confs(<<"iotdb">>, Name, Conf) ->
    %% [FIXME] this has no place here, it's used in parse_confs/3, which should
    %% rather delegate to a behavior callback than implementing domain knowledge
    %% here (reversed dependency)
    InsertTabletPathV1 = <<"rest/v1/insertTablet">>,
    InsertTabletPathV2 = <<"rest/v2/insertTablet">>,
    #{
        base_url := BaseURL,
        authentication :=
            #{
                username := Username,
                password := Secret
            }
    } = Conf,
    Password = emqx_secret:unwrap(Secret),
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    %% This version atom correspond to the macro ?VSN_1_1_X in
    %% emqx_bridge_iotdb.hrl. It would be better to use the macro directly, but
    %% this cannot be done without introducing a dependency on the
    %% emqx_iotdb_bridge app (which is an EE app).
    DefaultIOTDBBridge = 'v1.1.x',
    Version = maps:get(iotdb_version, Conf, DefaultIOTDBBridge),
    InsertTabletPath =
        case Version of
            DefaultIOTDBBridge -> InsertTabletPathV2;
            _ -> InsertTabletPathV1
        end,
    WebhookConfig =
        Conf#{
            method => <<"post">>,
            url => <<BaseURL/binary, InsertTabletPath/binary>>,
            headers => [
                {<<"Content-type">>, <<"application/json">>},
                {<<"Authorization">>, BasicToken}
            ]
        },
    parse_confs(
        <<"webhook">>,
        Name,
        WebhookConfig
    );
parse_confs(Type, Name, Conf) when ?IS_INGRESS_BRIDGE(Type) ->
    %% For some drivers that can be used as data-sources, we need to provide a
    %% hookpoint. The underlying driver will run `emqx_hooks:run/3` when it
    %% receives a message from the external database.
    BId = bridge_id(Type, Name),
    BridgeHookpoint = bridge_hookpoint(BId),
    Conf#{hookpoint => BridgeHookpoint};
parse_confs(BridgeType, _BridgeName, Config) ->
    connector_config(BridgeType, Config).

connector_config(BridgeType, Config) ->
    Mod = bridge_impl_module(BridgeType),
    case erlang:function_exported(Mod, connector_config, 1) of
        true ->
            Mod:connector_config(Config);
        false ->
            Config
    end.

parse_url(Url) ->
    Parsed = emqx_utils_uri:parse(Url),
    case Parsed of
        #{scheme := undefined} ->
            invalid_data(<<"Missing scheme in URL: ", Url/binary>>);
        #{authority := undefined} ->
            invalid_data(<<"Missing host in URL: ", Url/binary>>);
        #{authority := #{userinfo := Userinfo}} when Userinfo =/= undefined ->
            invalid_data(<<"Userinfo is not supported in URL: ", Url/binary>>);
        #{fragment := Fragment} when Fragment =/= undefined ->
            invalid_data(<<"Fragments are not supported in URL: ", Url/binary>>);
        _ ->
            case emqx_utils_uri:request_base(Parsed) of
                {ok, Base} ->
                    {Base, emqx_maybe:define(emqx_utils_uri:path(Parsed), <<>>)};
                {error, Reason0} ->
                    Reason1 = emqx_utils:readable_error_msg(Reason0),
                    invalid_data(<<"Invalid URL: ", Url/binary, ", details: ", Reason1/binary>>)
            end
    end.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

safe_atom(Bin) when is_binary(Bin) -> binary_to_existing_atom(Bin, utf8);
safe_atom(Atom) when is_atom(Atom) -> Atom.

parse_opts(Conf, Opts0) ->
    override_start_after_created(Conf, Opts0).

override_start_after_created(Config, Opts) ->
    Enabled = maps:get(enable, Config, true),
    StartAfterCreated = Enabled andalso maps:get(start_after_created, Opts, Enabled),
    Opts#{start_after_created => StartAfterCreated}.
