%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_resource).

-include("../../emqx_bridge/include/emqx_bridge_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include("emqx_connector.hrl").

-export([
    connector_to_resource_type/1,
    resource_id/2,
    resource_id/3,
    connector_id/2,
    parse_connector_id/1,
    parse_connector_id/2,
    connector_hookpoint/1,
    connector_hookpoint_to_connector_id/1
]).

-export([
    create/3,
    create/4,
    create/5,
    create_dry_run/2,
    create_dry_run/3,
    recreate/2,
    recreate/3,
    remove/1,
    remove/2,
    remove/3,
    remove/4,
    remove/5,
    restart/2,
    restart/3,
    start/2,
    start/3,
    stop/2,
    stop/3,
    update/2,
    update/3,
    update/4,
    update/5,
    get_channels/2,
    get_channels/3
]).

-export([parse_url/1]).

-define(PROBE_ID_SEP, $_).

%% Some connectors (e.g., Kafka producer/wolff) have some timeouts of 5 s when tearing
%% down unresponsive processes.
-define(STOP_TIMEOUT, 10_000).

-callback connector_config(ParsedConfig, Context) ->
    ParsedConfig
when
    ParsedConfig :: #{atom() => any()}, Context :: #{atom() => any()}.
-optional_callbacks([connector_config/2]).

connector_to_resource_type(ConnectorType) ->
    try
        emqx_connector_info:resource_callback_module(ConnectorType)
    catch
        _:_ ->
            error({unknown_connector_type, ConnectorType})
    end.

connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(ConnectorType) ->
    emqx_connector_info:config_transform_module(ConnectorType).

resource_id(ConnectorType, ConnectorName) ->
    resource_id(_Namespace = undefined, ConnectorType, ConnectorName).

resource_id(Namespace, ConnectorType, ConnectorName) ->
    ConnectorId = connector_id(ConnectorType, ConnectorName),
    ResId = <<"connector:", ConnectorId/binary>>,
    case is_binary(Namespace) of
        true ->
            <<"ns:", Namespace/binary, ":", ResId/binary>>;
        false ->
            ResId
    end.

connector_id(ConnectorType, ConnectorName) ->
    Name = bin(ConnectorName),
    Type = bin(ConnectorType),
    <<Type/binary, ":", Name/binary>>.

parse_connector_id(ConnectorId) ->
    parse_connector_id(ConnectorId, #{atom_name => true}).

-spec parse_connector_id(binary() | atom(), #{atom_name => boolean()}) ->
    {atom(), atom() | binary()}.
parse_connector_id(<<"ns:", NSConnectorId/binary>>, Opts) ->
    case binary:split(NSConnectorId, <<":">>) of
        [Namespace, ConnectorId] when size(Namespace) > 0 ->
            parse_connector_id(ConnectorId, Opts);
        _ ->
            throw(#{
                kind => validation_error,
                reason => <<"Invalid connector id: bad namespace tag">>
            })
    end;
parse_connector_id(<<"connector:", ConnectorId/binary>>, Opts) ->
    parse_connector_id(ConnectorId, Opts);
parse_connector_id(?PROBE_ID_MATCH(Suffix), Opts) ->
    <<?PROBE_ID_SEP, ConnectorId/binary>> = Suffix,
    parse_connector_id(ConnectorId, Opts);
parse_connector_id(ConnectorId, Opts) ->
    emqx_resource:parse_resource_id(ConnectorId, Opts).

connector_hookpoint(ConnectorId) ->
    <<"$connectors/", (bin(ConnectorId))/binary>>.

connector_hookpoint_to_connector_id(?BRIDGE_HOOKPOINT(ConnectorId)) ->
    {ok, ConnectorId};
connector_hookpoint_to_connector_id(_) ->
    {error, bad_connector_hookpoint}.

restart(Type, Name) ->
    restart(_Namespace = undefined, Type, Name).
restart(Namespace, Type, Name) ->
    ConnResId = resource_id(Namespace, Type, Name),
    emqx_resource:restart(ConnResId).

stop(Type, Name) ->
    stop(_Namespace = undefined, Type, Name).
stop(Namespace, Type, Name) ->
    ConnResId = resource_id(Namespace, Type, Name),
    emqx_resource:stop(ConnResId, ?STOP_TIMEOUT).

start(Type, Name) ->
    start(_Namespace = undefined, Type, Name).
start(Namespace, Type, Name) ->
    ConnResId = resource_id(Namespace, Type, Name),
    emqx_resource:start(ConnResId).

create(Type, Name, Conf) ->
    create(Type, Name, Conf, #{}).
create(Type, Name, Conf, Opts) ->
    create(_Namespace = undefined, Type, Name, Conf, Opts).
create(Namespace, Type, Name, Conf0, Opts) ->
    ?SLOG(info, #{
        msg => "create connector",
        type => Type,
        name => Name,
        namespace => Namespace,
        config => redact(Conf0, Type)
    }),
    TypeBin = bin(Type),
    ResourceId = resource_id(Namespace, Type, Name),
    Conf = Conf0#{connector_type => TypeBin, connector_name => Name},
    _ = emqx_alarm:ensure_deactivated(ResourceId),
    {ok, _Data} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?MODULE:connector_to_resource_type(Type),
        parse_confs(TypeBin, Name, Conf),
        parse_opts(Conf, Opts)
    ),
    ok.

update(ConnectorId, {OldConf, Conf}) ->
    {ConnectorType, ConnectorName} = parse_connector_id(ConnectorId),
    update(ConnectorType, ConnectorName, {OldConf, Conf}).
update(Type, Name, {OldConf, Conf}) ->
    update(Type, Name, {OldConf, Conf}, #{}).
update(Type, Name, {OldConf, Conf0}, Opts) ->
    update(_Namespace = undefined, Type, Name, {OldConf, Conf0}, Opts).
update(Namespace, Type, Name, {OldConf, Conf0}, Opts) ->
    %% TODO: sometimes its not necessary to restart the connector connection.
    %%
    %% - if the connection related configs like `servers` is updated, we should restart/start
    %% or stop connectors according to the change.
    %% - if the connection related configs are not update, only non-connection configs like
    %% the `method` or `headers` of a WebHook is changed, then the connector can be updated
    %% without restarting the connector.
    %%
    Conf = Conf0#{connector_type => bin(Type), connector_name => bin(Name)},
    case emqx_utils_maps:if_only_to_toggle_enable(OldConf, Conf0) of
        false ->
            ?SLOG(info, #{
                msg => "update connector",
                type => Type,
                name => Name,
                namespace => Namespace,
                config => redact(Conf, Type)
            }),
            case recreate(Namespace, Type, Name, Conf, Opts) of
                {ok, _} ->
                    ok;
                {error, not_found} ->
                    ?SLOG(warning, #{
                        msg => "updating_a_non_existing_connector",
                        type => Type,
                        name => Name,
                        namespace => Namespace,
                        config => redact(Conf, Type)
                    }),
                    create(Namespace, Type, Name, Conf, Opts);
                {error, Reason} ->
                    {error, {update_connector_failed, Reason}}
            end;
        true ->
            %% we don't need to recreate the connector if this config change is only to
            %% toggle the config 'connector.{type}.{name}.enable'
            _ =
                case maps:get(enable, Conf, true) of
                    true ->
                        restart(Namespace, Type, Name);
                    false ->
                        stop(Namespace, Type, Name)
                end,
            ok
    end.

get_channels(Type, Name) ->
    get_channels(_Namespace = undefined, Type, Name).
get_channels(Namespace, Type, Name) ->
    ConnResId = resource_id(Namespace, Type, Name),
    emqx_resource:get_channels(ConnResId).

recreate(Type, Name) ->
    recreate(Type, Name, emqx:get_config([connectors, Type, Name])).
recreate(Type, Name, Conf) ->
    recreate(Type, Name, Conf, #{}).
recreate(Type, Name, Conf, Opts) ->
    recreate(_Namespace = undefined, Type, Name, Conf, Opts).
recreate(Namespace, Type, Name, Conf, Opts) ->
    TypeBin = bin(Type),
    emqx_resource:recreate_local(
        resource_id(Namespace, Type, Name),
        ?MODULE:connector_to_resource_type(Type),
        parse_confs(TypeBin, Name, Conf),
        parse_opts(Conf, Opts)
    ).

create_dry_run(Type, Conf) ->
    create_dry_run(Type, Conf, fun(_) -> ok end).

create_dry_run(Type, Conf0, Callback) ->
    %% Already type checked, no need to catch errors
    TypeBin = bin(Type),
    TypeAtom = safe_atom(Type),
    %% We use a fixed name here to avoid creating an atom
    %% to avoid potential race condition, the resource id should be unique
    Prefix = ?PROBE_ID_NEW(),
    TmpName = iolist_to_binary([Prefix, ?PROBE_ID_SEP, TypeBin, $:, "dryrun"]),
    TmpPath = emqx_utils:safe_filename(TmpName),
    Conf1 = maps:without([<<"name">>], Conf0),
    RawConf = #{<<"connectors">> => #{TypeBin => #{<<"temp_name">> => Conf1}}},
    try
        CheckedConf1 =
            hocon_tconf:check_plain(
                emqx_connector_schema,
                RawConf,
                #{atom_key => true, required => false}
            ),
        CheckedConf2 = get_temp_conf(TypeAtom, CheckedConf1),
        CheckedConf = CheckedConf2#{connector_type => TypeBin, connector_name => TmpName},
        case emqx_connector_ssl:convert_certs(TmpPath, CheckedConf) of
            {error, Reason} ->
                {error, Reason};
            {ok, ConfNew} ->
                ParseConf = parse_confs(bin(Type), TmpName, ConfNew),
                emqx_resource:create_dry_run_local(
                    TmpName, ?MODULE:connector_to_resource_type(Type), ParseConf, Callback
                )
        end
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    after
        _ = file:del_dir_r(emqx_tls_lib:pem_dir(TmpPath))
    end.

get_temp_conf(TypeAtom, CheckedConf) ->
    case CheckedConf of
        #{connectors := #{TypeAtom := #{temp_name := Conf}}} ->
            Conf;
        #{connectors := #{TypeAtom := #{<<"temp_name">> := Conf}}} ->
            Conf
    end.

remove(ConnectorId) ->
    {ConnectorType, ConnectorName} = parse_connector_id(ConnectorId),
    remove(ConnectorType, ConnectorName, #{}, #{}).
remove(Type, Name) ->
    remove(_Namespace = undefined, Type, Name, #{}, #{}).
remove(Namespace, Type, Name) ->
    remove(Namespace, Type, Name, #{}, #{}).
remove(Type, Name, Conf, Opts) ->
    remove(_Namespace = undefined, Type, Name, Conf, Opts).
remove(Namespace, Type, Name, _Conf, _Opts) ->
    %% just for perform_connector_changes/1
    ?SLOG(info, #{
        msg => "remove_connector",
        type => Type,
        name => Name,
        namespace => Namespace
    }),
    ConnResId = resource_id(Namespace, Type, Name),
    emqx_resource:remove_local(ConnResId).

%% convert connector configs to what the connector modules want
parse_confs(
    <<"mqtt">> = Type,
    Name,
    Conf
) ->
    insert_hookpoints(Type, Name, Conf);
parse_confs(
    <<"http">>,
    _Name,
    #{
        url := Url,
        headers := Headers
    } = Conf
) ->
    Url1 = bin(Url),
    {RequestBase, Path} = parse_url(Url1),
    Conf#{
        request_base => RequestBase,
        request =>
            #{
                path => Path,
                headers => Headers,
                body => undefined,
                method => undefined
            }
    };
parse_confs(ConnectorType, Name, Config) ->
    connector_config(ConnectorType, Name, Config).

insert_hookpoints(Type, Name, Conf) ->
    BId = emqx_bridge_resource:bridge_id(Type, Name),
    BridgeHookpoint = emqx_bridge_resource:bridge_hookpoint(BId),
    ConnectorHookpoint = connector_hookpoint(BId),
    HookPoints = [BridgeHookpoint, ConnectorHookpoint],
    Conf#{hookpoints => HookPoints}.

connector_config(ConnectorType, Name, Config) ->
    Mod = connector_impl_module(ConnectorType),
    case erlang:function_exported(Mod, connector_config, 2) of
        true ->
            Mod:connector_config(Config, #{
                type => ConnectorType,
                name => Name,
                parse_confs => fun parse_confs/3
            });
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

-spec invalid_data(binary()) -> no_return().
invalid_data(Msg) ->
    throw(#{
        kind => validation_error,
        reason => Msg
    }).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

safe_atom(Bin) when is_binary(Bin) -> binary_to_existing_atom(Bin, utf8);
safe_atom(Atom) when is_atom(Atom) -> Atom.

parse_opts(Conf, Opts0) ->
    Opts1 = emqx_resource:fetch_creation_opts(Conf),
    Opts = maps:merge(Opts1, Opts0),
    set_no_buffer_workers(Opts).

set_no_buffer_workers(Opts) ->
    Opts#{spawn_buffer_workers => false}.

redact(Conf, _Type) ->
    emqx_utils:redact(Conf).
