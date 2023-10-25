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
-module(emqx_connector_resource).

-include_lib("emqx_bridge/include/emqx_bridge_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([
    connector_to_resource_type/1,
    resource_id/1,
    resource_id/2,
    connector_id/2,
    parse_connector_id/1,
    parse_connector_id/2,
    connector_hookpoint/1,
    connector_hookpoint_to_connector_id/1
]).

-export([
    create/3,
    create/4,
    create_dry_run/2,
    create_dry_run/3,
    recreate/2,
    recreate/3,
    remove/1,
    remove/2,
    remove/4,
    restart/2,
    start/2,
    stop/2,
    update/2,
    update/3,
    update/4,
    get_channels/2
]).

-callback connector_config(ParsedConfig) ->
    ParsedConfig
when
    ParsedConfig :: #{atom() => any()}.
-optional_callbacks([connector_config/1]).

-if(?EMQX_RELEASE_EDITION == ee).
connector_to_resource_type(ConnectorType) ->
    try
        emqx_connector_ee_schema:resource_type(ConnectorType)
    catch
        _:_ -> connector_to_resource_type_ce(ConnectorType)
    end.

connector_impl_module(ConnectorType) ->
    emqx_connector_ee_schema:connector_impl_module(ConnectorType).
-else.

connector_to_resource_type(ConnectorType) -> connector_to_resource_type_ce(ConnectorType).

connector_impl_module(_ConnectorType) -> undefined.

-endif.

connector_to_resource_type_ce(_) -> undefined.

resource_id(ConnectorId) when is_binary(ConnectorId) ->
    <<"connector:", ConnectorId/binary>>.

resource_id(ConnectorType, ConnectorName) ->
    ConnectorId = connector_id(ConnectorType, ConnectorName),
    resource_id(ConnectorId).

connector_id(ConnectorType, ConnectorName) ->
    Name = bin(ConnectorName),
    Type = bin(ConnectorType),
    <<Type/binary, ":", Name/binary>>.

parse_connector_id(ConnectorId) ->
    parse_connector_id(ConnectorId, #{atom_name => true}).

-spec parse_connector_id(list() | binary() | atom(), #{atom_name => boolean()}) ->
    {atom(), atom() | binary()}.
parse_connector_id(ConnectorId, Opts) ->
    case string:split(bin(ConnectorId), ":", all) of
        [Type, Name] ->
            {to_type_atom(Type), validate_name(Name, Opts)};
        [_, Type, Name] ->
            {to_type_atom(Type), validate_name(Name, Opts)};
        _ ->
            invalid_data(
                <<"should be of pattern {type}:{name} or connector:{type}:{name}, but got ",
                    ConnectorId/binary>>
            )
    end.

connector_hookpoint(ConnectorId) ->
    <<"$connectors/", (bin(ConnectorId))/binary>>.

connector_hookpoint_to_connector_id(?BRIDGE_HOOKPOINT(ConnectorId)) ->
    {ok, ConnectorId};
connector_hookpoint_to_connector_id(_) ->
    {error, bad_connector_hookpoint}.

validate_name(Name0, Opts) ->
    Name = unicode:characters_to_list(Name0, utf8),
    case is_list(Name) andalso Name =/= [] of
        true ->
            case lists:all(fun is_id_char/1, Name) of
                true ->
                    case maps:get(atom_name, Opts, true) of
                        % NOTE
                        % Rule may be created before connector, thus not `list_to_existing_atom/1`,
                        % also it is infrequent user input anyway.
                        true -> list_to_atom(Name);
                        false -> Name0
                    end;
                false ->
                    invalid_data(<<"bad name: ", Name0/binary>>)
            end;
        false ->
            invalid_data(<<"only 0-9a-zA-Z_-. is allowed in name: ", Name0/binary>>)
    end.

-spec invalid_data(binary()) -> no_return().
invalid_data(Reason) -> throw(#{kind => validation_error, reason => Reason}).

is_id_char(C) when C >= $0 andalso C =< $9 -> true;
is_id_char(C) when C >= $a andalso C =< $z -> true;
is_id_char(C) when C >= $A andalso C =< $Z -> true;
is_id_char($_) -> true;
is_id_char($-) -> true;
is_id_char($.) -> true;
is_id_char(_) -> false.

to_type_atom(Type) ->
    try
        erlang:binary_to_existing_atom(Type, utf8)
    catch
        _:_ ->
            invalid_data(<<"unknown connector type: ", Type/binary>>)
    end.

restart(Type, Name) ->
    emqx_resource:restart(resource_id(Type, Name)).

stop(Type, Name) ->
    emqx_resource:stop(resource_id(Type, Name)).

start(Type, Name) ->
    emqx_resource:start(resource_id(Type, Name)).

create(Type, Name, Conf) ->
    create(Type, Name, Conf, #{}).

create(Type, Name, Conf0, Opts) ->
    ?SLOG(info, #{
        msg => "create connector",
        type => Type,
        name => Name,
        config => emqx_utils:redact(Conf0)
    }),
    TypeBin = bin(Type),
    Conf = Conf0#{connector_type => TypeBin, connector_name => Name},
    {ok, _Data} = emqx_resource:create_local(
        resource_id(Type, Name),
        <<"emqx_connector">>,
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

update(Type, Name, {OldConf, Conf}, Opts) ->
    %% TODO: sometimes its not necessary to restart the connector connection.
    %%
    %% - if the connection related configs like `servers` is updated, we should restart/start
    %% or stop connectors according to the change.
    %% - if the connection related configs are not update, only non-connection configs like
    %% the `method` or `headers` of a WebHook is changed, then the connector can be updated
    %% without restarting the connector.
    %%
    case emqx_utils_maps:if_only_to_toggle_enable(OldConf, Conf) of
        false ->
            ?SLOG(info, #{
                msg => "update connector",
                type => Type,
                name => Name,
                config => emqx_utils:redact(Conf)
            }),
            case recreate(Type, Name, Conf, Opts) of
                {ok, _} ->
                    ok;
                {error, not_found} ->
                    ?SLOG(warning, #{
                        msg => "updating_a_non_existing_connector",
                        type => Type,
                        name => Name,
                        config => emqx_utils:redact(Conf)
                    }),
                    create(Type, Name, Conf, Opts);
                {error, Reason} ->
                    {error, {update_connector_failed, Reason}}
            end;
        true ->
            %% we don't need to recreate the connector if this config change is only to
            %% toggole the config 'connector.{type}.{name}.enable'
            _ =
                case maps:get(enable, Conf, true) of
                    true ->
                        restart(Type, Name);
                    false ->
                        stop(Type, Name)
                end,
            ok
    end.

get_channels(Type, Name) ->
    emqx_resource:get_channels(resource_id(Type, Name)).

recreate(Type, Name) ->
    recreate(Type, Name, emqx:get_config([connectors, Type, Name])).

recreate(Type, Name, Conf) ->
    recreate(Type, Name, Conf, #{}).

recreate(Type, Name, Conf, Opts) ->
    TypeBin = bin(Type),
    emqx_resource:recreate_local(
        resource_id(Type, Name),
        ?MODULE:connector_to_resource_type(Type),
        parse_confs(TypeBin, Name, Conf),
        parse_opts(Conf, Opts)
    ).

create_dry_run(Type, Conf) ->
    create_dry_run(Type, Conf, fun(_) -> ok end).

create_dry_run(Type, Conf0, Callback) ->
    %% Already typechecked, no need to catch errors
    TypeBin = bin(Type),
    TypeAtom = safe_atom(Type),
    %% We use a fixed name here to avoid createing an atom
    TmpName = iolist_to_binary([?TEST_ID_PREFIX, TypeBin, ":", <<"probedryrun">>]),
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
    remove(Type, Name, #{}, #{}).

%% just for perform_connector_changes/1
remove(Type, Name, _Conf, _Opts) ->
    ?SLOG(info, #{msg => "remove_connector", type => Type, name => Name}),
    emqx_resource:remove_local(resource_id(Type, Name)).

%% convert connector configs to what the connector modules want
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
    {BaseUrl, Path} = parse_url(Url1),
    BaseUrl1 =
        case emqx_http_lib:uri_parse(BaseUrl) of
            {ok, BUrl} ->
                BUrl;
            {error, Reason} ->
                Reason1 = emqx_utils:readable_error_msg(Reason),
                invalid_data(<<"Invalid URL: ", Url1/binary, ", details: ", Reason1/binary>>)
        end,
    RequestTTL = emqx_utils_maps:deep_get(
        [resource_opts, request_ttl],
        Conf
    ),
    Conf#{
        base_url => BaseUrl1,
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
                password := Password
            }
    } = Conf,
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    %% This version atom correspond to the macro ?VSN_1_1_X in
    %% emqx_connector_iotdb.hrl. It would be better to use the macro directly, but
    %% this cannot be done without introducing a dependency on the
    %% emqx_iotdb_connector app (which is an EE app).
    DefaultIOTDBConnector = 'v1.1.x',
    Version = maps:get(iotdb_version, Conf, DefaultIOTDBConnector),
    InsertTabletPath =
        case Version of
            DefaultIOTDBConnector -> InsertTabletPathV2;
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
%% TODO: rename this to `kafka_producer' after alias support is added
%% to hocon; keeping this as just `kafka' for backwards compatibility.
parse_confs(ConnectorType, _Name, Config) ->
    connector_config(ConnectorType, Config).

connector_config(ConnectorType, Config) ->
    Mod = connector_impl_module(ConnectorType),
    case erlang:function_exported(Mod, connector_config, 1) of
        true ->
            Mod:connector_config(Config);
        false ->
            Config
    end.

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
            invalid_data(<<"Missing scheme in URL: ", Url/binary>>)
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
