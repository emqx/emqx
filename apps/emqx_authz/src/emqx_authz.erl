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

-module(emqx_authz).
-behaviour(emqx_config_handler).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([
    register_metrics/0,
    init/0,
    deinit/0,
    lookup/0,
    lookup/1,
    move/2,
    update/2,
    authorize/5,
    %% for telemetry information
    get_enabled_authzs/0
]).

-export([post_config_update/5, pre_config_update/3]).

-export([acl_conf_file/0]).

-type source() :: map().

-type match_result() :: {matched, allow} | {matched, deny} | nomatch.

-type default_result() :: allow | deny.

-type authz_result() :: {stop, allow} | {ok, deny}.

-type sources() :: [source()].

-define(METRIC_SUPERUSER, 'authorization.superuser').
-define(METRIC_ALLOW, 'authorization.matched.allow').
-define(METRIC_DENY, 'authorization.matched.deny').
-define(METRIC_NOMATCH, 'authorization.nomatch').

-define(METRICS, [?METRIC_SUPERUSER, ?METRIC_ALLOW, ?METRIC_DENY, ?METRIC_NOMATCH]).

-define(IS_ENABLED(Enable), ((Enable =:= true) or (Enable =:= <<"true">>))).

%% Initialize authz backend.
%% Populate the passed configuration map with necessary data,
%% like `ResourceID`s
-callback create(source()) -> source().

%% Update authz backend.
%% Change configuration, or simply enable/disable
-callback update(source()) -> source().

%% Destroy authz backend.
%% Make cleanup of all allocated data.
%% An authz backend will not be used after `destroy`.
-callback destroy(source()) -> ok.

%% Get authz text description.
-callback description() -> string().

%% Authorize client action.
-callback authorize(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    source()
) -> match_result().

-optional_callbacks([
    update/1
]).

-spec register_metrics() -> ok.
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?METRICS).

init() ->
    ok = register_metrics(),
    ok = init_metrics(client_info_source()),
    emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    Sources = emqx_conf:get(?CONF_KEY_PATH, []),
    ok = check_dup_types(Sources),
    NSources = create_sources(Sources),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [NSources]}, ?HP_AUTHZ).

deinit() ->
    ok = emqx_hooks:del('client.authorize', {?MODULE, authorize}),
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    emqx_authz_utils:cleanup_resources().

lookup() ->
    {_M, _F, [A]} = find_action_in_hooks(),
    A.

lookup(Type) ->
    {Source, _Front, _Rear} = take(Type),
    Source.

move(Type, ?CMD_MOVE_BEFORE(Before)) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, type(Type), ?CMD_MOVE_BEFORE(type(Before))}
    );
move(Type, ?CMD_MOVE_AFTER(After)) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, type(Type), ?CMD_MOVE_AFTER(type(After))}
    );
move(Type, Position) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, type(Type), Position}
    ).

update({?CMD_REPLACE, Type}, Sources) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {{?CMD_REPLACE, type(Type)}, Sources});
update({?CMD_DELETE, Type}, Sources) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {{?CMD_DELETE, type(Type)}, Sources});
update(Cmd, Sources) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {Cmd, Sources}).

pre_config_update(_, Cmd, Sources) ->
    {ok, do_pre_config_update(Cmd, Sources)}.

do_pre_config_update({?CMD_MOVE, _, _} = Cmd, Sources) ->
    do_move(Cmd, Sources);
do_pre_config_update({?CMD_PREPEND, Source}, Sources) ->
    NSource = maybe_write_files(Source),
    NSources = [NSource] ++ Sources,
    ok = check_dup_types(NSources),
    NSources;
do_pre_config_update({?CMD_APPEND, Source}, Sources) ->
    NSource = maybe_write_files(Source),
    NSources = Sources ++ [NSource],
    ok = check_dup_types(NSources),
    NSources;
do_pre_config_update({{?CMD_REPLACE, Type}, Source}, Sources) ->
    NSource = maybe_write_files(Source),
    {_Old, Front, Rear} = take(Type, Sources),
    NSources = Front ++ [NSource | Rear],
    ok = check_dup_types(NSources),
    NSources;
do_pre_config_update({{?CMD_DELETE, Type}, _Source}, Sources) ->
    {_Old, Front, Rear} = take(Type, Sources),
    NSources = Front ++ Rear,
    NSources;
do_pre_config_update({?CMD_REPLACE, Sources}, _OldSources) ->
    %% overwrite the entire config!
    NSources = lists:map(fun maybe_write_files/1, Sources),
    ok = check_dup_types(NSources),
    NSources;
do_pre_config_update({Op, Source}, Sources) ->
    throw({bad_request, #{op => Op, source => Source, sources => Sources}}).

post_config_update(_, _, undefined, _OldSource, _AppEnvs) ->
    ok;
post_config_update(_, Cmd, NewSources, _OldSource, _AppEnvs) ->
    Actions = do_post_config_update(Cmd, NewSources),
    ok = update_authz_chain(Actions),
    ok = emqx_authz_cache:drain_cache().

do_post_config_update({?CMD_MOVE, _Type, _Where} = Cmd, _Sources) ->
    InitedSources = lookup(),
    do_move(Cmd, InitedSources);
do_post_config_update({?CMD_PREPEND, RawNewSource}, Sources) ->
    InitedNewSource = create_source(get_source_by_type(type(RawNewSource), Sources)),
    %% create metrics
    TypeName = type(RawNewSource),
    ok = emqx_metrics_worker:create_metrics(
        authz_metrics,
        TypeName,
        [total, allow, deny, nomatch],
        [total]
    ),
    [InitedNewSource] ++ lookup();
do_post_config_update({?CMD_APPEND, RawNewSource}, Sources) ->
    InitedNewSource = create_source(get_source_by_type(type(RawNewSource), Sources)),
    lookup() ++ [InitedNewSource];
do_post_config_update({{?CMD_REPLACE, Type}, RawNewSource}, Sources) ->
    OldSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldSources),
    NewSource = get_source_by_type(type(RawNewSource), Sources),
    InitedSources = update_source(type(RawNewSource), OldSource, NewSource),
    Front ++ [InitedSources] ++ Rear;
do_post_config_update({{?CMD_DELETE, Type}, _RawNewSource}, _Sources) ->
    OldInitedSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldInitedSources),
    %% delete metrics
    ok = emqx_metrics_worker:clear_metrics(authz_metrics, Type),
    ok = ensure_resource_deleted(OldSource),
    clear_certs(OldSource),
    Front ++ Rear;
do_post_config_update({?CMD_REPLACE, _RawNewSources}, Sources) ->
    %% overwrite the entire config!
    OldInitedSources = lookup(),
    lists:foreach(fun ensure_resource_deleted/1, OldInitedSources),
    lists:foreach(fun clear_certs/1, OldInitedSources),
    create_sources(Sources).

%% @doc do source move
do_move({?CMD_MOVE, Type, ?CMD_MOVE_FRONT}, Sources) ->
    {Source, Front, Rear} = take(Type, Sources),
    [Source | Front] ++ Rear;
do_move({?CMD_MOVE, Type, ?CMD_MOVE_REAR}, Sources) ->
    {Source, Front, Rear} = take(Type, Sources),
    Front ++ Rear ++ [Source];
do_move({?CMD_MOVE, Type, ?CMD_MOVE_BEFORE(Before)}, Sources) ->
    {S1, Front1, Rear1} = take(Type, Sources),
    {S2, Front2, Rear2} = take(Before, Front1 ++ Rear1),
    Front2 ++ [S1, S2] ++ Rear2;
do_move({?CMD_MOVE, Type, ?CMD_MOVE_AFTER(After)}, Sources) ->
    {S1, Front1, Rear1} = take(Type, Sources),
    {S2, Front2, Rear2} = take(After, Front1 ++ Rear1),
    Front2 ++ [S2, S1] ++ Rear2.

ensure_resource_deleted(#{enable := false}) ->
    ok;
ensure_resource_deleted(#{type := Type} = Source) ->
    Module = authz_module(Type),
    Module:destroy(Source).

check_dup_types(Sources) ->
    check_dup_types(Sources, []).

check_dup_types([], _Checked) ->
    ok;
check_dup_types([Source | Sources], Checked) ->
    %% the input might be raw or type-checked result, so lookup both 'type' and <<"type">>
    %% TODO: check: really?
    Type =
        case maps:get(<<"type">>, Source, maps:get(type, Source, undefined)) of
            undefined ->
                %% this should never happen if the value is type checked by honcon schema
                throw({bad_source_input, Source});
            Type0 ->
                type(Type0)
        end,
    case lists:member(Type, Checked) of
        true ->
            %% we have made it clear not to support more than one authz instance for each type
            throw({duplicated_authz_source_type, Type});
        false ->
            check_dup_types(Sources, [Type | Checked])
    end.

create_sources(Sources) ->
    {_Enabled, Disabled} = lists:partition(fun(#{enable := Enable}) -> Enable end, Sources),
    case Disabled =/= [] of
        true -> ?SLOG(info, #{msg => "disabled_sources_ignored", sources => Disabled});
        false -> ok
    end,
    ok = lists:foreach(fun init_metrics/1, Sources),
    lists:map(fun create_source/1, Sources).

create_source(#{type := Type} = Source) ->
    Module = authz_module(Type),
    Module:create(Source).

update_source(Type, OldSource, NewSource) ->
    Module = authz_module(Type),
    Module:update(maps:merge(OldSource, NewSource)).

init_metrics(Source) ->
    TypeName = type(Source),
    emqx_metrics_worker:create_metrics(
        authz_metrics,
        TypeName,
        [total, allow, deny, nomatch],
        [total]
    ).

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec authorize(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    default_result(),
    sources()
) ->
    authz_result().
authorize(
    #{
        username := Username,
        peerhost := IpAddress
    } = Client,
    PubSub,
    Topic,
    DefaultResult,
    Sources
) ->
    case maps:get(is_superuser, Client, false) of
        true ->
            log_allowed(#{
                username => Username,
                ipaddr => IpAddress,
                topic => Topic,
                is_superuser => true
            }),
            emqx_metrics:inc(?METRIC_SUPERUSER),
            {stop, allow};
        false ->
            authorize_non_superuser(Client, PubSub, Topic, DefaultResult, Sources)
    end.

authorize_non_superuser(
    #{
        username := Username,
        peerhost := IpAddress
    } = Client,
    PubSub,
    Topic,
    DefaultResult,
    Sources
) ->
    case do_authorize(Client, PubSub, Topic, sources_with_defaults(Sources)) of
        {{matched, allow}, AuthzSource} ->
            emqx:run_hook(
                'client.check_authz_complete',
                [Client, PubSub, Topic, allow, AuthzSource]
            ),
            log_allowed(#{
                username => Username,
                ipaddr => IpAddress,
                topic => Topic,
                source => AuthzSource
            }),
            emqx_metrics_worker:inc(authz_metrics, AuthzSource, allow),
            emqx_metrics:inc(?METRIC_ALLOW),
            {stop, allow};
        {{matched, deny}, AuthzSource} ->
            emqx:run_hook(
                'client.check_authz_complete',
                [Client, PubSub, Topic, deny, AuthzSource]
            ),
            ?SLOG(warning, #{
                msg => "authorization_permission_denied",
                username => Username,
                ipaddr => IpAddress,
                topic => Topic,
                source => AuthzSource
            }),
            emqx_metrics_worker:inc(authz_metrics, AuthzSource, deny),
            emqx_metrics:inc(?METRIC_DENY),
            {stop, deny};
        nomatch ->
            emqx:run_hook(
                'client.check_authz_complete',
                [Client, PubSub, Topic, DefaultResult, default]
            ),
            ?SLOG(info, #{
                msg => "authorization_failed_nomatch",
                username => Username,
                ipaddr => IpAddress,
                topic => Topic,
                reason => "no-match rule"
            }),
            emqx_metrics:inc(?METRIC_NOMATCH),
            {stop, DefaultResult}
    end.

log_allowed(Meta) ->
    ?SLOG(info, Meta#{msg => "authorization_permission_allowed"}).

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [#{enable := false} | Rest]) ->
    do_authorize(Client, PubSub, Topic, Rest);
do_authorize(
    Client,
    PubSub,
    Topic,
    [Connector = #{type := Type} | Tail]
) ->
    Module = authz_module(Type),
    emqx_metrics_worker:inc(authz_metrics, Type, total),
    try Module:authorize(Client, PubSub, Topic, Connector) of
        nomatch ->
            emqx_metrics_worker:inc(authz_metrics, Type, nomatch),
            do_authorize(Client, PubSub, Topic, Tail);
        Matched ->
            {Matched, Type}
    catch
        Class:Reason:Stacktrace ->
            emqx_metrics_worker:inc(authz_metrics, Type, nomatch),
            ?SLOG(warning, #{
                msg => "unexpected_error_in_authorize",
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace,
                authorize_type => Type
            }),
            do_authorize(Client, PubSub, Topic, Tail)
    end.

get_enabled_authzs() ->
    lists:usort([Type || #{type := Type} <- lookup()]).

%%--------------------------------------------------------------------
%% Internal function
%%--------------------------------------------------------------------

client_info_source() ->
    emqx_authz_client_info:create(
        #{type => client_info, enable => true}
    ).

sources_with_defaults(Sources) ->
    [client_info_source() | Sources].

take(Type) -> take(Type, lookup()).

%% Take the source of give type, the sources list is split into two parts
%% front part and rear part.
take(Type, Sources) ->
    {Front, Rear} = lists:splitwith(fun(T) -> type(T) =/= type(Type) end, Sources),
    case Rear =:= [] of
        true ->
            throw({not_found_source, Type});
        _ ->
            {hd(Rear), Front, tl(Rear)}
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback, {?MODULE, authorize, _} = Action, _, _} <- Callbacks],
    Action.

authz_module(built_in_database) ->
    emqx_authz_mnesia;
authz_module(Type) ->
    list_to_existing_atom("emqx_authz_" ++ atom_to_list(Type)).

type(#{type := Type}) -> type(Type);
type(#{<<"type">> := Type}) -> type(Type);
type(file) -> file;
type(<<"file">>) -> file;
type(http) -> http;
type(<<"http">>) -> http;
type(mongodb) -> mongodb;
type(<<"mongodb">>) -> mongodb;
type(mysql) -> mysql;
type(<<"mysql">>) -> mysql;
type(redis) -> redis;
type(<<"redis">>) -> redis;
type(postgresql) -> postgresql;
type(<<"postgresql">>) -> postgresql;
type(built_in_database) -> built_in_database;
type(<<"built_in_database">>) -> built_in_database;
type(client_info) -> client_info;
type(<<"client_info">>) -> client_info;
%% should never happen if the input is type-checked by hocon schema
type(Unknown) -> throw({unknown_authz_source_type, Unknown}).

maybe_write_files(#{<<"type">> := <<"file">>} = Source) ->
    write_acl_file(Source);
maybe_write_files(NewSource) ->
    maybe_write_certs(NewSource).

write_acl_file(#{<<"rules">> := Rules} = Source) ->
    NRules = check_acl_file_rules(Rules),
    Path = ?MODULE:acl_conf_file(),
    {ok, _Filename} = write_file(Path, NRules),
    maps:without([<<"rules">>], Source#{<<"path">> => Path}).

%% @doc where the acl.conf file is stored.
acl_conf_file() ->
    filename:join([emqx:data_dir(), "authz", "acl.conf"]).

maybe_write_certs(#{<<"type">> := Type} = Source) ->
    case
        emqx_tls_lib:ensure_ssl_files(
            ssl_file_path(Type), maps:get(<<"ssl">>, Source, undefined)
        )
    of
        {ok, SSL} ->
            new_ssl_source(Source, SSL);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

clear_certs(OldSource) ->
    OldSSL = maps:get(ssl, OldSource, undefined),
    ok = emqx_tls_lib:delete_ssl_files(ssl_file_path(type(OldSource)), undefined, OldSSL).

write_file(Filename, Bytes) ->
    ok = filelib:ensure_dir(Filename),
    case file:write_file(Filename, Bytes) of
        ok ->
            {ok, iolist_to_binary(Filename)};
        {error, Reason} ->
            ?SLOG(error, #{filename => Filename, msg => "write_file_error", reason => Reason}),
            throw(Reason)
    end.

ssl_file_path(Type) ->
    filename:join(["authz", Type]).

new_ssl_source(Source, undefined) ->
    Source;
new_ssl_source(Source, SSL) ->
    Source#{<<"ssl">> => SSL}.

get_source_by_type(Type, Sources) ->
    {Source, _Front, _Rear} = take(Type, Sources),
    Source.

%% @doc put hook with (maybe) initialized new source and old sources
update_authz_chain(Actions) ->
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [Actions]}, ?HP_AUTHZ).

check_acl_file_rules(RawRules) ->
    %% TODO: make sure the bin rules checked
    RawRules.
