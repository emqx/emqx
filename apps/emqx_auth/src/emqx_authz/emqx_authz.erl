%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    register_source/2,
    unregister_source/1,
    init/0,
    deinit/0,
    format_for_api/1,
    lookup_states/0,
    lookup_state/1,
    move/2,
    reorder/1,
    update/2,
    merge/1,
    merge_local/2,
    authorize/5,
    authorize_deny/4,
    %% for telemetry information
    get_enabled_authzs/0,
    %% for API
    maybe_read_source_files/1
]).

%% Config update hooks
-export([pre_config_update/4, post_config_update/5]).

%% Data backup
-export([
    import_config/1,
    maybe_read_files/1,
    maybe_write_files/1
]).

-import(emqx_utils_conv, [bin/1]).

-type default_result() :: allow | deny.

-type authz_result_value() :: #{result := allow | deny, from => atom()}.
-type authz_result() :: {stop, authz_result_value()} | {ok, authz_result_value()} | ignore.

-type raw_source() :: emqx_config:raw_config().
-type source() :: emqx_config:config().
-type source_state() :: emqx_authz_source:source_state().
-type source_states() :: [source_state()].
-type type() :: atom().

-define(METRIC_SUPERUSER, 'authorization.superuser').
-define(METRIC_ALLOW, 'authorization.matched.allow').
-define(METRIC_DENY, 'authorization.matched.deny').
-define(METRIC_NOMATCH, 'authorization.nomatch').

-define(METRICS, [?METRIC_SUPERUSER, ?METRIC_ALLOW, ?METRIC_DENY, ?METRIC_NOMATCH]).

init() ->
    ok = register_metrics(),
    emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    emqx_conf:add_handler(?ROOT_KEY, ?MODULE),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize_deny, []}, ?HP_AUTHZ),
    ok = register_builtin_sources(),
    ok.

register_source(Type, Module) ->
    ok = emqx_authz_source_registry:register(Type, Module),
    install_sources(not is_hook_installed() andalso are_all_providers_registered()).

unregister_source(Type) ->
    ok = emqx_authz_source_registry:unregister(Type).

is_hook_installed() ->
    lists:any(
        fun(Callback) ->
            case emqx_hooks:callback_action(Callback) of
                {?MODULE, authorize, _} -> true;
                _ -> false
            end
        end,
        emqx_hooks:lookup('client.authorize')
    ).

are_all_providers_registered() ->
    try
        _ = lists:foreach(
            fun(Type) ->
                _ = emqx_authz_source_registry:module(Type)
            end,
            configured_types()
        ),
        true
    catch
        {unknown_authz_source_type, _Type} ->
            false
    end.

register_metrics() ->
    ok = lists:foreach(fun emqx_metrics:ensure/1, ?METRICS).

register_builtin_sources() ->
    lists:foreach(
        fun({Type, Module}) ->
            register_source(Type, Module)
        end,
        ?BUILTIN_SOURCES
    ).

configured_types() ->
    lists:map(
        fun(Source) -> type(Source) end,
        emqx:get_config(?CONF_KEY_PATH, [])
    ).

install_sources(true) ->
    ok = init_metrics(client_info_source()),
    Sources = emqx:get_config(?CONF_KEY_PATH, []),
    ok = check_dup_types_for_sources(Sources),
    SourceStates = create_sources(Sources),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [SourceStates]}, ?HP_AUTHZ),
    ok = emqx_hooks:del('client.authorize', {?MODULE, authorize_deny});
install_sources(false) ->
    ok.

deinit() ->
    ok = emqx_hooks:del('client.authorize', {?MODULE, authorize}),
    ok = emqx_hooks:del('client.authorize', {?MODULE, authorize_deny}),
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    emqx_conf:remove_handler(?ROOT_KEY),
    emqx_authz_utils:cleanup_resources().

lookup_states() ->
    {?MODULE, authorize, [SourceStates]} = find_authz_action_in_hooks(),
    SourceStates.

lookup_state(Type) ->
    {SourceState, _Front, _Rear} = split_states_by_type(to_type(Type)),
    SourceState.

merge(NewConf) ->
    emqx_authz_utils:update_config(?ROOT_KEY, {?CMD_MERGE, NewConf}).

merge_local(NewConf, Opts) ->
    emqx:update_config(?ROOT_KEY, {?CMD_MERGE, NewConf}, Opts).

move(Type, ?CMD_MOVE_BEFORE(BeforeType)) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, to_type(Type), ?CMD_MOVE_BEFORE(to_type(BeforeType))}
    );
move(Type, ?CMD_MOVE_AFTER(AfterType)) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, to_type(Type), ?CMD_MOVE_AFTER(to_type(AfterType))}
    );
move(Type, Position) ->
    emqx_authz_utils:update_config(
        ?CONF_KEY_PATH, {?CMD_MOVE, to_type(Type), Position}
    ).

reorder(Types) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {?CMD_REORDER, Types}).

update({?CMD_REPLACE, Type}, Source) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {{?CMD_REPLACE, to_type(Type)}, Source});
update({?CMD_DELETE, Type}, Source) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {{?CMD_DELETE, to_type(Type)}, Source});
update(Cmd, Sources) ->
    emqx_authz_utils:update_config(?CONF_KEY_PATH, {Cmd, Sources}).

pre_config_update(Path, Cmd, OldConf, ClusterRpcOpts) ->
    try do_pre_config_update(Path, Cmd, OldConf, ClusterRpcOpts) of
        {error, Reason} -> {error, Reason};
        NewConf -> {ok, NewConf}
    catch
        throw:Reason ->
            ?SLOG(info, #{
                msg => "error_in_pre_config_update",
                reason => Reason
            }),
            {error, Reason};
        Error:Reason:Stack ->
            ?SLOG(warning, #{
                msg => "error_in_pre_config_update",
                exception => Error,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    end.

do_pre_config_update(?CONF_KEY_PATH, Cmd, Sources, Opts) ->
    do_pre_config_update(Cmd, Sources, Opts);
do_pre_config_update(?ROOT_KEY, {?CMD_MERGE, NewConf}, OldConf, Opts) ->
    do_pre_config_merge(NewConf, OldConf, Opts);
do_pre_config_update(?ROOT_KEY, NewConf, OldConf, Opts) ->
    do_pre_config_replace(NewConf, OldConf, Opts).

do_pre_config_merge(NewConf, OldConf, Opts) ->
    MergeConf = emqx_utils_maps:deep_merge(OldConf, NewConf),
    NewSources = merge_sources(OldConf, NewConf),
    do_pre_config_replace(MergeConf#{<<"sources">> => NewSources}, OldConf, Opts).

%% override the entire config when updating the root key
%% emqx_conf:update(?ROOT_KEY, Conf);
do_pre_config_replace(Conf, Conf, _Opts) ->
    Conf;
do_pre_config_replace(NewConf, OldConf, Opts) ->
    NewSources = get_sources(NewConf),
    OldSources = get_sources(OldConf),
    ReplaceSources = do_pre_config_update({?CMD_REPLACE, NewSources}, OldSources, Opts),
    NewConf#{<<"sources">> => ReplaceSources}.

do_pre_config_update({?CMD_MOVE, _, _} = Cmd, Sources, _Opts) ->
    do_move(Cmd, Sources);
do_pre_config_update({?CMD_PREPEND, Source}, Sources, _Opts) ->
    NSource = maybe_write_source_files(Source),
    NSources = [NSource | Sources],
    ok = check_dup_types_for_sources(NSources),
    NSources;
do_pre_config_update({?CMD_APPEND, Source}, Sources, _Opts) ->
    NSource = maybe_write_source_files(Source),
    NSources = Sources ++ [NSource],
    ok = check_dup_types_for_sources(NSources),
    NSources;
do_pre_config_update({{?CMD_REPLACE, Type}, Source}, Sources, _Opts) ->
    NSource = maybe_write_source_files(Source),
    {_Old, Front, Rear} = split_by_type(Type, Sources),
    NSources = Front ++ [NSource | Rear],
    ok = check_dup_types_for_sources(NSources),
    NSources;
do_pre_config_update({{?CMD_DELETE, Type}, _Source}, Sources, Opts) ->
    case take_by_type(Type, Sources) of
        not_found ->
            case emqx_cluster_rpc:is_initiator(Opts) of
                true -> throw({not_found_source, Type});
                false -> Sources
            end;
        {_Found, NSources} ->
            NSources
    end;
do_pre_config_update({?CMD_REPLACE, Sources}, _OldSources, _Opts) ->
    %% overwrite the entire config!
    NSources = lists:map(fun maybe_write_source_files/1, Sources),
    ok = check_dup_types_for_sources(NSources),
    NSources;
do_pre_config_update({?CMD_REORDER, Types}, OldSources, _Opts) ->
    reorder_sources(Types, OldSources);
do_pre_config_update({Op, Source}, Sources, _Opts) ->
    throw({bad_request, #{op => Op, source => Source, sources => Sources}}).

post_config_update(_, _, undefined, _OldConf, _AppEnvs) ->
    ok;
post_config_update(Path, Cmd, NewConf, _OldConf, _AppEnvs) ->
    NewSourceStates = do_post_config_update(Path, Cmd, NewConf),
    ok = update_authz_chain(NewSourceStates),
    ok = emqx_authz_cache:drain_cache().

do_post_config_update(?CONF_KEY_PATH, {?CMD_MOVE, _Type, _Where} = Cmd, _Sources) ->
    do_move(Cmd, lookup_states());
do_post_config_update(?CONF_KEY_PATH, {?CMD_PREPEND, NewSource}, Sources) ->
    Type = type(NewSource),
    [NewSourceState] = create_sources([get_source_by_type(Type, Sources)]),
    [NewSourceState | lookup_states()];
do_post_config_update(?CONF_KEY_PATH, {?CMD_APPEND, NewSource}, Sources) ->
    [NewSourceState] = create_sources([get_source_by_type(type(NewSource), Sources)]),
    lookup_states() ++ [NewSourceState];
do_post_config_update(?CONF_KEY_PATH, {{?CMD_REPLACE, Type}, _NewSource}, Sources) ->
    OldSourceStates = lookup_states(),
    {OldSourceState, Front, Rear} = split_by_type(Type, OldSourceStates),
    NewSource = get_source_by_type(Type, Sources),
    NewSourceState = update_source(Type, OldSourceState, NewSource),
    Front ++ [NewSourceState] ++ Rear;
do_post_config_update(?CONF_KEY_PATH, {{?CMD_DELETE, Type}, _NewSource}, _Sources) ->
    OldSourceStates = lookup_states(),
    case take_by_type(Type, OldSourceStates) of
        not_found ->
            OldSourceStates;
        {FoundSourceState, NSourceStates} ->
            ok = ensure_deleted(FoundSourceState, #{clear_metric => true}),
            NSourceStates
    end;
do_post_config_update(?CONF_KEY_PATH, {?CMD_REPLACE, _NewSources}, Sources) ->
    overwrite_entire_sources(Sources);
do_post_config_update(?CONF_KEY_PATH, {?CMD_REORDER, RawTypes}, _Sources) ->
    OldSourceStates = lookup_states(),
    lists:map(
        fun(RawType) ->
            Type = to_type(RawType),
            {value, SourceState} = lists:search(
                fun(SSt) -> type(SSt) =:= Type end, OldSourceStates
            ),
            SourceState
        end,
        RawTypes
    );
do_post_config_update(?ROOT_KEY, Conf, Conf) ->
    lookup_states();
do_post_config_update(?ROOT_KEY, _Conf, NewConf) ->
    #{sources := NewSources} = NewConf,
    overwrite_entire_sources(NewSources).

overwrite_entire_sources(Sources) ->
    PrevSourceStates = lookup_states(),
    PrevTypes = sets:from_list(lists:map(fun type/1, PrevSourceStates), [{version, 2}]),
    NewTypes = sets:from_list(lists:map(fun type/1, Sources), [{version, 2}]),
    RemovedTypes = sets:subtract(PrevTypes, NewTypes),
    AddedTypes = sets:subtract(NewTypes, PrevTypes),

    %% Remove sources
    RemovedSourceStates = lists:filter(
        fun(SourceState) -> sets:is_element(type(SourceState), RemovedTypes) end,
        PrevSourceStates
    ),
    ok = lists:foreach(
        fun(SourceState) -> ensure_deleted(SourceState, #{clear_metric => true}) end,
        RemovedSourceStates
    ),

    %% Construct the new source states
    %% NOTE
    %% It's important to preserve the order of the passed Sources
    SourceStates = lists:map(
        fun(Source) ->
            Type = type(Source),
            case sets:is_element(Type, AddedTypes) of
                true ->
                    [State] = create_sources([Source]),
                    State;
                false ->
                    {value, PrevSourceState} = lists:search(
                        fun(SSt) -> type(SSt) =:= Type end, PrevSourceStates
                    ),
                    update_source(Type, PrevSourceState, Source)
            end
        end,
        Sources
    ),
    SourceStates.

%% @doc do source move
do_move({?CMD_MOVE, Type, ?CMD_MOVE_FRONT}, SourceStates) ->
    {SourceState, Front, Rear} = split_by_type(Type, SourceStates),
    [SourceState | Front] ++ Rear;
do_move({?CMD_MOVE, Type, ?CMD_MOVE_REAR}, SourceStates) ->
    {SourceState, Front, Rear} = split_by_type(Type, SourceStates),
    Front ++ Rear ++ [SourceState];
do_move({?CMD_MOVE, Type, ?CMD_MOVE_BEFORE(Before)}, SourceStates) ->
    {SourceState1, Front1, Rear1} = split_by_type(Type, SourceStates),
    {SourceState2, Front2, Rear2} = split_by_type(Before, Front1 ++ Rear1),
    Front2 ++ [SourceState1, SourceState2] ++ Rear2;
do_move({?CMD_MOVE, Type, ?CMD_MOVE_AFTER(After)}, SourceStates) ->
    {SourceState1, Front1, Rear1} = split_by_type(Type, SourceStates),
    {SourceState2, Front2, Rear2} = split_by_type(After, Front1 ++ Rear1),
    Front2 ++ [SourceState2, SourceState1] ++ Rear2.

ensure_deleted(#{enable := false}, _) ->
    ok;
ensure_deleted(SourceState, #{clear_metric := ClearMetric}) ->
    Type = type(SourceState),
    Module = authz_module(Type),
    Module:destroy(SourceState),
    ClearMetric andalso emqx_metrics_worker:clear_metrics(authz_metrics, Type).

%% Called for both sources and raw sources
check_dup_types_for_sources(Sources) ->
    check_dup_types([type(S) || S <- Sources]).

check_dup_types(Types) ->
    Duplicates = lists:uniq(Types -- lists:uniq(Types)),
    case Duplicates of
        [] ->
            ok;
        _ ->
            throw({duplicated_authz_source_type, Duplicates})
    end.

create_sources(Sources) ->
    {_Enabled, Disabled} = lists:partition(fun(#{enable := Enable}) -> Enable end, Sources),
    case Disabled =/= [] of
        true -> ?SLOG(info, #{msg => "disabled_sources", sources => Disabled});
        false -> ok
    end,
    ok = lists:foreach(fun init_metrics/1, Sources),
    lists:map(fun create_source/1, Sources).

create_source(#{type := Type} = Source) ->
    Module = authz_module(Type),
    Module:create(Source).

update_source(Type, OldSourceState, NewSource) ->
    Module = authz_module(Type),
    Module:update(OldSourceState, NewSource).

init_metrics(Source) ->
    Type = type(Source),
    case emqx_metrics_worker:has_metrics(authz_metrics, Type) of
        %% Don't reset the metrics if it already exists
        true ->
            ok;
        false ->
            emqx_metrics_worker:create_metrics(
                authz_metrics,
                Type,
                [total, allow, deny, nomatch, ignore],
                [total]
            )
    end.

%%------------------------------------------------------------------------------
%% AuthZ callbacks
%%------------------------------------------------------------------------------

-spec authorize_deny(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    default_result()
) ->
    {stop, #{result => deny, from => ?MODULE}}.
authorize_deny(
    #{
        username := Username
    } = _Client,
    _PubSub,
    Topic,
    _DefaultResult
) ->
    emqx_metrics:inc(?METRIC_DENY),
    ?SLOG(warning, #{
        msg => "authorization_not_initialized",
        username => Username,
        topic => Topic,
        source => ?MODULE
    }),
    {stop, #{result => deny, from => ?MODULE}}.

%% @doc Check AuthZ.
%% DefaultResult is always ignored in this callback because the final decision
%% is to be made by `emqx_access_control' module after all authorization
%% sources are exhausted.
-spec authorize(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    default_result(),
    source_states()
) ->
    authz_result().
authorize(#{username := Username} = Client, PubSub, Topic, _DefaultResult, SourceStates) ->
    case maps:get(is_superuser, Client, false) of
        true ->
            ?tp(authz_skipped, #{reason => client_is_superuser, action => PubSub}),
            ?TRACE("AUTHZ", "authorization_skipped_as_superuser", #{
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            }),
            emqx_metrics:inc(?METRIC_SUPERUSER),
            {stop, #{result => allow, from => superuser}};
        false ->
            authorize_non_superuser(Client, PubSub, Topic, SourceStates)
    end.

authorize_non_superuser(Client, PubSub, Topic, SourceStates) ->
    case do_authorize(Client, PubSub, Topic, source_states_with_defaults(SourceStates)) of
        {{matched, allow}, MatchedType} ->
            emqx_metrics_worker:inc(authz_metrics, MatchedType, allow),
            emqx_metrics:inc(?METRIC_ALLOW),
            {stop, #{result => allow, from => source_for_logging(MatchedType, Client)}};
        {{matched, deny}, MatchedType} ->
            emqx_metrics_worker:inc(authz_metrics, MatchedType, deny),
            emqx_metrics:inc(?METRIC_DENY),
            {stop, #{result => deny, from => source_for_logging(MatchedType, Client)}};
        nomatch ->
            ?tp(authz_non_superuser, #{result => nomatch}),
            emqx_metrics:inc(?METRIC_NOMATCH),
            %% return ignore here because there might be other hook callbacks
            ignore
    end.

source_for_logging(client_info, #{acl := Acl}) ->
    maps:get(source_for_logging, Acl, client_info);
source_for_logging(Type, _) ->
    Type.

do_authorize(_Client, _Action, _Topic, []) ->
    nomatch;
do_authorize(Client, Action, Topic, [#{enable := false} = _SourceState | SourceStates]) ->
    do_authorize(Client, Action, Topic, SourceStates);
do_authorize(
    #{
        username := Username
    } = Client,
    Action = ?authz_action(_PubSub),
    Topic,
    [SourceState | SourceStates]
) ->
    Type = type(SourceState),
    Module = authz_module(Type),
    emqx_metrics_worker:inc(authz_metrics, Type, total),
    Result = ?EXT_TRACE_CLIENT_AUTHZ_BACKEND(
        ?EXT_TRACE_ATTR(#{
            'client.clientid' => maps:get(clientid, Client, undefined),
            'client.username' => Username,
            'authz.module' => Module,
            'authz.backend_type' => Type,
            'authz.topic' => Topic,
            'authz.action_type' => _PubSub
        }),
        fun() ->
            Result0 =
                try Module:authorize(Client, Action, Topic, SourceState) of
                    Res ->
                        ok = inc_metrics(Type, Res),
                        ok = log_trace(Res, Type, Module, Username, Topic, Action),
                        Res
                catch
                    Class:Reason:Stacktrace ->
                        ok = inc_metrics(Type, nomatch),
                        ?SLOG(warning, #{
                            msg => "unexpected_error_in_authorize",
                            exception => Class,
                            reason => Reason,
                            stacktrace => Stacktrace,
                            authorize_type => Type
                        }),
                        error
                end,
            ?EXT_TRACE_ADD_ATTRS(#{'authz.result' => format_result(Result0)}),
            Result0
        end,
        []
    ),

    case Result of
        error ->
            do_authorize(Client, Action, Topic, SourceStates);
        nomatch ->
            do_authorize(Client, Action, Topic, SourceStates);
        ignore ->
            do_authorize(Client, Action, Topic, SourceStates);
        {matched, ignore} ->
            do_authorize(Client, Action, Topic, SourceStates);
        {matched, _Permission} = Matched ->
            {Matched, Type}
    end.

inc_metrics(Type, nomatch) ->
    emqx_metrics_worker:inc(authz_metrics, Type, nomatch);
inc_metrics(Type, ignore) ->
    emqx_metrics_worker:inc(authz_metrics, Type, ignore);
inc_metrics(Type, {matched, ignore}) ->
    emqx_metrics_worker:inc(authz_metrics, Type, ignore);
inc_metrics(_, {matched, _}) ->
    ok.

log_trace(Res, Type, Module, Username, Topic, PubSub) ->
    case Res of
        nomatch ->
            ?TRACE("AUTHZ", "authorization_nomatch", #{
                authorize_type => Type,
                module => Module,
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            });
        ignore ->
            ?TRACE("AUTHZ", "authorization_module_ignore", #{
                authorize_type => Type,
                module => Module,
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            });
        {matched, ignore} ->
            ?TRACE("AUTHZ", "authorization_matched_ignore", #{
                authorize_type => Type,
                module => Module,
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            });
        {matched, allow} ->
            ?TRACE("AUTHZ", "authorization_matched_allow", #{
                authorize_type => Type,
                module => Module,
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            });
        {matched, deny} ->
            ?TRACE("AUTHZ", "authorization_matched_deny", #{
                authorize_type => Type,
                module => Module,
                username => Username,
                topic => Topic,
                action => emqx_access_control:format_action(PubSub)
            })
    end.

-if(?EMQX_RELEASE_EDITION == ee).
format_result(error) ->
    error;
format_result(nomatch) ->
    nomatch;
format_result(ignore) ->
    ignore;
format_result({matched, ignore}) ->
    matched_ignore;
format_result({matched, allow}) ->
    matched_allow;
format_result({matched, deny}) ->
    matched_deny.
-else.
-endif.

get_enabled_authzs() ->
    lists:usort([Type || #{type := Type, enable := true} <- lookup_states()]).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

import_config(#{?CONF_NS_BINARY := AuthzConf}) ->
    Sources = get_sources(AuthzConf),
    OldSources = emqx:get_raw_config(?CONF_KEY_PATH, [emqx_authz_schema:default_authz()]),
    MergedSources = emqx_utils:merge_lists(OldSources, Sources, fun type/1),
    MergedAuthzConf = AuthzConf#{<<"sources">> => MergedSources},
    case emqx_conf:update([?CONF_NS_ATOM], MergedAuthzConf, #{override_to => cluster}) of
        {ok, #{raw_config := #{<<"sources">> := NewSources}}} ->
            {ok, #{
                root_key => ?CONF_NS_ATOM,
                changed => changed_paths(OldSources, NewSources)
            }};
        Error ->
            {error, #{root_key => ?CONF_NS_ATOM, reason => Error}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_NS_ATOM, changed => []}}.

changed_paths(OldSources, NewSources) ->
    Changed = maps:get(changed, emqx_utils:diff_lists(NewSources, OldSources, fun type/1)),
    [?CONF_KEY_PATH ++ [type(OldSource)] || {OldSource, _} <- Changed].

maybe_read_files(RawConf) ->
    maybe_convert_sources(RawConf, fun maybe_read_source_files/1).

maybe_write_files(RawConf) ->
    maybe_convert_sources(RawConf, fun maybe_write_source_files/1).

maybe_convert_sources(
    #{?CONF_NS_BINARY := #{<<"sources">> := Sources} = AuthRawConf} = RawConf, Fun
) ->
    Sources1 = lists:map(Fun, Sources),
    RawConf#{?CONF_NS_BINARY => AuthRawConf#{<<"sources">> => Sources1}};
maybe_convert_sources(RawConf, _Fun) ->
    RawConf.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

client_info_source() ->
    #{type => client_info, enable => true}.

client_info_source_state() ->
    emqx_authz_client_info:create(client_info_source()).

source_states_with_defaults(SourceStates) ->
    [client_info_source_state() | SourceStates].

take_by_type(Type, Sources) ->
    try split_by_type(Type, Sources) of
        {Found, Front, Rear} -> {Found, Front ++ Rear}
    catch
        throw:{type_not_found, Type} -> not_found
    end.

split_states_by_type(Type) -> split_by_type(Type, lookup_states()).

%% Take the source/ of give type, the sources list is split into two parts
%% front part and rear part.
split_by_type(Type, Values) ->
    case lists:splitwith(fun(Value) -> type(Value) =/= Type end, Values) of
        {_Front, []} ->
            throw({type_not_found, Type});
        {Front, [Found | Rear]} ->
            {Found, Front, Rear}
    end.

find_authz_action_in_hooks() ->
    Actions = lists:filtermap(
        fun(Callback) ->
            case emqx_hooks:callback_action(Callback) of
                {?MODULE, authorize, _} = Action -> {true, Action};
                _ -> false
            end
        end,
        emqx_hooks:lookup('client.authorize')
    ),
    case Actions of
        [] ->
            ?SLOG(error, #{
                msg => "authz_not_initialized",
                configured_types => configured_types(),
                registered_types => emqx_authz_source_registry:registered_types()
            }),
            error(authz_not_initialized);
        [Action] ->
            Action
    end.

authz_module(Type) ->
    emqx_authz_source_registry:module(Type).

-spec type(source() | raw_source() | source_state()) -> type().
%% Extracts type from source raw config
type(#{<<"type">> := Type}) ->
    to_type(Type);
%% Extracts type from source or source_state
type(#{type := Type}) ->
    to_type(Type).

%% Converts to authorizer type and validates it
to_type(Type) when is_atom(Type) ->
    _Module = emqx_authz_source_registry:module(Type),
    Type;
to_type(TypeBin) when is_binary(TypeBin) ->
    try binary_to_existing_atom(TypeBin, utf8) of
        Type ->
            to_type(Type)
    catch
        error:badarg ->
            throw({unknown_authz_source_type, TypeBin})
    end.

format_for_api(RawSource) ->
    Type = type(RawSource),
    Mod = authz_module(Type),
    try
        Mod:format_for_api(RawSource)
    catch
        error:undef ->
            RawSource
    end.

maybe_write_source_files(Source) ->
    Module = authz_module(type(Source)),
    ok = emqx_utils:interactive_load(Module),
    case erlang:function_exported(Module, write_files, 1) of
        true ->
            Module:write_files(Source);
        false ->
            maybe_write_certs(Source)
    end.

maybe_read_source_files(Source) ->
    Module = authz_module(type(Source)),
    case erlang:function_exported(Module, read_files, 1) of
        true ->
            Module:read_files(Source);
        false ->
            Source
    end.

maybe_write_certs(#{<<"type">> := Type, <<"ssl">> := SSL = #{}} = Source) ->
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(ssl_file_path(Type), SSL) of
        {ok, NSSL} ->
            Source#{<<"ssl">> => NSSL};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end;
maybe_write_certs(#{} = Source) ->
    Source.

ssl_file_path(Type) ->
    filename:join(["authz", Type]).

get_source_by_type(Type, Sources) ->
    {Source, _} = take_by_type(Type, Sources),
    Source.

%% @doc put hook with (maybe) initialized new source and old sources
update_authz_chain(SourceStates) ->
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [SourceStates]}, ?HP_AUTHZ).

merge_sources(OriginConf, NewConf) ->
    {OriginSource, NewSources} =
        lists:foldl(
            fun(OldSource, {OriginAcc, NewAcc}) ->
                Type = type(OldSource),
                case take_by_type(Type, NewAcc) of
                    not_found ->
                        {[OldSource | OriginAcc], NewAcc};
                    {NewSource, NewAcc1} ->
                        MergedSource = emqx_utils_maps:deep_merge(OldSource, NewSource),
                        {[MergedSource | OriginAcc], NewAcc1}
                end
            end,
            {[], get_sources(NewConf)},
            get_sources(OriginConf)
        ),
    lists:reverse(OriginSource) ++ NewSources.

get_sources(Conf) ->
    Default = [emqx_authz_schema:default_authz()],
    maps:get(<<"sources">>, Conf, Default).

%% Works with raw configs
reorder_sources(RawTypes, OldSources) ->
    Types = lists:map(fun to_type/1, RawTypes),
    OldSourcesWithTypes = [{type(Source), Source} || Source <- OldSources],
    reorder_sources(Types, OldSourcesWithTypes, [], []).

reorder_sources([], [] = _RemSourcesWithTypes, ReorderedSources, [] = _NotFoundTypes) ->
    lists:reverse(ReorderedSources);
reorder_sources([], RemSourcesWithTypes, _ReorderedSources, NotFoundTypes) ->
    {error, #{
        not_found => NotFoundTypes, not_reordered => [bin(Type) || {Type, _} <- RemSourcesWithTypes]
    }};
reorder_sources([Type | RemOrder], RemSourcesWithTypes, ReorderedSources, NotFoundTypes) ->
    case lists:keytake(Type, 1, RemSourcesWithTypes) of
        {value, {_Type, Source}, RemSourcesWithTypes1} ->
            reorder_sources(
                RemOrder, RemSourcesWithTypes1, [Source | ReorderedSources], NotFoundTypes
            );
        false ->
            reorder_sources(RemOrder, RemSourcesWithTypes, ReorderedSources, [
                bin(Type) | NotFoundTypes
            ])
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(nowarn_export_all).
-compile(export_all).

merge_sources_test() ->
    ok = emqx_authz_source_registry:create(),
    ok = lists:foreach(
        fun(Type) ->
            ok = emqx_authz_source_registry:register(Type, ?MODULE)
        end,
        [file, http, mysql, mongodb, redis, postgresql]
    ),
    Default = [emqx_authz_schema:default_authz()],
    Http = #{<<"type">> => <<"http">>, <<"enable">> => true},
    Mysql = #{<<"type">> => <<"mysql">>, <<"enable">> => true},
    Mongo = #{<<"type">> => <<"mongodb">>, <<"enable">> => true},
    Redis = #{<<"type">> => <<"redis">>, <<"enable">> => true},
    Postgresql = #{<<"type">> => <<"postgresql">>, <<"enable">> => true},
    HttpDisable = Http#{<<"enable">> => false},
    MysqlDisable = Mysql#{<<"enable">> => false},
    MongoDisable = Mongo#{<<"enable">> => false},

    %% has default source
    ?assertEqual(Default, merge_sources(#{}, #{})),
    ?assertEqual([], merge_sources(#{<<"sources">> => []}, #{<<"sources">> => []})),
    ?assertEqual(Default, merge_sources(#{}, #{<<"sources">> => []})),

    %% add
    ?assertEqual(
        [Http, Mysql, Mongo, Redis, Postgresql],
        merge_sources(
            #{<<"sources">> => [Http, Mysql]},
            #{<<"sources">> => [Mongo, Redis, Postgresql]}
        )
    ),
    %% replace
    ?assertEqual(
        [HttpDisable, MysqlDisable],
        merge_sources(
            #{<<"sources">> => [Http, Mysql]},
            #{<<"sources">> => [HttpDisable, MysqlDisable]}
        )
    ),
    %% add + replace + change position
    ?assertEqual(
        [HttpDisable, Mysql, MongoDisable, Redis],
        merge_sources(
            #{<<"sources">> => [Http, Mysql, Mongo]},
            #{<<"sources">> => [MongoDisable, HttpDisable, Redis]}
        )
    ),
    ok.

-endif.
