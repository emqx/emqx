%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_manager).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/2
]).

-export([
    running/0,
    lookup_state/1,
    make_resource_id/1,
    create_resource/3,
    update_resource/3,
    call/1
]).

-export([
    update/2,
    delete/1,
    pre_config_update/3,
    post_config_update/5,
    propagated_post_config_update/5
]).

-import(emqx_dashboard_sso, [provider/1]).

-define(MOD_TAB, emqx_dashboard_sso).
-define(MOD_KEY_PATH, [dashboard, sso]).
-define(CALL_TIMEOUT, timer:seconds(15)).
-define(MOD_KEY_PATH(Sub), [dashboard, sso, Sub]).
-define(RESOURCE_GROUP, <<"emqx_dashboard_sso">>).
-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-record(?MOD_TAB, {
    backend :: atom(),
    state :: map()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

running() ->
    maps:fold(
        fun
            (Type, #{enable := true}, Acc) ->
                [Type | Acc];
            (_Type, _Cfg, Acc) ->
                Acc
        end,
        [],
        emqx:get_config(?MOD_KEY_PATH)
    ).

update(Backend, Config) ->
    update_config(Backend, {?FUNCTION_NAME, Backend, Config}).
delete(Backend) ->
    update_config(Backend, {?FUNCTION_NAME, Backend}).

lookup_state(Backend) ->
    case ets:lookup(?MOD_TAB, Backend) of
        [Data] ->
            Data#?MOD_TAB.state;
        [] ->
            undefined
    end.

make_resource_id(Backend) ->
    BackendBin = bin(Backend),
    emqx_resource:generate_id(<<"sso:", BackendBin/binary>>).

create_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:create_local(
        ResourceId,
        ?RESOURCE_GROUP,
        Module,
        Config,
        ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(ResourceId, Result, Config).

update_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:recreate_local(
        ResourceId, Module, Config, ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(ResourceId, Result, Config).

call(Req) ->
    try
        gen_server:call(?MODULE, Req, ?CALL_TIMEOUT)
    catch
        exit:{timeout, _} ->
            {error, <<"Update backend failed: timeout">>}
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    add_handler(),
    emqx_utils_ets:new(
        ?MOD_TAB,
        [
            ordered_set,
            public,
            named_table,
            {keypos, #?MOD_TAB.backend},
            {read_concurrency, true}
        ]
    ),
    start_backend_services(),
    {ok, #{}}.

handle_call({update_config, Req, NewConf, OldConf}, _From, State) ->
    Result = on_config_update(Req, NewConf, OldConf),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    remove_handler(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
start_backend_services() ->
    Backends = emqx_conf:get(?MOD_KEY_PATH, #{}),
    lists:foreach(
        fun({Backend, Config}) ->
            Provider = provider(Backend),
            case emqx_dashboard_sso:create(Provider, Config) of
                {ok, State} ->
                    ?SLOG(info, #{
                        msg => "start_sso_backend_successfully",
                        backend => Backend
                    }),
                    ets:insert(?MOD_TAB, #?MOD_TAB{backend = Backend, state = State});
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "start_sso_backend_failed",
                        backend => Backend,
                        reason => emqx_utils:redact(Reason)
                    })
            end
        end,
        maps:to_list(Backends)
    ).

update_config(Backend, UpdateReq) ->
    case emqx_conf:update(?MOD_KEY_PATH(Backend), UpdateReq, #{override_to => cluster}) of
        {ok, UpdateResult} ->
            #{post_config_update := #{?MODULE := Result}} = UpdateResult,
            ?SLOG(info, #{
                msg => "update_sso_successfully",
                backend => Backend,
                result => emqx_utils:redact(Result)
            }),
            Result;
        {error, Reason} ->
            SafeReason = emqx_utils:redact(Reason),
            ?SLOG(error, #{
                msg => "update_sso_failed",
                backend => Backend,
                reason => SafeReason
            }),
            {error,
                case SafeReason of
                    {_Stage, _Mod, Reason2} ->
                        Reason2;
                    _ ->
                        Reason
                end}
    end.

pre_config_update(_, {update, _Backend, Config}, _OldConf) ->
    maybe_write_certs(Config);
pre_config_update(_, {delete, _Backend}, undefined) ->
    throw(not_exists);
pre_config_update(_, {delete, _Backend}, _OldConf) ->
    {ok, null}.

post_config_update(_, UpdateReq, NewConf, OldConf, _AppEnvs) ->
    call({update_config, UpdateReq, NewConf, OldConf}).

propagated_post_config_update(
    ?MOD_KEY_PATH(BackendBin) = Path, _UpdateReq, undefined, OldConf, AppEnvs
) ->
    case atom(BackendBin) of
        {ok, Backend} ->
            post_config_update(Path, {delete, Backend}, undefined, OldConf, AppEnvs);
        Error ->
            Error
    end;
propagated_post_config_update(
    ?MOD_KEY_PATH(BackendBin) = Path, _UpdateReq, NewConf, OldConf, AppEnvs
) ->
    case atom(BackendBin) of
        {ok, Backend} ->
            post_config_update(Path, {update, Backend, undefined}, NewConf, OldConf, AppEnvs);
        Error ->
            Error
    end.

on_config_update({update, Backend, _RawConfig}, Config, OldConfig) ->
    Provider = provider(Backend),
    case lookup(Backend) of
        undefined ->
            on_backend_updated(
                emqx_dashboard_sso:create(Provider, Config),
                fun(State) ->
                    ets:insert(?MOD_TAB, #?MOD_TAB{backend = Backend, state = State})
                end
            );
        Data ->
            %% the steps for updating/recreating a resource are:
            %% 1. destroy the old resource
            %% 2. create a new resource
            %% to keep consistency we need to follow those steps too,
            %% however a failed update will not change the config, but will lose the resource
            %% hence for consistency and atomicity, we should rollback when the update fails
            ets:delete(?MOD_TAB, Backend),
            UpdateState = fun(State) ->
                ets:insert(?MOD_TAB, Data#?MOD_TAB{state = State})
            end,
            on_backend_updated(
                emqx_dashboard_sso:update(Provider, Config, Data#?MOD_TAB.state),
                UpdateState,
                rollback(
                    Backend,
                    OldConfig,
                    UpdateState
                )
            )
    end;
on_config_update({delete, Backend}, _NewConf, _OldConf) ->
    case lookup(Backend) of
        undefined ->
            {error, not_exists};
        Data ->
            Provider = provider(Backend),
            on_backend_updated(
                emqx_dashboard_sso:destroy(Provider, Data#?MOD_TAB.state),
                fun() ->
                    ets:delete(?MOD_TAB, Backend)
                end
            )
    end.

lookup(Backend) ->
    case ets:lookup(?MOD_TAB, Backend) of
        [Data] ->
            Data;
        [] ->
            undefined
    end.

start_resource_if_enabled(ResourceId, {ok, _} = Result, #{enable := true}) ->
    case emqx_resource:start(ResourceId) of
        ok ->
            Result;
        {error, Reason} ->
            SafeReason = emqx_utils:redact(Reason),
            ?SLOG(error, #{
                msg => "start_backend_failed",
                resource_id => ResourceId,
                reason => SafeReason
            }),
            clean_when_start_failed(ResourceId),
            {error, emqx_dashboard_sso:format(["Start backend failed, Reason: ", SafeReason])}
    end;
start_resource_if_enabled(_ResourceId, Result, _Config) ->
    Result.

%% ensure the backend creation is atomic, clean the corresponding resource when necessary,
%% when creating a backend fails, nothing will be inserted into the SSO table,
%% thus the resources created by backend will leakage.
%% Although we can treat start failure as successful,
%% and insert the resource data into the SSO table,
%% it may be strange for users: it succeeds, but can't be used.
clean_when_start_failed(ResourceId) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

on_backend_updated(Result, OkFun) ->
    on_backend_updated(Result, OkFun, undefined).

%% this first level `ok` is for emqx_config_handler, and the second level is for the caller
on_backend_updated({ok, State} = Ok, Fun, _ErrFun) ->
    Fun(State),
    {ok, Ok};
on_backend_updated(ok, Fun, _ErrFun) ->
    Fun(),
    {ok, ok};
on_backend_updated(Error, _, ErrFun) ->
    erlang:is_function(ErrFun) andalso ErrFun(Error),
    Error.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

atom(B) ->
    emqx_utils:safe_to_existing_atom(B).

add_handler() ->
    ok = emqx_conf:add_handler(?MOD_KEY_PATH('?'), ?MODULE).

remove_handler() ->
    ok = emqx_conf:remove_handler(?MOD_KEY_PATH('?')).

maybe_write_certs(#{<<"backend">> := Backend} = Conf) ->
    case
        emqx_tls_lib:ensure_ssl_files(
            ssl_file_path(Backend), maps:get(<<"ssl">>, Conf, undefined)
        )
    of
        {ok, SSL} ->
            {ok, new_ssl_source(Conf, SSL)};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

ssl_file_path(Backend) ->
    filename:join(["sso", Backend]).

new_ssl_source(Source, undefined) ->
    Source;
new_ssl_source(Source, SSL) ->
    Source#{<<"ssl">> => SSL}.

rollback(Backend, OldConf, OnSucc) ->
    fun(_) ->
        try_recreate(Backend, OldConf, OnSucc)
    end.

try_recreate(_Backend, undefined, _OnSucc) ->
    ok;
try_recreate(_Backend, #{enable := false}, _OnSucc) ->
    ok;
try_recreate(Backend, Config, OnSucc) ->
    Provider = provider(Backend),
    ?SLOG(info, #{
        msg => "backend_rollback",
        backend => Backend,
        reason => "update_sso_failed",
        config => emqx_utils:redact(Config)
    }),
    on_backend_updated(
        emqx_dashboard_sso:create(Provider, Config),
        OnSucc,
        fun(Error) ->
            ?SLOG(error, #{
                msg => "backend_rollback_failed",
                backend => Backend,
                reason => emqx_utils:redact(Error)
            })
        end
    ).
