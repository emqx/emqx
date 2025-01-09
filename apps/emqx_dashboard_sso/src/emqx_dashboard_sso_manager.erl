%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    handle_continue/2,
    terminate/2
]).

-export([
    running/0,
    lookup_state/1,
    get_backend_status/2,
    make_resource_id/1,
    create_resource/3,
    update_resource/3
]).

-export([
    update/2,
    delete/1,
    pre_config_update/3,
    post_config_update/5,
    propagated_post_config_update/5
]).

-import(emqx_dashboard_sso, [provider/1, format/1]).

-define(MOD_TAB, emqx_dashboard_sso).
-define(MOD_KEY_PATH, [dashboard, sso]).
-define(MOD_KEY_PATH(Sub), [dashboard, sso, Sub]).
-define(RESOURCE_GROUP, <<"dashboard_sso">>).
-define(NO_ERROR, <<>>).
-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-define(DEFAULT_START_OPTS, #{
    start_timeout => timer:seconds(30)
}).

-record(?MOD_TAB, {
    backend :: atom(),
    state :: undefined | map(),
    last_error = ?NO_ERROR :: term()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

running() ->
    SSO = emqx:get_config(?MOD_KEY_PATH, #{}),
    lists:filtermap(
        fun
            (#{backend := Backend, enable := true}) ->
                case lookup(Backend) of
                    undefined ->
                        false;
                    #?MOD_TAB{last_error = ?NO_ERROR} ->
                        {true, Backend};
                    _ ->
                        false
                end;
            (_) ->
                false
        end,
        maps:values(SSO)
    ).

get_backend_status(Backend, false) ->
    #{
        backend => Backend,
        enable => false,
        running => false,
        last_error => ?NO_ERROR
    };
get_backend_status(Backend, _) ->
    case lookup(Backend) of
        undefined ->
            #{
                backend => Backend,
                enable => true,
                running => false,
                last_error => <<"Resource not found">>
            };
        Data ->
            maps:merge(#{backend => Backend, enable => true}, do_get_backend_status(Data))
    end.

update(Backend, Config) ->
    UpdateConf =
        case emqx:get_raw_config(?MOD_KEY_PATH(Backend), #{}) of
            RawConf when is_map(RawConf) ->
                emqx_utils:deobfuscate(Config, RawConf);
            null ->
                Config
        end,
    update_config(Backend, {?FUNCTION_NAME, Backend, UpdateConf}).
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
    {ok, #{}, {continue, start_backend_services}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

handle_continue(start_backend_services, State) ->
    start_backend_services(),
    {noreply, State};
handle_continue(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    remove_handler(),
    ok.

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
                    update_state(Backend, State);
                {error, Reason} ->
                    SafeReason = emqx_utils:redact(Reason),
                    update_last_error(Backend, SafeReason),
                    ?SLOG(error, #{
                        msg => "start_sso_backend_failed",
                        backend => Backend,
                        reason => SafeReason
                    })
            end
        end,
        maps:to_list(Backends)
    ).

update_config(Backend, UpdateReq) ->
    %% we always make sure the valid configuration will update successfully,
    %% ignore the runtime error during its update
    case
        emqx_conf:update(
            ?MOD_KEY_PATH(Backend),
            UpdateReq,
            #{override_to => cluster, lazy_evaluator => fun emqx_schema_secret:source/1}
        )
    of
        {ok, _UpdateResult} ->
            case lookup(Backend) of
                undefined ->
                    ok;
                #?MOD_TAB{state = State, last_error = ?NO_ERROR} ->
                    {ok, State};
                Data ->
                    {error, Data#?MOD_TAB.last_error}
            end;
        {error, Reason} = Error ->
            SafeReason = emqx_utils:redact(Reason),
            ?SLOG(error, #{
                msg => "update_sso_failed",
                backend => Backend,
                reason => SafeReason
            }),
            Error
    end.

pre_config_update(_, {update, _Backend, Config}, _OldConf) ->
    {ok, maybe_write_certs(Config)};
pre_config_update(_, {delete, _Backend}, undefined) ->
    throw(not_exists);
pre_config_update(_, {delete, _Backend}, _OldConf) ->
    {ok, null}.

post_config_update(_, UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    _ = on_config_update(UpdateReq, NewConf),
    ok.

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

on_config_update({update, Backend, _RawConfig}, Config) ->
    Provider = provider(Backend),
    case lookup(Backend) of
        undefined ->
            on_backend_updated(
                Backend,
                emqx_dashboard_sso:create(Provider, Config),
                fun(State) ->
                    update_state(Backend, State)
                end
            );
        Data ->
            update_last_error(Backend, ?NO_ERROR),
            on_backend_updated(
                Backend,
                emqx_dashboard_sso:update(Provider, Config, Data#?MOD_TAB.state),
                fun(State) ->
                    update_state(Backend, State)
                end
            )
    end;
on_config_update({delete, Backend}, _NewConf) ->
    case lookup(Backend) of
        undefined ->
            on_backend_updated(Backend, {error, not_exists}, undefined);
        Data ->
            Provider = provider(Backend),
            on_backend_updated(
                Backend,
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

%% to avoid resource leakage the resource start will never affect the update result,
%% so the resource_id will always be recorded
start_resource_if_enabled(ResourceId, {ok, _} = Result, #{enable := true, backend := Backend}) ->
    case emqx_resource:start(ResourceId, ?DEFAULT_START_OPTS) of
        ok ->
            ok;
        {error, Reason} ->
            SafeReason = emqx_utils:redact(Reason),
            ?SLOG(error, #{
                msg => "start_backend_failed",
                resource_id => ResourceId,
                reason => SafeReason
            }),
            update_last_error(Backend, SafeReason),
            ok
    end,
    Result;
start_resource_if_enabled(_ResourceId, Result, _Config) ->
    Result.

on_backend_updated(_Backend, {ok, State} = Ok, Fun) ->
    Fun(State),
    Ok;
on_backend_updated(_Backend, ok, Fun) ->
    Fun(),
    ok;
on_backend_updated(Backend, {error, Reason} = Error, _) ->
    update_last_error(Backend, Reason),
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
    Dir = certs_path(Backend),
    Provider = provider(Backend),
    emqx_dashboard_sso:convert_certs(Provider, Dir, Conf).

certs_path(Backend) ->
    filename:join(["sso", Backend]).

update_state(Backend, State) ->
    Data = ensure_backend_data(Backend),
    ets:insert(?MOD_TAB, Data#?MOD_TAB{state = State}).

update_last_error(Backend, LastError) ->
    Data = ensure_backend_data(Backend),
    ets:insert(?MOD_TAB, Data#?MOD_TAB{last_error = LastError}).

ensure_backend_data(Backend) ->
    case ets:lookup(?MOD_TAB, Backend) of
        [Data] ->
            Data;
        [] ->
            #?MOD_TAB{backend = Backend}
    end.

do_get_backend_status(#?MOD_TAB{state = #{resource_id := ResourceId}}) ->
    case emqx_resource_manager:lookup(ResourceId) of
        {ok, _Group, #{status := connected}} ->
            #{running => true, last_error => ?NO_ERROR};
        {ok, _Group, #{status := Status}} ->
            #{
                running => false,
                last_error => format([<<"Resource not valid, status: ">>, Status])
            };
        {error, not_found} ->
            #{
                running => false,
                last_error => <<"Resource not found">>
            }
    end;
do_get_backend_status(#?MOD_TAB{last_error = ?NO_ERROR}) ->
    #{running => true, last_error => ?NO_ERROR};
do_get_backend_status(#?MOD_TAB{last_error = LastError}) ->
    #{
        running => false,
        last_error => format([LastError])
    }.
