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

-define(MOD_KEY_PATH, [dashboard, sso]).
-define(MOD_KEY_PATH(Sub), [dashboard, sso, Sub]).
-define(RESOURCE_GROUP, <<"emqx_dashboard_sso">>).
-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-record(dashboard_sso, {
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
    case ets:lookup(dashboard_sso, Backend) of
        [Data] ->
            Data#dashboard_sso.state;
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
    gen_server:call(?MODULE, Req).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    add_handler(),
    emqx_utils_ets:new(
        dashboard_sso,
        [
            set,
            public,
            named_table,
            {keypos, #dashboard_sso.backend},
            {read_concurrency, true}
        ]
    ),
    start_backend_services(),
    {ok, #{}}.

handle_call({update_config, Req, NewConf}, _From, State) ->
    Result = on_config_update(Req, NewConf),
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
                        msg => "Start SSO backend successfully",
                        backend => Backend
                    }),
                    ets:insert(dashboard_sso, #dashboard_sso{backend = Backend, state = State});
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "Start SSO backend failed",
                        backend => Backend,
                        reason => Reason
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
                msg => "Update SSO configuration successfully",
                backend => Backend,
                result => Result
            }),
            Result;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "Update SSO configuration failed",
                backend => Backend,
                reason => Reason
            }),
            Error
    end.

pre_config_update(_, {update, _Backend, Config}, _OldConf) ->
    {ok, Config};
pre_config_update(_, {delete, _Backend}, undefined) ->
    throw(not_exists);
pre_config_update(_, {delete, _Backend}, _OldConf) ->
    {ok, null}.

post_config_update(_, UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    Result = call({update_config, UpdateReq, NewConf}),
    {ok, Result}.

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
                emqx_dashboard_sso:create(Provider, Config),
                fun(State) ->
                    ets:insert(dashboard_sso, #dashboard_sso{backend = Backend, state = State})
                end
            );
        Data ->
            on_backend_updated(
                emqx_dashboard_sso:update(Provider, Config, Data#dashboard_sso.state),
                fun(State) ->
                    ets:insert(dashboard_sso, Data#dashboard_sso{state = State})
                end
            )
    end;
on_config_update({delete, Backend}, _NewConf) ->
    case lookup(Backend) of
        undefined ->
            {error, not_exists};
        Data ->
            Provider = provider(Backend),
            on_backend_updated(
                emqx_dashboard_sso:destroy(Provider, Data#dashboard_sso.state),
                fun() ->
                    ets:delete(dashboard_sso, Backend)
                end
            )
    end.

lookup(Backend) ->
    case ets:lookup(dashboard_sso, Backend) of
        [Data] ->
            Data;
        [] ->
            undefined
    end.

start_resource_if_enabled(ResourceId, {ok, _} = Result, #{enable := true}) ->
    _ = emqx_resource:start(ResourceId),
    Result;
start_resource_if_enabled(_ResourceId, Result, _Config) ->
    Result.

on_backend_updated({ok, State} = Ok, Fun) ->
    Fun(State),
    Ok;
on_backend_updated(ok, Fun) ->
    Fun(),
    ok;
on_backend_updated(Error, _) ->
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
