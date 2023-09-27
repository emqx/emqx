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
    get_backend_status/2,
    lookup_state/1,
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
-define(RESOURCE_GROUP, <<"emqx_dashboard_sso">>).
-define(START_ERROR_KEY, start_error).
-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-record(?MOD_TAB, {
    backend :: atom(),
    state :: map(),
    last_error = <<>> :: term()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

running() ->
    lists:filtermap(
        fun
            (#?MOD_TAB{backend = Backend, last_error = <<>>}) ->
                {true, Backend};
            (_) ->
                false
        end,
        ets:tab2list(?MOD_TAB)
    ).

get_backend_status(Backend, false) ->
    #{
        backend => Backend,
        enable => false,
        running => false,
        last_error => <<>>
    };
get_backend_status(Backend, _) ->
    case lookup(Backend) of
        undefined ->
            #{
                backend => Backend,
                enable => true,
                running => false,
                last_error => <<"resource not found">>
            };
        Data ->
            maps:merge(#{backend => Backend, enable => true}, do_get_backend_status(Data))
    end.

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
            end,
            record_start_error(Backend, false)
        end,
        maps:to_list(Backends)
    ).

update_config(Backend, UpdateReq) ->
    OkFun = fun(Result) ->
        ?SLOG(info, #{
            msg => "update_sso_successfully",
            backend => Backend,
            result => emqx_utils:redact(Result)
        }),
        Result
    end,

    ErrFun = fun({error, Reason} = Error) ->
        SafeReason = emqx_utils:redact(Reason),
        ?SLOG(error, #{
            msg => "update_sso_failed",
            backend => Backend,
            reason => SafeReason
        }),
        Error
    end,

    %% we always make sure the valid configuration will update successfully,
    %% ignore the runtime error during its update
    case emqx_conf:update(?MOD_KEY_PATH(Backend), UpdateReq, #{override_to => cluster}) of
        {ok, UpdateResult} ->
            #{post_config_update := #{?MODULE := Result}} = UpdateResult,
            case Result of
                ok ->
                    OkFun(Result);
                {ok, _} ->
                    OkFun(Result);
                {error, _} = Error ->
                    ErrFun(Error)
            end;
        {error, _} = Error ->
            ErrFun(Error)
    end.

pre_config_update(_, {update, _Backend, Config}, _OldConf) ->
    maybe_write_certs(Config);
pre_config_update(_, {delete, _Backend}, undefined) ->
    throw(not_exists);
pre_config_update(_, {delete, _Backend}, _OldConf) ->
    {ok, null}.

post_config_update(_, UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    {ok, on_config_update(UpdateReq, NewConf)}.

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
                fun(State, LastError) ->
                    ets:insert(
                        ?MOD_TAB,
                        #?MOD_TAB{backend = Backend, state = State, last_error = LastError}
                    )
                end
            );
        Data ->
            on_backend_updated(
                Backend,
                emqx_dashboard_sso:update(Provider, Config, Data#?MOD_TAB.state),
                fun(State, LastError) ->
                    ets:insert(?MOD_TAB, Data#?MOD_TAB{state = State, last_error = LastError})
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
start_resource_if_enabled(ResourceId, {ok, _} = Result, #{enable := true}) ->
    clear_start_error(),
    case emqx_resource:start(ResourceId) of
        ok ->
            ok;
        {error, Reason} ->
            SafeReason = emqx_utils:redact(Reason),
            mark_start_error(SafeReason),
            ?SLOG(error, #{
                msg => "start_backend_failed",
                resource_id => ResourceId,
                reason => SafeReason
            }),
            ok
    end,
    Result;
start_resource_if_enabled(_ResourceId, Result, _Config) ->
    Result.

on_backend_updated(Backend, {ok, State} = Ok, Fun) ->
    Fun(State, <<>>),
    record_start_error(Backend, true),
    Ok;
on_backend_updated(_Backend, {ok, State, LastError} = Ok, Fun) ->
    Fun(State, LastError),
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

clear_start_error() ->
    mark_start_error(<<>>).

mark_start_error(Reason) ->
    erlang:put(?START_ERROR_KEY, Reason).

record_start_error(Backend, Force) ->
    case erlang:get(?START_ERROR_KEY) of
        <<>> when Force ->
            update_last_error(Backend, <<>>);
        <<>> ->
            ok;
        Reason ->
            update_last_error(Backend, Reason)
    end.

update_last_error(Backend, Reason) ->
    ets:update_element(?MOD_TAB, Backend, {#?MOD_TAB.last_error, Reason}).

do_get_backend_status(#?MOD_TAB{state = #{resource_id := ResourceId}}) ->
    case emqx_resource_manager:lookup(ResourceId) of
        {ok, _Group, #{status := connected}} ->
            #{running => true, last_error => <<>>};
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
do_get_backend_status(#?MOD_TAB{last_error = <<>>}) ->
    #{running => true, last_error => <<>>};
do_get_backend_status(#?MOD_TAB{last_error = LastError}) ->
    #{
        running => false,
        last_error => format([LastError])
    }.
