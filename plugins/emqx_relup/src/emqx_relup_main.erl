-module(emqx_relup_main).

-behaviour(gen_server).

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    load/1,
    unload/0,
    upgrade/1
]).

-export([
    get_all_upgrade_logs/0,
    get_latest_upgrade_status/0,
    delete_all_upgrade_logs/0
]).

-import(emqx_relup_utils, [bin/1]).

-type state() :: #{}.

-type upgrade_status() :: 'in-progress' | finished.

-type upgrade_error() :: #{err_type => atom(), details => binary()}.

-record(emqx_relup_log, {
    started_at :: integer() | undefined,
    finished_at :: integer() | undefined,
    from_vsn :: binary() | undefined,
    target_vsn :: binary() | undefined,
    upgrade_opts = #{} :: map(),
    status :: upgrade_status() | undefined,
    result :: success | upgrade_error() | undefined,
    extra = #{} :: map()
}).

-define(LOG(LEVEL, MSG),
    logger:log(LEVEL, (begin
        MSG
    end)#{
        tag => "RELUP"
    })
).

%%==============================================================================
%% API
%%==============================================================================
start_link() ->
    ok = mria:create_table(
        emqx_relup_log,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, emqx_relup_log},
            {attributes, record_info(fields, emqx_relup_log)}
        ]
    ),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

upgrade(TarballPath) ->
    gen_server:call(?MODULE, {upgrade, #{tarball => TarballPath}}, infinity).

%% Called when the plugin application start
load(_Env) ->
    ok.
%% Called when the plugin application stop
unload() ->
    ok.

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec init(list()) -> {ok, state()}.
init([]) ->
    {ok, #{}}.

handle_call({upgrade, Opts}, _From, State) ->
    CurrVsn = emqx_release:version(),
    RootDir = code:root_dir(),
    %% target_vsn isn't known until check_and_unpack parses
    %% releases/emqx_vars from the extracted tarball.
    Key = log_upgrade_started(CurrVsn, undefined, Opts),
    Result = do_upgrade(CurrVsn, RootDir, Opts),
    ok = log_upgrade_result(Key, Result),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

do_upgrade(CurrVsn, RootDir, Opts) ->
    case emqx_relup_handler:check_and_unpack(CurrVsn, RootDir, Opts) of
        {error, Reason} ->
            ?LOG(error, #{msg => check_upgrade_failed, reason => Reason}),
            {error, Reason#{stage => check_and_unpack}};
        {ok, #{target_vsn := TargetVsn} = Opts1} ->
            ?LOG(notice, #{msg => perform_upgrade, from_vsn => CurrVsn, target_vsn => TargetVsn}),
            try emqx_relup_handler:perform_upgrade(CurrVsn, TargetVsn, RootDir, Opts1) of
                ok ->
                    case emqx_relup_handler:permanent_upgrade(CurrVsn, TargetVsn, RootDir, Opts1) of
                        ok ->
                            ?LOG(notice, #{
                                msg => upgrade_complete,
                                from_vsn => CurrVsn,
                                target_vsn => TargetVsn
                            }),
                            ok;
                        {error, Reason} ->
                            ?LOG(error, #{
                                msg => permanent_upgrade_failed,
                                reason => Reason,
                                from_vsn => CurrVsn,
                                target_vsn => TargetVsn
                            }),
                            {error, Reason#{stage => permanent_upgrade}}
                    end;
                {error, Reason} ->
                    ?LOG(error, #{
                        msg => perform_upgrade_failed,
                        reason => Reason,
                        from_vsn => CurrVsn,
                        target_vsn => TargetVsn
                    }),
                    {error, Reason#{stage => perform_upgrade}}
            catch
                throw:Reason ->
                    restart_vm(Reason);
                Err:Reason:ST ->
                    restart_vm({Err, Reason, ST})
            end
    end.

restart_vm(Reason) ->
    ?LOG(error, #{msg => restart_vm, reason => Reason}),
    %% Maybe we can rollback the system rather than restart the VM. Here we simply
    %% restart the VM because if we reload the modules we just upgraded,
    %% some processes will probably be killed as they are still runing old code.
    init:restart(),
    {error_vm_restarted, Reason}.

%%==============================================================================
%% upgrade logs
%%==============================================================================

delete_all_upgrade_logs() ->
    {atomic, ok} = mnesia:clear_table(emqx_relup_log),
    ok.

get_all_upgrade_logs() ->
    lists:map(fun format_upgrade_log/1, ets:tab2list(emqx_relup_log)).

get_latest_upgrade_status() ->
    case ets:last(emqx_relup_log) of
        '$end_of_table' ->
            idle;
        Key ->
            case ets:lookup(emqx_relup_log, Key) of
                [#emqx_relup_log{status = finished}] -> idle;
                [#emqx_relup_log{status = 'in-progress'}] -> 'in-progress'
            end
    end.

format_upgrade_log(#emqx_relup_log{
    started_at = StartedAt,
    finished_at = FinishedAt,
    from_vsn = FromVsn,
    target_vsn = TargetVsn,
    upgrade_opts = Opts,
    status = Status,
    result = Result
}) ->
    #{
        started_at => maybe_to_rfc3339(StartedAt),
        finished_at => maybe_to_rfc3339(FinishedAt),
        from_vsn => FromVsn,
        target_vsn => TargetVsn,
        upgrade_opts => Opts,
        status => Status,
        result => maybe_result(Result)
    }.

maybe_to_rfc3339(undefined) -> <<>>;
maybe_to_rfc3339(Int) -> bin(calendar:system_time_to_rfc3339(Int, [{unit, millisecond}])).

maybe_result(undefined) -> <<>>;
maybe_result(Result) -> Result.

log_upgrade_started(CurrVsn, TargetVsn, Opts) ->
    Now = erlang:system_time(millisecond),
    ?LOG(notice, #{
        msg => got_upgrade_request, from_vsn => CurrVsn, target_vsn => TargetVsn, opts => Opts
    }),
    ok = mnesia:dirty_write(#emqx_relup_log{
        started_at = Now,
        from_vsn = bin(CurrVsn),
        target_vsn = bin(TargetVsn),
        upgrade_opts = Opts,
        status = 'in-progress'
    }),
    Now.

log_upgrade_result(Key, Result) ->
    case mnesia:dirty_read(emqx_relup_log, Key) of
        [#emqx_relup_log{from_vsn = FromVsn, target_vsn = TargetVsn} = Log] ->
            ?LOG(notice, #{
                msg => upgrade_finished,
                from_vsn => FromVsn,
                target_vsn => TargetVsn,
                result => Result
            }),
            mnesia:dirty_write(Log#emqx_relup_log{
                finished_at = erlang:system_time(millisecond),
                status = finished,
                result = format_result(Result)
            });
        [] ->
            ?LOG(notice, #{msg => upgrade_finished, result => Result}),
            ?LOG(error, #{msg => upgrade_log_not_found, key => Key}),
            ok
    end.

format_result(ok) ->
    success;
format_result({error, Details}) ->
    #{
        err_type => maps:get(err_type, Details, unknown),
        details => bin(io_lib:format("~0p", [maps:remove(err_type, Details)]))
    }.
