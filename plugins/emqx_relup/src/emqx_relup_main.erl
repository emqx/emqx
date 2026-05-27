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
    upgrade/1,
    upgrade/2
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
    %% `emqx_relup_log` is a local-content disc table — each node owns
    %% its own audit trail of upgrade attempts. The table is created
    %% here (idempotent: if the table already exists on disk from a
    %% previous install, mria attaches to it). Nothing in this plugin
    %% deletes the table on stop/unload, so plugin uninstall keeps the
    %% rows on disk; reinstalling re-attaches and the history is
    %% preserved. Operators clear history explicitly via
    %% `emqx ctl relup logs-clear`.
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
    upgrade(TarballPath, #{}).

upgrade(TarballPath, ExtraOpts) when is_map(ExtraOpts) ->
    Opts = ExtraOpts#{tarball => TarballPath},
    gen_server:call(?MODULE, {upgrade, Opts}, infinity).

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
    %% releases/emqx_vars from the extracted tarball; do_upgrade/3
    %% surfaces it so the log row can be stamped.
    Key = log_upgrade_started(CurrVsn, Opts),
    {Result, TargetVsn} = do_upgrade(CurrVsn, RootDir, Opts),
    ok = log_upgrade_result(Key, Result, TargetVsn),
    ok = maybe_restart_vm(Result),
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
            {{error, Reason#{stage => check_and_unpack}}, undefined};
        {ok, #{target_vsn := TargetVsn} = Opts1} ->
            ?LOG(notice, #{msg => perform_upgrade, from_vsn => CurrVsn, target_vsn => TargetVsn}),
            Result = do_perform_and_permanent(CurrVsn, TargetVsn, RootDir, Opts1),
            {Result, TargetVsn}
    end.

do_perform_and_permanent(CurrVsn, TargetVsn, RootDir, Opts) ->
    try emqx_relup_handler:perform_upgrade(CurrVsn, TargetVsn, RootDir, Opts) of
        ok ->
            case emqx_relup_handler:permanent_upgrade(CurrVsn, TargetVsn, RootDir, Opts) of
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
            pending_vm_restart(Reason);
        Err:Reason:ST ->
            pending_vm_restart({Err, Reason, ST})
    end.

-doc """
The catch clauses above can't call `init:restart/0` directly: we
still need to (a) write the audit log row and (b) `mnesia:sync_log/0`
to flush it to disk. Return a sentinel that `handle_call` recognises;
it triggers the actual restart after the log is persisted.

Rolling back the system rather than restarting the VM might be
preferable, but is risky — reloading the modules we just upgraded
can kill processes that are still running old code.
""".
pending_vm_restart(Reason) ->
    ?LOG(error, #{msg => restart_vm, reason => Reason}),
    {error_vm_restarted, Reason}.

maybe_restart_vm({error_vm_restarted, _Reason}) ->
    _ = mnesia:sync_log(),
    _ = init:restart(),
    ok;
maybe_restart_vm(_) ->
    ok.

%%==============================================================================
%% upgrade logs
%%==============================================================================

delete_all_upgrade_logs() ->
    {atomic, ok} = mnesia:clear_table(emqx_relup_log),
    ok.

get_all_upgrade_logs() ->
    lists:map(fun format_upgrade_log/1, ets:tab2list(emqx_relup_log)).

get_latest_upgrade_status() ->
    %% If `<code:root_dir()>/relup/current` exists, an upgrade has been
    %% committed against this install and the running BEAM has hot-loaded
    %% the target's modules; a node restart is required to boot the
    %% deployed tree. Surface that state explicitly so operators don't
    %% confuse it with `idle`.
    case read_current_marker() of
        {ok, TargetVsn} ->
            {hot_upgraded, TargetVsn};
        none ->
            case ets:last_lookup(emqx_relup_log) of
                '$end_of_table' -> idle;
                {_, [#emqx_relup_log{status = finished}]} -> idle;
                {_, [#emqx_relup_log{status = 'in-progress'}]} -> 'in-progress'
            end
    end.

read_current_marker() ->
    Path = filename:join([code:root_dir(), "relup", "current"]),
    case file:read_file(Path) of
        {ok, Bin} -> {ok, unicode:characters_to_list(string:trim(Bin))};
        {error, _} -> none
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
        from_vsn => maybe_undefined(FromVsn),
        target_vsn => maybe_undefined(TargetVsn),
        upgrade_opts => Opts,
        status => Status,
        result => maybe_undefined(Result)
    }.

maybe_to_rfc3339(undefined) -> <<>>;
maybe_to_rfc3339(Int) -> bin(calendar:system_time_to_rfc3339(Int, [{unit, millisecond}])).

maybe_undefined(undefined) -> <<>>;
maybe_undefined(V) -> V.

log_upgrade_started(CurrVsn, Opts) ->
    Now = erlang:system_time(millisecond),
    ?LOG(notice, #{
        msg => got_upgrade_request, from_vsn => CurrVsn, opts => Opts
    }),
    ok = mnesia:dirty_write(#emqx_relup_log{
        started_at = Now,
        from_vsn = bin(CurrVsn),
        target_vsn = undefined,
        upgrade_opts = Opts,
        status = 'in-progress'
    }),
    Now.

log_upgrade_result(Key, Result, TargetVsn) ->
    TargetBin =
        case TargetVsn of
            undefined -> undefined;
            _ -> bin(TargetVsn)
        end,
    case mnesia:dirty_read(emqx_relup_log, Key) of
        [#emqx_relup_log{from_vsn = FromVsn} = Log] ->
            ?LOG(notice, #{
                msg => upgrade_finished,
                from_vsn => FromVsn,
                target_vsn => TargetBin,
                result => Result
            }),
            mnesia:dirty_write(Log#emqx_relup_log{
                target_vsn = TargetBin,
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
