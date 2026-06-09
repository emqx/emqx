-module(emqx_acme_issuer).

-behaviour(gen_server).

-include("emqx_acme.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("public_key/include/public_key.hrl").

%% API
-export([
    start_link/0,
    issue/0,
    renew/0,
    status/0,
    reconfigure/0,
    leader_node/0,
    %% Imperative listener wiring — used by the API's /apply_listener
    %% endpoint to bind a single listener (or the dashboard https
    %% listener) to the bundle without going through the auto-attach
    %% path in store_result/2 (which only fires on first issuance).
    migrate_one_listener/3,
    enable_dashboard_https/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
-export([
    pre_state/2,
    maybe_migrate_listeners/3,
    build_acc_key_opt/2,
    ensure_acc_key_file/2,
    ensure_bundle_acc_key/2,
    store_result/2
]).
-endif.

-define(SERVER, ?MODULE).
-define(CHECK_MSG, check_renewal).
-define(WORKER_RESULT, emqx_acme_worker_result).
-define(ISSUANCE_TIMEOUT, 120_000).

-type action() :: issue | renew | check.
-type in_progress() ::
    false
    | #{
        action := action(),
        started_at := calendar:datetime(),
        worker := {pid(), reference()}
    }.

-record(state, {
    timer_ref :: reference() | undefined,
    last_result :: ok | {error, term()} | undefined,
    last_check :: calendar:datetime() | undefined,
    %% Async issuance lifecycle: a single worker process runs `do_issue/0`
    %% (for issue/renew) or `check_and_renew/1` (for the periodic timer)
    %% out-of-band. The API call returns immediately with `{ok, started}`
    %% so the dashboard's plugin-API gateway never trips its 5-second
    %% budget. `last_result` is updated when the worker finishes; the UI
    %% polls `/status` to follow progress.
    in_progress = false :: in_progress()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% issue/renew/status delegate to the cluster's ACME leader (the
%% lowest-sorted running core node). This keeps periodic renewal and
%% on-demand issuance to a single node: replicants and non-leader cores
%% just forward the dashboard's request via gen_server:call so the
%% issuance state (in_progress, last_result) stays consistent.

%% Kicks off an issuance asynchronously. Returns immediately:
%% - `{ok, started}` if a worker was spawned;
%% - `{error, {already_running, Action}}` if a worker is already running;
%% - `{error, leader_unavailable}` if no core node is reachable.
%% The actual outcome lands in `status().last_result` when the worker
%% finishes. UIs/CLIs poll `status/0` to follow progress.
-spec issue() ->
    {ok, started}
    | {error, {already_running, action()} | leader_unavailable | no_core_nodes}.
issue() ->
    call_leader({start, issue}).

-spec renew() ->
    {ok, started}
    | {error, {already_running, action()} | leader_unavailable | no_core_nodes}.
renew() ->
    call_leader({start, renew}).

-spec status() -> map() | {error, leader_unavailable | no_core_nodes}.
status() ->
    call_leader(status).

-spec reconfigure() -> ok.
reconfigure() ->
    %% Cluster-local: each node's gen_server picks up its own config
    %% (replicated by emqx_conf) and reschedules its own timer. On non-
    %% leader nodes that timer fires but skips the issuance worker.
    gen_server:cast(?SERVER, reconfigure).

%% Lowest-sorted running core node owns issuance. Returns the node
%% atom or {error, no_core_nodes} if mria reports an empty core list —
%% which also covers the test-VM / very-early-boot case where mria
%% isn't running yet (the membership ETS doesn't exist and a raw call
%% would crash, so we catch the badarg here).
-spec leader_node() -> {ok, node()} | {error, no_core_nodes}.
leader_node() ->
    Cores =
        try
            mria_membership:running_core_nodelist()
        catch
            _:_ -> []
        end,
    case lists:sort(Cores) of
        [] -> {error, no_core_nodes};
        [Leader | _] -> {ok, Leader}
    end.

-spec is_leader() -> boolean().
is_leader() ->
    case leader_node() of
        {ok, Leader} -> Leader =:= node();
        _ -> false
    end.

%% Route a gen_server:call to whichever node is currently the leader.
%% Local calls take the short path; remote calls use `infinity` because
%% the handler always replies immediately (it spawns the worker and
%% returns; or reads status straight from state). Network-level failures
%% surface as `leader_unavailable` so the caller does not have to know
%% how Erlang distribution failure modes are spelled.
call_leader(Msg) ->
    case leader_node() of
        {ok, Leader} when Leader =:= node() ->
            gen_server:call(?SERVER, Msg, infinity);
        {ok, Leader} ->
            try
                gen_server:call({?SERVER, Leader}, Msg, infinity)
            catch
                exit:{noproc, _} -> {error, leader_unavailable};
                exit:{nodedown, _} -> {error, leader_unavailable};
                exit:{{nodedown, _}, _} -> {error, leader_unavailable}
            end;
        {error, _} = E ->
            E
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    State = #state{},
    State1 = schedule_check(State, 1_000),
    {ok, State1}.

handle_call({start, _}, _From, #state{in_progress = #{action := A}} = State) ->
    {reply, {error, {already_running, A}}, State};
handle_call({start, Action}, _From, #state{in_progress = false} = State) ->
    InProgress = spawn_worker(Action),
    {reply, {ok, started}, State#state{in_progress = InProgress}};
handle_call(status, _From, State) ->
    Settings = emqx_acme_config:settings(),
    CertInfo = get_cert_info(Settings),
    Status = #{
        domains => maps:get(domains, Settings),
        cert_bundle_name => maps:get(cert_bundle_name, Settings),
        in_progress => format_in_progress(State#state.in_progress),
        last_result => format_last_result(State#state.last_result),
        last_check => format_datetime(State#state.last_check),
        certificate => CertInfo
    },
    {reply, Status, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(reconfigure, State) ->
    State1 = cancel_timer(State),
    State2 = schedule_check(State1, 1_000),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?CHECK_MSG, State) ->
    State1 = State#state{timer_ref = undefined},
    State2 = maybe_start_check(State1),
    {noreply, schedule_check(State2)};
handle_info(
    {'DOWN', Ref, process, Pid, {?WORKER_RESULT, Result}},
    #state{in_progress = #{worker := {Pid, Ref}}} = State
) ->
    %% Normal completion: the worker exits with its result encoded in
    %% the exit reason so we only have to handle a single message.
    {noreply, finish(Result, State)};
handle_info(
    {'DOWN', Ref, process, Pid, Reason},
    #state{in_progress = #{worker := {Pid, Ref}}} = State
) ->
    %% Worker crashed before producing a result. Surface that as the
    %% last_result so the operator sees the cause in /status.
    ?LOG(error, #{msg => "issuance_worker_crashed", reason => Reason}),
    {noreply, finish({error, {worker_crashed, Reason}}, State)};
handle_info({'DOWN', _, process, _, _}, State) ->
    %% DOWN from a previous worker we already cleared. Ignore.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State),
    ok.

%%--------------------------------------------------------------------
%% Internal: scheduling
%%--------------------------------------------------------------------

schedule_check(State) ->
    Settings = emqx_acme_config:settings(),
    Hours = maps:get(check_interval_hours, Settings, ?DEFAULT_CHECK_INTERVAL_HOURS),
    DelayMs = Hours * 3_600_000,
    schedule_check(State, DelayMs).

schedule_check(State, DelayMs) ->
    State1 = cancel_timer(State),
    Ref = erlang:send_after(DelayMs, self(), ?CHECK_MSG),
    State1#state{timer_ref = Ref}.

cancel_timer(#state{timer_ref = undefined} = State) ->
    State;
cancel_timer(#state{timer_ref = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    State#state{timer_ref = undefined}.

%%--------------------------------------------------------------------
%% Internal: async worker
%%--------------------------------------------------------------------

%% Spawn a monitored worker to run the actual ACME flow. The worker
%% exits with its result wrapped in a tagged tuple, so success and crash
%% are both delivered via the same DOWN message — no extra signalling
%% channel needed. run_action/1 is wrapped in a catch-all so that any
%% exception raised deep in acme-erlang-client (e.g. idna's
%% `exit:{bad_label, _}` on a malformed domain) lands in last_result as
%% a structured `{error, {Class, Reason}}` and the gen_server clears
%% in_progress the same way it does for normal errors.
spawn_worker(Action) ->
    {Pid, Ref} = spawn_monitor(fun() ->
        Result =
            try
                run_action(Action)
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        exit({?WORKER_RESULT, Result})
    end),
    #{
        action => Action,
        started_at => calendar:universal_time(),
        worker => {Pid, Ref}
    }.

run_action(check) ->
    Settings = emqx_acme_config:settings(),
    check_and_renew(Settings);
run_action(issue) ->
    do_issue();
run_action(renew) ->
    do_issue().

%% Worker finished (success or structured failure). Stamp the result
%% and clear in_progress.
finish(Result, State) ->
    State#state{
        last_result = Result,
        last_check = calendar:universal_time(),
        in_progress = false
    }.

%% Periodic timer fired. Three reasons to skip the tick:
%%   1. A worker is already running on this node.
%%   2. This node is not the cluster's ACME leader — only the leader
%%      issues / renews; replicants and non-leader cores keep ticking
%%      so they pick up the duty if the leader leaves the cluster.
%%   3. (implicit) mria has no running core nodes yet.
%% The timer reschedules unconditionally so leadership changes are
%% picked up at the next tick.
maybe_start_check(#state{in_progress = false} = State) ->
    case is_leader() of
        true -> State#state{in_progress = spawn_worker(check)};
        false -> State
    end;
maybe_start_check(State) ->
    State.

format_in_progress(false) ->
    false;
format_in_progress(#{action := Action, started_at := StartedAt}) ->
    #{action => Action, started_at => format_datetime(StartedAt)}.

%% Status is JSON-encoded by the plugin API gateway, and the encoder
%% rejects Erlang tuples. Map ok/undefined through verbatim; flatten
%% error tuples into a single binary so curl-driven smoke tests and the
%% dashboard see the cause instead of a 500.
format_last_result(undefined) -> null;
format_last_result(ok) -> ok;
format_last_result({error, Reason}) -> #{error => iolist_to_binary(io_lib:format("~p", [Reason]))}.

%%--------------------------------------------------------------------
%% Internal: certificate issuance
%%--------------------------------------------------------------------

check_and_renew(Settings) ->
    #{
        cert_bundle_name := BundleName,
        renew_before_expiry_days := RenewDays
    } = Settings,
    case emqx_managed_certs:list_managed_files(?global_ns, BundleName) of
        {ok, #{chain := #{path := ChainPath}}} ->
            case read_cert_expiry(ChainPath) of
                {ok, ExpiryTime} ->
                    NowSecs = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
                    ExpirySecs = calendar:datetime_to_gregorian_seconds(ExpiryTime),
                    RemainingDays = (ExpirySecs - NowSecs) / 86400,
                    if
                        RemainingDays =< RenewDays ->
                            ?LOG(info, #{
                                msg => "certificate_renewal_needed",
                                remaining_days => RemainingDays,
                                renew_threshold => RenewDays
                            }),
                            do_issue();
                        true ->
                            ?LOG(debug, #{
                                msg => "certificate_still_valid",
                                remaining_days => RemainingDays
                            }),
                            ok
                    end;
                {error, Reason} ->
                    ?LOG(warning, #{msg => "cert_expiry_read_failed", reason => Reason}),
                    do_issue()
            end;
        _ ->
            ?LOG(info, #{msg => "no_existing_certificate", bundle => BundleName}),
            do_issue()
    end.

do_issue() ->
    Settings = emqx_acme_config:settings(),
    #{
        dir_url := DirUrl,
        domains := Domains,
        contact := Contact,
        cert_type := CertType,
        challenge_port := ChallengePort,
        cert_bundle_name := BundleName,
        listener_ids := ListenerIds,
        enable_dashboard_https := EnableDashHttps,
        dashboard_https_port := DashHttpsPort
    } = Settings,
    case Domains of
        [] ->
            {error, no_domains_configured};
        _ ->
            PreState = pre_state(?global_ns, BundleName),
            Params = #{
                dir_url => DirUrl,
                domains => Domains,
                contact => Contact,
                cert_type => CertType,
                challenge_port => ChallengePort,
                bundle_name => BundleName,
                listener_ids => ListenerIds,
                enable_dashboard_https => EnableDashHttps,
                dashboard_https_port => DashHttpsPort,
                pre_state => PreState,
                acc_key_config => maps:get(acc_key, Settings, undefined),
                acc_key_password_config => maps:get(acc_key_password, Settings, undefined)
            },
            run_acme(Params)
    end.

%% Returns bundle_in_use when both chain and key already exist in the bundle,
%% otherwise bundle_empty. Captured before issuance so the Scenario-A/B branch
%% reflects the state the listener was in at boot, not the state right after we
%% wrote the new files.
pre_state(Namespace, BundleName) ->
    case emqx_managed_certs:list_managed_files(Namespace, BundleName) of
        {ok, #{?FILE_KIND_CHAIN := _, ?FILE_KIND_KEY := _}} ->
            bundle_in_use;
        _ ->
            bundle_empty
    end.

run_acme(#{bundle_name := BundleName, challenge_port := ChallengePort} = Params) ->
    AccKeyOpt = build_acc_key_opt(Params, BundleName),
    ChallengeFn = fun(Challenges) ->
        emqx_acme_challenge:set_challenges(Challenges),
        ok
    end,
    Request = maps:merge(
        #{
            dir_url => maps:get(dir_url, Params),
            domains => maps:get(domains, Params),
            contact => maps:get(contact, Params),
            cert_type => maps:get(cert_type, Params),
            challenge_type => <<"http-01">>,
            challenge_fn => ChallengeFn
        },
        AccKeyOpt
    ),
    ?LOG(info, #{msg => "starting_certificate_issuance", domains => maps:get(domains, Params)}),
    %% Fan the HTTP-01 listener out to every cluster node so an NLB
    %% routing the CA's validation request to a replicant or non-leader
    %% core gets answered locally (with a remote lookup back to us for
    %% the token).
    case emqx_acme_challenge:cluster_start_listener(ChallengePort) of
        ok ->
            try
                issue_and_store(Request, Params)
            after
                _ = emqx_acme_challenge:cluster_stop_listener()
            end;
        {error, _} = Error ->
            Error
    end.

issue_and_store(Request, Params) ->
    case acme_client_issuance:run(Request, ?ISSUANCE_TIMEOUT) of
        {ok, Result} ->
            store_result(Result, Params);
        {error, Reason} = Error ->
            ?LOG(error, #{msg => "acme_issuance_failed", reason => Reason}),
            Error
    end.

store_result(Result, #{bundle_name := BundleName, pre_state := PreState} = Params) ->
    #{
        cert_key := CertKey,
        cert_chain := CertChain
    } = Result,
    CertKeyPem = encode_private_key(CertKey),
    ChainPem = encode_cert_chain(CertChain),
    %% The ACME account key is operator-managed (or auto-generated by the
    %% plugin at the configured path). The bundle stores only the issued
    %% chain + key — never the account key.
    Files = #{
        ?FILE_KIND_KEY => CertKeyPem,
        ?FILE_KIND_CHAIN => ChainPem
    },
    case emqx_managed_certs:add_managed_files(?global_ns, BundleName, Files) of
        ok ->
            ?LOG(info, #{msg => "certificate_stored", bundle => BundleName}),
            ok = maybe_migrate_listeners(PreState, BundleName, maps:get(listener_ids, Params)),
            maybe_enable_dashboard_https(
                PreState,
                BundleName,
                maps:get(enable_dashboard_https, Params, false),
                maps:get(dashboard_https_port, Params, ?DEFAULT_DASHBOARD_HTTPS_PORT)
            );
        {error, Reason} = Error ->
            ?LOG(error, #{msg => "certificate_store_failed", reason => Reason}),
            Error
    end.

%% Two modes:
%%   - acc_key_config set: operator manages the file (e.g. mounted Secret).
%%     We pass the path through to acme-erlang-client and expect it to
%%     exist; ensure_acc_key_file/2 will lazily auto-generate it if absent
%%     on the local node (back-compat with the old single-node UX).
%%   - acc_key_config undefined (the default): the plugin manages the
%%     account key inside the cert bundle. ensure_bundle_acc_key/2
%%     generates one on first issuance and stores it via emqx_managed_certs,
%%     which replicates it to every cluster node alongside the cert chain
%%     -- so the cluster shares one ACME identity without operator setup.
build_acc_key_opt(Params, BundleName) ->
    AccKeyUri =
        case emqx_acme_config:resolve_file_uri(maps:get(acc_key_config, Params)) of
            undefined ->
                ensure_bundle_acc_key(BundleName, maps:get(cert_type, Params));
            Path ->
                ok = ensure_acc_key_file(Path, maps:get(cert_type, Params)),
                Path
        end,
    Base = #{acc_key => AccKeyUri},
    case maps:get(acc_key_password_config, Params, undefined) of
        undefined -> Base;
        Secret -> Base#{acc_key_pass => unwrap_password(Secret)}
    end.

%% acme_client passes acc_key_pass straight through to
%% public_key:pem_entry_decode/2, which only accepts string()
%% (charlist) — a binary throws function_clause. emqx_secret_loader
%% returns the file contents as a binary (with the trailing newline
%% trimmed); inline passwords are also stored as binaries by the
%% parser. unicode:characters_to_list/1 covers both.
unwrap_password(Secret) ->
    unicode:characters_to_list(emqx_secret:unwrap(Secret)).

%% Bundle-managed account key. emqx_managed_certs has a first-class slot
%% (?FILE_KIND_ACC_KEY -> "acc-key.pem") for exactly this. add_managed_files
%% replicates to every cluster node via BPAPI, so once the first issuance
%% generates the key, every node has the same identity for renewals.
%% No chmod is applied: replication uses plain file:write_file/2 on each
%% node, so any local mode tweak would be inconsistent across the cluster
%% -- the bundle dir's directory perms (owned by the emqx user) provide
%% access control for the whole certs2/ tree.
ensure_bundle_acc_key(BundleName, CertType) ->
    case emqx_managed_certs:list_managed_files(?global_ns, BundleName) of
        {ok, #{?FILE_KIND_ACC_KEY := #{path := Path}}} ->
            "file://" ++ Path;
        _ ->
            ?LOG(info, #{
                msg => "generating_acme_account_key",
                bundle => BundleName,
                cert_type => CertType
            }),
            Pem = encode_private_key(acme_client_lib:generate_key(CertType)),
            ok = emqx_managed_certs:add_managed_files(
                ?global_ns, BundleName, #{?FILE_KIND_ACC_KEY => Pem}
            ),
            {ok, #{?FILE_KIND_ACC_KEY := #{path := Path}}} =
                emqx_managed_certs:list_managed_files(?global_ns, BundleName),
            "file://" ++ Path
    end.

%% Operator-managed acc_key path. Auto-generates on the local node only
%% when the file is absent (back-compat for single-node deployments that
%% pointed acc_key at a writable file outside the bundle).
ensure_acc_key_file("file://" ++ FsPath0, CertType) ->
    FsPath = emqx_utils_schema:naive_env_interpolation(FsPath0),
    case filelib:is_regular(FsPath) of
        true ->
            ok;
        false ->
            ?LOG(info, #{
                msg => "generating_acme_account_key",
                path => FsPath,
                cert_type => CertType
            }),
            ok = filelib:ensure_dir(FsPath),
            Pem = encode_private_key(acme_client_lib:generate_key(CertType)),
            ok = file:write_file(FsPath, Pem),
            ok
    end.

%% Scenario A: the bundle was already populated before this issuance, so the
%% SSL listeners are already configured to read from it. We rely on Erlang
%% ssl's PEM cache (default 2 minute mtime poll) to pick up the new files
%% transparently. No listener config change.
%%
%% Scenario B: the bundle was empty. The configured listeners must have been
%% running with their static cert paths (or disabled). Rewrite each existing
%% one's config to consume the bundle. emqx_conf:update/3 with override_to
%% cluster propagates and emqx_listeners:post_config_update/5 restarts the
%% listener on each node.
maybe_migrate_listeners(bundle_in_use, _BundleName, _ListenerIds) ->
    ok;
maybe_migrate_listeners(bundle_empty, BundleName, ListenerIds) ->
    migrate_listeners(BundleName, ListenerIds).

%% After a successful issuance, ensure the dashboard's HTTPS listener is
%% configured and pointed at this bundle. Only acts on the first issuance
%% (bundle_empty) so that operator-driven changes to the dashboard listener
%% are not clobbered on subsequent renewals. The existing HTTP listener is
%% left running — operators flip it off via the plugin UI once they've
%% confirmed HTTPS works.
maybe_enable_dashboard_https(_PreState, _BundleName, false, _Port) ->
    ok;
maybe_enable_dashboard_https(bundle_in_use, _BundleName, true, _Port) ->
    ok;
maybe_enable_dashboard_https(bundle_empty, BundleName, true, Port) ->
    enable_dashboard_https(BundleName, Port).

enable_dashboard_https(BundleName, Port) ->
    %% The dashboard's listener config validator (emqx_dashboard_listener_config)
    %% does not yet understand managed_certs and requires keyfile + certfile.
    %% Point them straight at the bundle's stable paths — Erlang's ssl PEM
    %% cache (2-minute mtime poll) reloads in place when renewals rewrite
    %% those files, so we still get auto-renewal without listener restart.
    case emqx_managed_certs:list_managed_files(?global_ns, BundleName) of
        {ok, #{?FILE_KIND_CHAIN := #{path := ChainPath}, ?FILE_KIND_KEY := #{path := KeyPath}}} ->
            do_enable_dashboard_https(BundleName, Port, ChainPath, KeyPath);
        Other ->
            ?LOG(error, #{
                msg => "dashboard_https_enable_failed",
                reason => bundle_files_unavailable,
                detail => Other
            }),
            ok
    end.

do_enable_dashboard_https(BundleName, Port, ChainPath, KeyPath) ->
    %% pre_config_update lives at [dashboard] and deep-merges the request
    %% into the existing config, so we update the parent to inject the
    %% https listener without disturbing the http listener.
    Update = #{
        <<"listeners">> => #{
            <<"https">> => #{
                <<"bind">> => list_to_binary(
                    "0.0.0.0:" ++ integer_to_list(Port)
                ),
                <<"ssl_options">> => #{
                    <<"certfile">> => ChainPath,
                    <<"keyfile">> => KeyPath
                }
            }
        }
    },
    case emqx_conf:update([dashboard], Update, #{override_to => cluster}) of
        {ok, _} ->
            ?LOG(info, #{
                msg => "dashboard_https_enabled",
                bundle => BundleName,
                port => Port
            }),
            ok;
        {error, Reason} ->
            ?LOG(error, #{
                msg => "dashboard_https_enable_failed",
                bundle => BundleName,
                reason => Reason
            }),
            ok
    end.

migrate_listeners(_BundleName, []) ->
    ok;
migrate_listeners(BundleName, [{Type, Name} | Rest]) ->
    _ = migrate_one_listener(BundleName, Type, Name),
    migrate_listeners(BundleName, Rest).

migrate_one_listener(BundleName, Type, Name) ->
    case emqx_config:find_listener_conf(Type, Name, []) of
        {not_found, _, _} ->
            ?LOG(info, #{
                msg => "skipping_unknown_listener",
                listener_type => Type,
                listener_name => Name
            }),
            ok;
        {ok, _Conf} ->
            %% emqx_listeners:pre_config_update/3 expects {update, Request}
            %% (or {create, ...}, {action, ...}) — passing a bare map falls
            %% through every clause and crashes function_clause.
            NewConf =
                {update, #{
                    <<"enable">> => true,
                    <<"ssl_options">> => #{
                        <<"managed_certs">> => #{
                            <<"bundle_name">> => BundleName
                        }
                    }
                }},
            ConfPath = [listeners, Type, Name],
            case emqx_conf:update(ConfPath, NewConf, #{override_to => cluster}) of
                {ok, _} ->
                    ?LOG(info, #{
                        msg => "listener_migrated_to_managed_cert_bundle",
                        listener_type => Type,
                        listener_name => Name,
                        bundle => BundleName
                    }),
                    ok;
                {error, Reason} = Error ->
                    ?LOG(error, #{
                        msg => "listener_migration_failed",
                        listener_type => Type,
                        listener_name => Name,
                        reason => Reason
                    }),
                    Error
            end
    end.

%%--------------------------------------------------------------------
%% Internal: PEM encoding/decoding
%%--------------------------------------------------------------------

encode_private_key(#'ECPrivateKey'{} = Key) ->
    PemEntry = public_key:pem_entry_encode('ECPrivateKey', Key),
    public_key:pem_encode([PemEntry]);
encode_private_key(#'RSAPrivateKey'{} = Key) ->
    PemEntry = public_key:pem_entry_encode('RSAPrivateKey', Key),
    public_key:pem_encode([PemEntry]).

encode_cert_chain(Certs) when is_list(Certs) ->
    PemEntries = lists:map(
        fun(Cert) ->
            Der = public_key:pkix_encode('OTPCertificate', Cert, otp),
            {'Certificate', Der, not_encrypted}
        end,
        Certs
    ),
    public_key:pem_encode(PemEntries).

%%--------------------------------------------------------------------
%% Internal: certificate info
%%--------------------------------------------------------------------

read_cert_expiry(ChainPath) ->
    case file:read_file(ChainPath) of
        {ok, PemBin} ->
            case public_key:pem_decode(PemBin) of
                [{'Certificate', Der, _} | _] ->
                    OTPCert = public_key:pkix_decode_cert(Der, otp),
                    TBSCert = OTPCert#'OTPCertificate'.tbsCertificate,
                    Validity = TBSCert#'OTPTBSCertificate'.validity,
                    #'Validity'{notAfter = NotAfter} = Validity,
                    parse_asn1_time(NotAfter);
                _ ->
                    {error, no_certificate_in_pem}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

parse_asn1_time({utcTime, TimeStr}) ->
    %% YYMMDDhhmmssZ
    <<YY:2/binary, MM:2/binary, DD:2/binary, HH:2/binary, Mi:2/binary, SS:2/binary, _/binary>> = list_to_binary(
        TimeStr
    ),
    Year0 = binary_to_integer(YY),
    Year =
        if
            Year0 >= 50 -> 1900 + Year0;
            true -> 2000 + Year0
        end,
    {ok,
        {{Year, binary_to_integer(MM), binary_to_integer(DD)}, {
            binary_to_integer(HH), binary_to_integer(Mi), binary_to_integer(SS)
        }}};
parse_asn1_time({generalTime, TimeStr}) ->
    %% YYYYMMDDhhmmssZ
    <<YYYY:4/binary, MM:2/binary, DD:2/binary, HH:2/binary, Mi:2/binary, SS:2/binary, _/binary>> = list_to_binary(
        TimeStr
    ),
    {ok,
        {{binary_to_integer(YYYY), binary_to_integer(MM), binary_to_integer(DD)}, {
            binary_to_integer(HH), binary_to_integer(Mi), binary_to_integer(SS)
        }}}.

get_cert_info(Settings) ->
    #{cert_bundle_name := BundleName} = Settings,
    %% emqx_managed_certs wraps each kind in #{path := <<Path/binary>>}
    %% (alongside future metadata); take care to thread the inner path
    %% through to file readers, not the wrapper map.
    case emqx_managed_certs:list_managed_files(?global_ns, BundleName) of
        {ok, #{chain := #{path := ChainPath}, key := #{path := KeyPath}}} ->
            Base = #{
                exists => true,
                chain_path => ChainPath,
                key_path => KeyPath
            },
            case read_cert_expiry(ChainPath) of
                {ok, Expiry} -> Base#{expiry => format_datetime(Expiry)};
                {error, _} -> Base#{expiry => unknown}
            end;
        _ ->
            #{exists => false}
    end.

format_datetime(undefined) ->
    null;
format_datetime({{Y, M, D}, {H, Mi, S}}) ->
    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ", [Y, M, D, H, Mi, S])
    ).
