%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hooks).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    start_link/0,
    stop/0
]).

%% Hooks API
-export([
    add/3,
    add/4,
    put/3,
    put/4,
    del/2,
    run/2,
    run_fold/3,
    lookup/1
]).

-export([
    context/1,
    stash_context/2,
    unstash_context/1
]).

-export([
    callback_action/1,
    callback_filter/1,
    callback_priority/1
]).

%% gen_server Function Exports
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([
    hookpoint/0,
    action/0,
    filter/0
]).

%% Multiple callbacks can be registered on a hookpoint.
%% The execution order depends on the priority value:
%%   - Callbacks with greater priority values will be run before
%%     the ones with lower priority values. e.g. A Callback with
%%     priority = 2 precedes the callback with priority = 1.
%%   - If the priorities of the hooks are equal then their execution
%%     order is determined by the lexicographic of hook function
%%     names.

-type hookpoint() :: atom() | binary().
-type action() :: {module(), atom(), [term()] | undefined}.
-type filter() :: {module(), atom(), [term()] | undefined}.

-record(callback, {
    action :: action(),
    filter :: option(filter()),
    priority :: integer()
}).

-type callback() :: #callback{}.

-define(PTERM, ?MODULE).
-define(SERVER, ?MODULE).

-define(HOOK_CTX_PD_KEY(NAME), {?MODULE, ctx, NAME}).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link(
        {local, ?SERVER},
        ?MODULE,
        [],
        [{hibernate_after, 1000}]
    ).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER, normal, infinity).

%%--------------------------------------------------------------------
%% Test APIs
%%--------------------------------------------------------------------

%% @doc Get callback action.
callback_action(#callback{action = A}) -> A.

%% @doc Get callback filter.
callback_filter(#callback{filter = F}) -> F.

%% @doc Get callback priority.
callback_priority(#callback{priority = P}) -> P.

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

%% @doc `add/3,4` add a new hook, returns 'already_exists' if the hook exists.
-spec add(hookpoint(), action(), integer()) ->
    ok_or_error(already_exists).
add(HookPoint, Action, Priority) when is_integer(Priority) ->
    do_add(HookPoint, #callback{action = Action, priority = Priority}).

-spec add(hookpoint(), action(), integer(), filter()) ->
    ok_or_error(already_exists).
add(HookPoint, Action, Priority, Filter) when is_integer(Priority) ->
    do_add(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

do_add(HookPoint, Callback) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    gen_server:call(?SERVER, {add, HookPoint, Callback}, infinity).

%% @doc `put/3,4` updates the existing hook, add it if not exists.
-spec put(hookpoint(), action(), integer()) -> ok.
put(HookPoint, Action, Priority) when is_integer(Priority) ->
    do_put(HookPoint, #callback{action = Action, priority = Priority}).

-spec put(hookpoint(), action(), integer(), filter()) -> ok.
put(HookPoint, Action, Priority, Filter) when is_integer(Priority) ->
    do_put(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

do_put(HookPoint, Callback) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    case do_add(HookPoint, Callback) of
        ok -> ok;
        {error, already_exists} -> gen_server:call(?SERVER, {put, HookPoint, Callback}, infinity)
    end.

%% @doc Unregister a callback.
-spec del(hookpoint(), action() | {module(), atom()}) -> ok.
del(HookPoint, Action) ->
    gen_server:cast(?SERVER, {del, HookPoint, Action}).

%% @doc Run hooks.
-spec run(hookpoint(), list(Arg :: term())) -> ok.
run(HookPoint, Args) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    do_run(lookup(HookPoint), Args).

%% @doc Run hooks with Accumulator.
-spec run_fold(hookpoint(), list(Arg :: term()), Acc :: term()) -> Acc :: term().
run_fold(HookPoint, Args, Acc) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    do_run_fold(lookup(HookPoint), Args, Acc).

do_run([#callback{action = Action, filter = Filter} | Callbacks], Args) ->
    case filter_passed(Filter, Args) andalso safe_execute(Action, Args) of
        %% stop the hook chain and return
        stop -> ok;
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop'
        _ -> do_run(Callbacks, Args)
    end;
do_run([], _Args) ->
    ok.

do_run_fold([#callback{action = Action, filter = Filter} | Callbacks], Args, Acc) ->
    Args1 = Args ++ [Acc],
    case filter_passed(Filter, Args1) andalso safe_execute(Action, Args1) of
        %% stop the hook chain
        stop -> Acc;
        %% stop the hook chain with NewAcc
        {stop, NewAcc} -> NewAcc;
        %% continue the hook chain with NewAcc
        {ok, NewAcc} -> do_run_fold(Callbacks, Args, NewAcc);
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop' or {'stop', NewAcc}
        _ -> do_run_fold(Callbacks, Args, Acc)
    end;
do_run_fold([], _Args, Acc) ->
    Acc.

-spec filter_passed(filter(), Args :: term()) -> true | false.
filter_passed(undefined, _Args) -> true;
filter_passed(Filter, Args) -> execute(Filter, Args).

safe_execute({M, F, A}, Args) ->
    try execute({M, F, A}, Args) of
        Result -> Result
    catch
        Error:Reason:Stacktrace ->
            ?tp(error, "hook_callback_exception", #{
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace,
                callback_module => M,
                callback_function => F,
                callback_args => emqx_utils_redact:redact(Args ++ A)
            })
    end.

%% @doc execute a function.
execute({M, F, A}, Args) ->
    erlang:apply(M, F, Args ++ A).

%% @doc Lookup callbacks.
-spec lookup(hookpoint()) -> [callback()].
lookup(HookPoint) ->
    persistent_term:get({?PTERM, HookPoint}, []).

%%--------------------------------------------------------------------
%% Context stashing
%%   We need this here to maintain backwards compatibility while still being able to
%%   thread more context down to hooks.
%%--------------------------------------------------------------------

-spec context(hookpoint()) -> undefined | term().
context(Name) ->
    get(?HOOK_CTX_PD_KEY(Name)).

-spec stash_context(hookpoint(), term()) -> ok.
stash_context(Name, Context) ->
    _ = put(?HOOK_CTX_PD_KEY(Name), Context),
    ok.

-spec unstash_context(hookpoint()) -> ok.
unstash_context(Name) ->
    _ = erase(?HOOK_CTX_PD_KEY(Name)),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = erlang:process_flag(trap_exit, true),
    ok = emqx_hookpoints:register_hookpoints(),
    ok = delete_all_hooks(),
    {ok, #{}}.

handle_call({add, HookPoint, Callback = #callback{action = {M, F, _}}}, _From, State) ->
    Callbacks = lookup(HookPoint),
    Existing = lists:any(
        fun(#callback{action = {M0, F0, _}}) ->
            M0 =:= M andalso F0 =:= F
        end,
        Callbacks
    ),
    Reply =
        case Existing of
            true -> {error, already_exists};
            false -> insert_hook(HookPoint, add_callback(Callback, Callbacks))
        end,
    {reply, Reply, State};
handle_call({put, HookPoint, Callback = #callback{action = {M, F, _}}}, _From, State) ->
    Callbacks = del_callback({M, F}, lookup(HookPoint)),
    Reply = insert_hook(HookPoint, add_callback(Callback, Callbacks)),
    {reply, Reply, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast({del, HookPoint, Action}, State) ->
    case del_callback(Action, lookup(HookPoint)) of
        [] ->
            delete_hook(HookPoint);
        Callbacks ->
            insert_hook(HookPoint, Callbacks)
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = delete_all_hooks().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

insert_hook(HookPoint, Callbacks) ->
    persistent_term:put({?PTERM, HookPoint}, Callbacks).

delete_hook(HookPoint) ->
    persistent_term:erase({?PTERM, HookPoint}).

delete_all_hooks() ->
    maps:foreach(
        fun(HookPoint, _) -> delete_hook(HookPoint) end,
        emqx_hookpoints:registered_hookpoints()
    ).

add_callback(
    C1 = #callback{priority = P1, action = MFA1},
    [C2 = #callback{priority = P2, action = MFA2} | More] = Cs
) ->
    case (P1 < P2) orelse (P1 =:= P2 andalso MFA1 >= MFA2) of
        true ->
            [C2 | add_callback(C1, More)];
        false ->
            [C1 | Cs]
    end;
add_callback(C1, []) ->
    [C1].

del_callback(Action, Callbacks) ->
    del_callback(Action, Callbacks, []).

del_callback(_Action, [], Acc) ->
    lists:reverse(Acc);
del_callback(Action, [#callback{action = Action} | Callbacks], Acc) ->
    del_callback(Action, Callbacks, Acc);
del_callback(Action = {M, F}, [#callback{action = {M, F, _A}} | Callbacks], Acc) ->
    del_callback(Action, Callbacks, Acc);
del_callback(Action, [Callback | Callbacks], Acc) ->
    del_callback(Action, Callbacks, [Callback | Acc]).
