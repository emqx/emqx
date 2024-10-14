%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(alinkdata_hooks).

-export([
    start_link/0,
    stop/0
]).

%% Hooks API
-export([
    add/2,
    add/3,
    add/4,
    put/2,
    put/3,
    put/4,
    del/2,
    run/2,
    run_fold/3,
    lookup/1
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


%% Multiple callbacks can be registered on a hookpoint.
%% The execution order depends on the priority value:
%%   - Callbacks with greater priority values will be run before
%%     the ones with lower priority values. e.g. A Callback with
%%     priority = 2 precedes the callback with priority = 1.
%%   - The execution order is the adding order of callbacks if they have
%%     equal priority values.


-record(callback, {
    action,
    filter,
    priority
}).



-record(hook, {
    name,
    callbacks
}).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).


start_link() ->
    gen_server:start_link({local, ?SERVER},
        ?MODULE, [], [{hibernate_after, 1000}]).


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
callback_priority(#callback{priority= P}) -> P.

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

%% @doc Register a callback

add(HookPoint, Callback) when is_record(Callback, callback) ->
    gen_server:call(?SERVER, {add, HookPoint, Callback}, infinity);
add(HookPoint, Action) when is_function(Action); is_tuple(Action) ->
    add(HookPoint, #callback{action = Action, priority = 0}).


add(HookPoint, Action, InitArgs) when is_function(Action), is_list(InitArgs) ->
    add(HookPoint, #callback{action = {Action, InitArgs}, priority = 0});
add(HookPoint, Action, Filter) when is_function(Filter); is_tuple(Filter) ->
    add(HookPoint, #callback{action = Action, filter = Filter, priority = 0});
add(HookPoint, Action, Priority) when is_integer(Priority) ->
    add(HookPoint, #callback{action = Action, priority = Priority}).


add(HookPoint, Action, Filter, Priority) when is_integer(Priority) ->
    add(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

%% @doc Like add/2, it register a callback, discard 'already_exists' error.

put(HookPoint, Callback) when is_record(Callback, callback) ->
    case add(HookPoint, Callback) of
        ok -> ok;
        {error, already_exists} -> ok
    end;
put(HookPoint, Action) when is_function(Action); is_tuple(Action) ->
    ?MODULE:put(HookPoint, #callback{action = Action, priority = 0}).


put(HookPoint, Action, {_M, _F, _A} = Filter) ->
    ?MODULE:put(HookPoint, #callback{action = Action, filter = Filter, priority = 0});
put(HookPoint, Action, Priority) when is_integer(Priority) ->
    ?MODULE:put(HookPoint, #callback{action = Action, priority = Priority}).


put(HookPoint, Action, Filter, Priority) when is_integer(Priority) ->
    ?MODULE:put(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

%% @doc Unregister a callback.

del(HookPoint, Action) ->
    gen_server:cast(?SERVER, {del, HookPoint, Action}).

%% @doc Run hooks.

run(HookPoint, Args) ->
    do_run(lookup(HookPoint), Args).

%% @doc Run hooks with Accumulator.

run_fold(HookPoint, Args, Acc) ->
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
        {stop, NewAcc}   -> NewAcc;
        %% continue the hook chain with NewAcc
        {ok, NewAcc}   -> do_run_fold(Callbacks, Args, NewAcc);
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop' or {'stop', NewAcc}
        _ -> do_run_fold(Callbacks, Args, Acc)
    end;
do_run_fold([], _Args, Acc) ->
    Acc.


filter_passed(undefined, _Args) -> true;
filter_passed(Filter, Args) ->
    execute(Filter, Args).

safe_execute(Fun, Args) ->
    try execute(Fun, Args) of
        Result -> Result
    catch
        Error:Reason:Stacktrace ->
            logger:error("Failed to execute ~0p: ~0p", [Fun, {Error, Reason, Stacktrace}]),
            ok
    end.

%% @doc execute a function.
execute(Fun, Args) when is_function(Fun) ->
    erlang:apply(Fun, Args);
execute({Fun, InitArgs}, Args) when is_function(Fun) ->
    erlang:apply(Fun, Args ++ InitArgs);
execute({M, F, A}, Args) ->
    erlang:apply(M, F, Args ++ A).

%% @doc Lookup callbacks.

lookup(HookPoint) ->
    case ets:lookup(?TAB, HookPoint) of
        [#hook{callbacks = Callbacks}] ->
            Callbacks;
        [] -> []
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = alinkutil_tables:new(?TAB, [{keypos, #hook.name}, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({add, HookPoint, Callback = #callback{action = Action}}, _From, State) ->
    Reply = case lists:keymember(Action, #callback.action, Callbacks = lookup(HookPoint)) of
                true ->
                    {error, already_exists};
                false ->
                    insert_hook(HookPoint, add_callback(Callback, Callbacks))
            end,
    {reply, Reply, State};

handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({del, HookPoint, Action}, State) ->
    case del_callback(Action, lookup(HookPoint)) of
        [] ->
            ets:delete(?TAB, HookPoint);
        Callbacks ->
            insert_hook(HookPoint, Callbacks)
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    logger:error("Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    logger:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

insert_hook(HookPoint, Callbacks) ->
    ets:insert(?TAB, #hook{name = HookPoint, callbacks = Callbacks}), ok.

add_callback(C, Callbacks) ->
    add_callback(C, Callbacks, []).

add_callback(C, [], Acc) ->
    lists:reverse([C|Acc]);
add_callback(C1 = #callback{priority = P1}, [C2 = #callback{priority = P2}|More], Acc)
    when P1 =< P2 ->
    add_callback(C1, More, [C2|Acc]);
add_callback(C1, More, Acc) ->
    lists:append(lists:reverse(Acc), [C1 | More]).

del_callback(Action, Callbacks) ->
    del_callback(Action, Callbacks, []).

del_callback(_Action, [], Acc) ->
    lists:reverse(Acc);
del_callback(Action, [#callback{action = Action} | Callbacks], Acc) ->
    del_callback(Action, Callbacks, Acc);
del_callback(Action = {M, F}, [#callback{action = {M, F, _A}} | Callbacks], Acc) ->
    del_callback(Action, Callbacks, Acc);
del_callback(Func, [#callback{action = {Func, _A}} | Callbacks], Acc) ->
    del_callback(Func, Callbacks, Acc);
del_callback(Action, [Callback | Callbacks], Acc) ->
    del_callback(Action, Callbacks, [Callback | Acc]).

