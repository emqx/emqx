%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_hooks).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").

-logger_header("[Hooks]").

-export([ start_link/0
        , stop/0
        ]).

%% Hooks API
-export([ add/2
        , add/3
        , add/4
        , del/2
        , run/2
        , run_fold/3
        , lookup/1
        ]).

%% gen_server Function Exports
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([ hookpoint/0
             , action/0
             , filter/0
             ]).

%% Multiple callbacks can be registered on a hookpoint.
%% The execution order depends on the priority value:
%%   - Callbacks with greater priority values will be run before
%%     the ones with lower priority values. e.g. A Callback with
%%     priority = 2 precedes the callback with priority = 1.
%%   - The execution order is the adding order of callbacks if they have
%%     equal priority values.

-type(hookpoint() :: atom()).
-type(action() :: function() | mfa()).
-type(filter() :: function() | mfa()).

-record(callback, {
          action :: action(),
          filter :: filter(),
          priority :: integer()
         }).

-record(hook, {
          name :: hookpoint(),
          callbacks :: list(#callback{})
         }).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER},
                          ?MODULE, [], [{hibernate_after, 1000}]).

-spec(stop() -> ok).
stop() ->
    gen_server:stop(?SERVER, normal, infinity).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

%% @doc Register a callback
-spec(add(hookpoint(), action() | #callback{}) -> ok_or_error(already_exists)).
add(HookPoint, Callback) when is_record(Callback, callback) ->
    gen_server:call(?SERVER, {add, HookPoint, Callback}, infinity);
add(HookPoint, Action) when is_function(Action); is_tuple(Action) ->
    add(HookPoint, #callback{action = Action, priority = 0}).

-spec(add(hookpoint(), action(), filter() | integer() | list())
      -> ok_or_error(already_exists)).
add(HookPoint, Action, InitArgs) when is_function(Action), is_list(InitArgs) ->
    add(HookPoint, #callback{action = {Action, InitArgs}, priority = 0});
add(HookPoint, Action, Filter) when is_function(Filter); is_tuple(Filter) ->
    add(HookPoint, #callback{action = Action, filter = Filter, priority = 0});
add(HookPoint, Action, Priority) when is_integer(Priority) ->
    add(HookPoint, #callback{action = Action, priority = Priority}).

-spec(add(hookpoint(), action(), filter(), integer())
      -> ok_or_error(already_exists)).
add(HookPoint, Action, Filter, Priority) when is_integer(Priority) ->
    add(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

%% @doc Unregister a callback.
-spec(del(hookpoint(), action()) -> ok).
del(HookPoint, Action) ->
    gen_server:cast(?SERVER, {del, HookPoint, Action}).

%% @doc Run hooks.
-spec(run(atom(), list(Arg::term())) -> ok).
run(HookPoint, Args) ->
    do_run(lookup(HookPoint), Args).

%% @doc Run hooks with Accumulator.
-spec(run_fold(atom(), list(Arg::term()), Acc::term()) -> Acc::term()).
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

-spec(filter_passed(filter(), Args::term()) -> true | false).
filter_passed(undefined, _Args) -> true;
filter_passed(Filter, Args) ->
    execute(Filter, Args).

safe_execute(Fun, Args) ->
    try execute(Fun, Args) of
        Result -> Result
    catch
        _:Reason:Stacktrace ->
            ?LOG(error, "Failed to execute ~0p: ~0p", [Fun, {Reason, Stacktrace}]),
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
-spec(lookup(hookpoint()) -> [#callback{}]).
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
    ok = emqx_tables:new(?TAB, [{keypos, #hook.name}, {read_concurrency, true}]),
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
    ?LOG(error, "Unexpected call: ~p", [Req]),
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
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
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

