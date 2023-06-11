%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-logger_header("[Hooks]").

-export([ start_link/0
        , stop/0
        ]).

%% Hooks API
-export([ add/2
        , add/3
        , add/4
        , put/2
        , put/3
        , put/4
        , del/2
        , run/2
        , run_fold/3
        , lookup/1
        , reorder_acl_callbacks/0
        , reorder_auth_callbacks/0
        ]).

-export([ callback_action/1
        , callback_filter/1
        , callback_priority/1
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
-type(action() :: function() | {function(), [term()]} | mfargs()).
-type(filter() :: function() | mfargs()).

-record(callback, {
          action :: action(),
          filter :: maybe(filter()),
          priority :: integer()
         }).

-type callback() :: #callback{}.

-record(hook, {
          name :: hookpoint(),
          callbacks :: list(#callback{})
         }).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(UNKNOWN_ORDER, 999999999).

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER},
                          ?MODULE, [], [{hibernate_after, 1000}]).

-spec(stop() -> ok).
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
-spec(add(hookpoint(), action() | callback()) -> ok_or_error(already_exists)).
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

%% @doc Like add/2, it register a callback, discard 'already_exists' error.
-spec put(hookpoint(), action() | callback()) -> ok.
put(HookPoint, Callback) when is_record(Callback, callback) ->
    case add(HookPoint, Callback) of
        ok -> ok;
        {error, already_exists} -> ok
    end;
put(HookPoint, Action) when is_function(Action); is_tuple(Action) ->
    ?MODULE:put(HookPoint, #callback{action = Action, priority = 0}).

-spec put(hookpoint(), action(), filter() | integer() | list()) -> ok.
put(HookPoint, Action, {_M, _F, _A} = Filter) ->
    ?MODULE:put(HookPoint, #callback{action = Action, filter = Filter, priority = 0});
put(HookPoint, Action, Priority) when is_integer(Priority) ->
    ?MODULE:put(HookPoint, #callback{action = Action, priority = Priority}).

-spec put(hookpoint(), action(), filter(), integer()) -> ok.
put(HookPoint, Action, Filter, Priority) when is_integer(Priority) ->
    ?MODULE:put(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

%% @doc Unregister a callback.
-spec(del(hookpoint(), action() | {module(), atom()}) -> ok).
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
        Error:Reason:Stacktrace ->
            ?LOG(error, "Failed to execute ~0p: ~0p", [Fun, {Error, Reason, Stacktrace}]),
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
-spec(lookup(hookpoint()) -> [callback()]).
lookup(HookPoint) ->
    case ets:lookup(?TAB, HookPoint) of
        [#hook{callbacks = Callbacks}] ->
            Callbacks;
        [] -> []
    end.

%% @doc Reorder ACL check callbacks
-spec reorder_acl_callbacks() -> ok.
reorder_acl_callbacks() ->
    gen_server:cast(?SERVER, {reorder_callbacks, 'client.check_acl'}).

%% @doc Reorder Authentication check callbacks
-spec reorder_auth_callbacks() -> ok.
reorder_auth_callbacks() ->
    gen_server:cast(?SERVER, {reorder_callbacks, 'client.authenticate'}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(?TAB, [{keypos, #hook.name}, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({add, HookPoint, Callback = #callback{action = Action}}, _From, State) ->
    Callbacks = lookup(HookPoint),
    Reply = case lists:keymember(Action, #callback.action, Callbacks) of
                true ->
                    {error, already_exists};
                false ->
                    ok = add_and_insert(HookPoint, [Callback],  Callbacks)
            end,
    {reply, Reply, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({reorder_callbacks, HookPoint}, State) ->
    Callbacks = lookup(HookPoint),
    case Callbacks =:= [] of
        true ->
            %% no callbaks, make sure not to insert []
            ok;
        false ->
            ok = add_and_insert(HookPoint, Callbacks, [])
    end,
    {noreply, State};
handle_cast({del, HookPoint, Action}, State) ->
    case del_callback(Action, lookup(HookPoint)) of
        [] ->
            ets:delete(?TAB, HookPoint);
        Callbacks ->
            ok = insert_hook(HookPoint, Callbacks)
    end,
    ?tp(debug, emqx_hook_removed, #{hookpoint => HookPoint, action => Action}),
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

add_and_insert(HookPoint, NewCallbacks, Callbacks) ->
    HookOrder = get_hook_order(HookPoint),
    NewCallbaks = add_callbacks(HookOrder, NewCallbacks, Callbacks),
    ok = insert_hook(HookPoint, NewCallbaks).

get_hook_order('client.authenticate') ->
    get_auth_acl_hook_order(auth_order);
get_hook_order('client.check_acl') ->
    get_auth_acl_hook_order(acl_order);
get_hook_order(_) ->
    [].

get_auth_acl_hook_order(AppEnvName) ->
    case emqx:get_env(AppEnvName) of
        [_|_] = CSV ->
            %% non-empty string
            parse_auth_acl_hook_order(AppEnvName, CSV);
        _ ->
            []
    end.

parse_auth_acl_hook_order(auth_order, CSV) ->
    parse_auth_acl_hook_order(fun parse_auth_name/1, CSV);
parse_auth_acl_hook_order(acl_order, CSV) ->
    parse_auth_acl_hook_order(fun parse_acl_name/1, CSV);
parse_auth_acl_hook_order(NameParser, CSV) when is_function(NameParser) ->
    do_parse_auth_acl_hook_order(NameParser, string:tokens(CSV, ", ")).

do_parse_auth_acl_hook_order(_, []) -> [];
do_parse_auth_acl_hook_order(Parser, ["none" | Names]) ->
    %% "none" is the default config value
    do_parse_auth_acl_hook_order(Parser, Names);
do_parse_auth_acl_hook_order(Parser, [Name0 | Names]) ->
    Name = Parser(Name0),
    [Name | do_parse_auth_acl_hook_order(Parser, Names)].

%% NOTE: It's ugly to enumerate plugin names here.
%% But it's the most straightforward way.
parse_auth_name("http") -> "emqx_auth_http";
parse_auth_name("jwt") -> "emqx_auth_jwt";
parse_auth_name("ldap") -> "emqx_auth_ldap";
parse_auth_name("mnesia") -> "emqx_auth_mnesia";
parse_auth_name("mongodb") -> "emqx_auth_mongo";
parse_auth_name("mongo") -> "emqx_auth_mongo";
parse_auth_name("mysql") -> "emqx_auth_mysql";
parse_auth_name("pgsql") -> "emqx_auth_pgsql";
parse_auth_name("postgres") -> "emqx_auth_pgsql";
parse_auth_name("redis") -> "emqx_auth_redis";
parse_auth_name(Other) -> Other. %% maybe a user defined plugin or the module name directly

parse_acl_name("file") -> "emqx_mod_acl_internal";
parse_acl_name("internal") -> "emqx_mod_acl_internal";
parse_acl_name("http") -> "emqx_acl_http";
parse_acl_name("jwt") -> "emqx_auth_jwt"; %% this is not a typo, there is no emqx_acl_jwt module
parse_acl_name("ldap") -> "emqx_acl_ldap";
parse_acl_name("mnesia") -> "emqx_acl_mnesia";
parse_acl_name("mongo") -> "emqx_acl_mongo";
parse_acl_name("mongodb") -> "emqx_acl_mongo";
parse_acl_name("mysql") -> "emqx_acl_mysql";
parse_acl_name("pgsql") -> "emqx_acl_pgsql";
parse_acl_name("postgres") -> "emqx_acl_pgsql";
parse_acl_name("redis") -> "emqx_acl_redis";
parse_acl_name(Other) -> Other. %% maybe a user defined plugin or the module name directly

insert_hook(HookPoint, Callbacks) ->
    ets:insert(?TAB, #hook{name = HookPoint, callbacks = Callbacks}),
    ok.

add_callbacks(_Order, [], Callbacks) ->
    Callbacks;
add_callbacks(Order, [C | More], Callbacks) ->
    NewCallbacks = add_callback(Order, C, Callbacks),
    add_callbacks(Order, More, NewCallbacks).

add_callback(Order, C, Callbacks) ->
    add_callback(Order, C, Callbacks, []).

add_callback(_Order, C, [], Acc) ->
    lists:reverse([C|Acc]);
add_callback(Order, C1, [C2|More], Acc) ->
    case is_lower_priority(Order, C1, C2) of
        true ->
            add_callback(Order, C1, More, [C2|Acc]);
        false ->
            lists:append(lists:reverse(Acc), [C1, C2 | More])
    end.

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


%% does A have lower priority than B?
is_lower_priority(Order,
                  #callback{priority = PrA, action = ActA},
                  #callback{priority = PrB, action = ActB}) ->
    PosA = callback_position(Order, ActA),
    PosB = callback_position(Order, ActB),
    case PosA =:= PosB of
        true ->
            %% When priority is equal, the new callback (A) goes after the existing (B) hence '=<'
            PrA =< PrB;
        false ->
            %% When OrdA > OrdB the new callback (A) positioned after the exiting (B)
            PosA > PosB
    end.

callback_position(Order, Callback) ->
    M = callback_module(Callback),
    find_list_item_position(Order, atom_to_list(M)).

callback_module({M, _F, _A}) -> M;
callback_module({F, _A}) when is_function(F) ->
    {module, M} = erlang:fun_info(F, module),
    M;
callback_module(F) when is_function(F) ->
    {module, M} = erlang:fun_info(F, module),
    M.

find_list_item_position(Order, Name) ->
    find_list_item_position(Order, Name, 1).

find_list_item_position([], _ModuleName, _N) ->
    %% Not found, make sure it's ordered behind the found ones
    ?UNKNOWN_ORDER;
find_list_item_position([Prefix | Rest], ModuleName, N) ->
    case is_prefix(Prefix, ModuleName) of
        true ->
            N;
        false ->
            find_list_item_position(Rest, ModuleName, N + 1)
    end.

is_prefix(Prefix, ModuleName) ->
    case string:prefix(ModuleName, Prefix) of
        nomatch ->
            false;
        _Sufix ->
            true
    end.

-ifdef(TEST).
add_priority_rules_test_() ->
    [{ "high prio",
       fun() ->
               OrderString = "foo, bar",
               Existing = [make_hook(0, emqx_acl_pgsql), make_hook(0, emqx_acl_mysql)],
               New = make_hook(1, emqx_acl_mnesia),
               Expected = [New | Existing],
               ?assertEqual(Expected, test_add_acl(OrderString, New, Existing))
       end},
     { "low prio",
       fun() ->
               OrderString = "foo, bar",
               Existing = [make_hook(0, emqx_auth_jwt), make_hook(0, emqx_acl_mongo)],
               New = make_hook(-1, emqx_acl_mnesia),
               Expected = Existing++ [New],
               ?assertEqual(Expected, test_add_acl(OrderString, New, Existing))
       end},
     { "mid prio",
       fun() ->
               OrderString = "",
               Existing = [make_hook(3, emqx_acl_http), make_hook(1, emqx_acl_redis)],
               New = make_hook(2, emqx_acl_ldap),
               Expected = [hd(Existing), New | tl(Existing)],
               ?assertEqual(Expected, test_add_acl(OrderString, New, Existing))
       end}
    ].

add_order_rules_test_() ->
    [{"initial add",
       fun() ->
               OrderString = "ldap,pgsql,file",
               Existing = [],
               New = make_hook(2, foo),
               ?assertEqual([New], test_add_auth(OrderString, New, Existing))
       end},
     { "before",
       fun() ->
               OrderString = "mongodb,postgres,internal",
               Existing = [make_hook(1, emqx_auth_pgsql), make_hook(3, emqx_auth_mysql)],
               New = make_hook(2, emqx_auth_mongo),
               Expected = [New | Existing],
               ?assertEqual(Expected, test_add_auth(OrderString, New, Existing))
       end},
     { "after",
       fun() ->
               OrderString = "mysql,postgres,ldap",
               Existing = [make_hook(1, emqx_auth_pgsql), make_hook(3, emqx_auth_mysql)],
               New = make_hook(2, emqx_auth_ldap),
               Expected = Existing ++ [New],
               ?assertEqual(Expected, test_add_auth(OrderString, New, Existing))
       end},
     { "unknown goes after knowns",
       fun() ->
               OrderString = "mongo,mysql,,mnesia", %% ,, is intended to test empty string
               Existing = [make_hook(1, emqx_auth_mnesia), make_hook(3, emqx_auth_mysql)],
               New1 = make_hook(2, fun() -> foo end), %% fake hook
               New2 = make_hook(3, {fun lists:append/1, []}), %% fake hook
               Expected1 = Existing ++ [New1],
               Expected2 = Existing ++ [New2, New1], %% 2 is before 1 due to higher prio
               ?assertEqual(Expected1, test_add_auth(OrderString, New1, Existing)),
               ?assertEqual(Expected2, test_add_auth(OrderString, New2, Expected1))
       end},
     { "known goes first",
       fun() ->
               OrderString = "redis,jwt",
               Existing = [make_hook(1, emqx_auth_mnesia), make_hook(3, emqx_auth_mysql)],
               Redis = make_hook(2, emqx_auth_redis),
               Jwt = make_hook(2, emqx_auth_jwt),
               Expected1 = [Redis | Existing],
               ?assertEqual(Expected1, test_add_auth(OrderString, Redis, Existing)),
               Expected2 = [Redis, Jwt | Existing],
               ?assertEqual(Expected2, test_add_auth(OrderString, Jwt, Expected1))
       end}
    ].

make_hook(Priority, CallbackModule) when is_atom(CallbackModule) ->
    #callback{priority = Priority, action = {CallbackModule, dummy, []}};
make_hook(Priority, F) ->
    #callback{priority = Priority, action = F}.

test_add_acl(OrderString, NewHook, ExistingHooks) ->
    Order = parse_auth_acl_hook_order(acl_order, OrderString),
    add_callback(Order, NewHook, ExistingHooks).

test_add_auth(OrderString, NewHook, ExistingHooks) ->
    Order = parse_auth_acl_hook_order(auth_order, OrderString),
    add_callback(Order, NewHook, ExistingHooks).

-endif.
