%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hook_manager).
-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include("emqx_hooks_internal.hrl").

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(PTERM, emqx_hooks).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], [{hibernate_after, 1000}]).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE, normal, infinity).

init([]) ->
    _ = erlang:process_flag(trap_exit, true),
    ok = emqx_hookpoints:register_hookpoints(),
    ok = delete_all_hooks(),
    {ok, #{}}.

handle_call({add, HookPoint, Callback = #callback{action = {M, F, _}}}, _From, State) ->
    Callbacks = emqx_hooks:lookup(HookPoint),
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
    Callbacks = del_callback({M, F}, emqx_hooks:lookup(HookPoint)),
    Reply = insert_hook(HookPoint, add_callback(Callback, Callbacks)),
    {reply, Reply, State};
handle_call({del, HookPoint, Action}, _From, State) ->
    ok = do_del(HookPoint, Action),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

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

do_del(HookPoint, Action) ->
    case del_callback(Action, emqx_hooks:lookup(HookPoint)) of
        [] ->
            delete_hook(HookPoint);
        Callbacks ->
            insert_hook(HookPoint, Callbacks)
    end,
    ok.

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
