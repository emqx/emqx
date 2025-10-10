%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    %% RPC target (`emqx_retainer_proto_v2:wait_dispatch_complete/2`)
    wait_dispatch_complete/1,
    register_reader/2,
    worker/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(POOL, ?DISPATCHER_POOL).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%%===================================================================
%%% API
%%%===================================================================

%% RPC target (`emqx_retainer_proto_v2:wait_dispatch_complete/2`)
-spec wait_dispatch_complete(timeout()) -> ok.
wait_dispatch_complete(Timeout) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            ok = gen_server:call(Pid, ?FUNCTION_NAME, Timeout)
        end,
        Workers
    ).

-spec register_reader(pid(), integer()) -> ok.
register_reader(Pid, Incarnation) ->
    cast({register, Pid, Incarnation}).

-spec worker() -> pid().
worker() ->
    gproc_pool:pick_worker(?POOL, self()).

-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    State = #{
        pool => Pool,
        id => Id,
        %% Monitors dispatchers
        readers => emqx_pmon:new(),
        %% Incarnation number -> count of dispatchers using it
        incarnations => gb_trees:empty(),
        %% Callers waiting for dispatchers older than current incarnation to
        %% complete, keyed on greatest incarnation number they are waiting on to be
        %% empty.  Used by reindex.
        callers => gb_trees:empty()
    },
    {ok, State}.

handle_call(wait_dispatch_complete, From, #{incarnations := Incarnations} = State0) ->
    #{callers := Callers0} = State0,
    Incarnation = current_index_incarnation(),
    case gb_trees:smaller(Incarnation, Incarnations) of
        none ->
            {reply, ok, State0};
        {OlderIncarnation, _Count} ->
            Callers = add_caller(Callers0, OlderIncarnation, From),
            State = State0#{callers := Callers},
            {noreply, State}
    end;
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({register, Pid, Incarnation}, State0) ->
    #{
        readers := Readers0,
        incarnations := Incarnations0
    } = State0,
    Incarnations =
        case emqx_pmon:find(Pid, Readers0) of
            error ->
                inc_incarnation_counter(Incarnations0, Incarnation);
            {ok, _} ->
                %% Pid is already registered
                Incarnations0
        end,
    Readers = emqx_pmon:monitor(Pid, Incarnation, Readers0),
    State = State0#{readers := Readers, incarnations := Incarnations},
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, _} = Info, State0) ->
    #{
        readers := Readers0,
        incarnations := Incarnations0,
        callers := Callers0
    } = State0,
    case emqx_pmon:find(Pid, Readers0) of
        error ->
            ?SLOG(error, #{msg => "unexpected_info", info => Info}),
            {noreply, State0};
        {ok, Incarnation} ->
            Readers = emqx_pmon:erase(Pid, Readers0),
            {Incarnations, Callers} =
                case dec_incarnation_counter(Incarnations0, Incarnation) of
                    {ok, Incarnations1} ->
                        %% Not the last one
                        {Incarnations1, Callers0};
                    {last, Incarnations1} ->
                        Callers1 = notify_callers(Callers0, Incarnation),
                        {Incarnations1, Callers1}
                end,
            State = State0#{
                readers := Readers,
                incarnations := Incarnations,
                callers := Callers
            },
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cast(Msg) ->
    gen_server:cast(worker(), Msg).

inc_incarnation_counter(Tree, Incarnation) ->
    case gb_trees:lookup(Incarnation, Tree) of
        none ->
            gb_trees:insert(Incarnation, 1, Tree);
        {value, N} ->
            gb_trees:update(Incarnation, N + 1, Tree)
    end.

dec_incarnation_counter(Tree, Incarnation) ->
    case gb_trees:lookup(Incarnation, Tree) of
        none ->
            {ok, Tree};
        {value, 1} ->
            {last, gb_trees:delete(Incarnation, Tree)};
        {value, N} ->
            {ok, gb_trees:update(Incarnation, N - 1, Tree)}
    end.

add_caller(Callers0, Incarnation, From) ->
    case gb_trees:lookup(Incarnation, Callers0) of
        none ->
            gb_trees:insert(Incarnation, [From], Callers0);
        {value, Froms} ->
            gb_trees:update(Incarnation, [From | Froms], Callers0)
    end.

notify_callers(Callers0, Incarnation) ->
    case gb_trees:lookup(Incarnation, Callers0) of
        none ->
            Callers0;
        {value, Waiting} ->
            Callers1 = gb_trees:delete(Incarnation, Callers0),
            case gb_trees:smaller(Incarnation, Callers1) of
                none ->
                    lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Waiting),
                    Callers1;
                {OlderIncarnation, Froms} ->
                    %% If there are even older readers running, wait for them as well.
                    gb_trees:update(OlderIncarnation, Froms ++ Waiting, Callers1)
            end
    end.

current_index_incarnation() ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:current_index_incarnation(State).
