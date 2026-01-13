%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_gc).

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    register_client/1,
    unregister_client/1
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(clients, clients).

%% Calls/casts/infos/continues
-record(register_client, {pid}).
-record(unregister_client, {pid}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

register_client(Pid) ->
    gen_server:cast(?MODULE, #register_client{pid = Pid}).

unregister_client(Pid) ->
    gen_server:cast(?MODULE, #unregister_client{pid = Pid}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    State = #{
        ?clients => #{}
    },
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#register_client{pid = Pid}, State0) ->
    State =
        case State0 of
            #{?clients := #{Pid := _}} ->
                State0;
            #{?clients := Clients0} ->
                _ = monitor(process, Pid),
                State0#{?clients := Clients0#{Pid => true}}
        end,
    {noreply, State};
handle_cast(#unregister_client{pid = Pid}, State0) ->
    #{?clients := Clients0} = State0,
    Clients = maps:remove(Pid, Clients0),
    State = State0#{?clients := Clients},
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', _, _, Pid, _}, #{?clients := Clients0} = State0) when
    is_map_key(Pid, Clients0)
->
    Clients = maps:remove(Pid, Clients0),
    State = State0#{?clients := Clients},
    emqx_schema_registry_spb_state:cleanup_entries_for(Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
