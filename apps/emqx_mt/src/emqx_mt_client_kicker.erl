%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_client_kicker).

-behaviour(gen_server).

%% API
-export([
    start_kicking/1,
    stop/1,
    whereis_kicker/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports
-export([
    start_link/1
]).

-include("emqx_mt.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(ID(NS), {?MODULE, NS}).
-define(SUP, emqx_mt_sup).

-define(ns, ns).
-define(last_seen_clientid, last_seen_clientid).

-define(kick, kick).

-define(BATCH_SIZE, 1_000).
-define(KICK_INTERVAL, 250).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_kicking(emqx_mt:tns()) -> ok | {error, already_started}.
start_kicking(Ns) ->
    ChildSpec = child_spec(Ns),
    Node =
        case mria_rlog:role() of
            core ->
                node();
            replicant ->
                mria_membership:coordinator()
        end,
    case supervisor:start_child({?SUP, Node}, ChildSpec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            {error, already_started}
    end.

-spec stop(emqx_mt:tns()) -> ok.
stop(Ns) ->
    case whereis_kicker(Ns) of
        {ok, Pid} ->
            Node = node(Pid),
            case supervisor:terminate_child({?SUP, Node}, ?ID(Ns)) of
                ok ->
                    ok;
                {error, not_found} ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.

-spec whereis_kicker(emqx_mt:tns()) -> {ok, pid()} | {error, not_found}.
whereis_kicker(Ns) ->
    case global:whereis_name(?ID(Ns)) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        undefined ->
            {error, not_found}
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Ns) ->
    State = #{?ns => Ns, ?last_seen_clientid => ?MIN_CLIENTID},
    {ok, State, {continue, ?kick}}.

handle_continue(?kick, State0) ->
    case handle_kick(State0) of
        {continue, State} ->
            {noreply, State};
        {stop, State} ->
            {stop, normal, State}
    end.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(?kick, State0) ->
    case handle_kick(State0) of
        {continue, State} ->
            {noreply, State};
        {stop, State} ->
            {stop, normal, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

start_link(Ns) ->
    gen_server:start_link({global, ?ID(Ns)}, ?MODULE, Ns, []).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

child_spec(Ns) ->
    #{
        id => ?ID(Ns),
        start => {?MODULE, start_link, [Ns]},
        restart => temporary,
        shutdown => 5_000,
        type => worker
    }.

handle_kick(State0) ->
    #{
        ?ns := Ns,
        ?last_seen_clientid := LastSeenClientId
    } = State0,
    case emqx_mt_state:list_clients_no_check(Ns, LastSeenClientId, ?BATCH_SIZE) of
        [] ->
            {stop, State0};
        [_ | _] = ClientIds ->
            LastClientId = lists:last(ClientIds),
            _ = kick_clients(ClientIds),
            start_kick_timer(),
            State = State0#{?last_seen_clientid := LastClientId},
            {continue, State}
    end.

kick_clients(ClientIds) ->
    emqx_mgmt:kickout_clients(ClientIds).

start_kick_timer() ->
    _ = erlang:send_after(?KICK_INTERVAL, self(), ?kick),
    ok.
