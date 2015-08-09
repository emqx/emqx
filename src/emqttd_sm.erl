%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd session manager.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_sm).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Function Exports
-export([start_link/2, pool/0]).

-export([start_session/2, lookup_session/1]).

-export([register_session/3, unregister_session/2]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id, statsfun}).

-define(SM_POOL, sm_pool).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================

mnesia(boot) ->
    %% global session...
    ok = emqttd_mnesia:create_table(session, [
                {type, ordered_set},
                {ram_copies, [node()]},
                {record_name, mqtt_session},
                {attributes, record_info(fields, mqtt_session)},
                {index, [sess_pid]}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(session).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start a session manager
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Id, StatsFun) -> {ok, pid()} | ignore | {error, any()} when
        Id       :: pos_integer(),
        StatsFun :: fun().
start_link(Id, StatsFun) ->
    gen_server:start_link(?MODULE, [Id, StatsFun], []).

%%------------------------------------------------------------------------------
%% @doc Pool name.
%% @end
%%------------------------------------------------------------------------------
pool() -> ?SM_POOL.

%%------------------------------------------------------------------------------
%% @doc Start a session
%% @end
%%------------------------------------------------------------------------------
-spec start_session(CleanSess :: boolean(), binary()) -> {ok, pid()} | {error, any()}.
start_session(CleanSess, ClientId) ->
    SM = gproc_pool:pick_worker(?SM_POOL, ClientId),
    call(SM, {start_session, {CleanSess, ClientId, self()}}).

%%------------------------------------------------------------------------------
%% @doc Lookup a Session
%% @end
%%------------------------------------------------------------------------------
-spec lookup_session(binary()) -> pid() | undefined.
lookup_session(ClientId) ->
    case mnesia:dirty_read(session, ClientId) of
        [Session] -> Session;
        [] -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Register a session with info.
%% @end
%%------------------------------------------------------------------------------
-spec register_session(CleanSess, ClientId, Info) -> ok when
    CleanSess :: boolean(),
    ClientId :: binary(),
    Info :: [tuple()].
register_session(CleanSess, ClientId, Info) ->
    SM = gproc_pool:pick_worker(?SM_POOL, ClientId),
    gen_server:cast(SM, {register, CleanSess, ClientId, Info}).

%%------------------------------------------------------------------------------
%% @doc Unregister a session.
%% @end
%%------------------------------------------------------------------------------
-spec unregister_session(CleanSess, ClientId) -> ok when
    CleanSess :: boolean(),
    ClientId :: binary().
unregister_session(CleanSess, ClientId) ->
    SM = gproc_pool:pick_worker(?SM_POOL, ClientId),
    gen_server:cast(SM, {unregister, CleanSess, ClientId}).

call(SM, Req) -> gen_server:call(SM, Req, infinity).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id, StatsFun]) ->
    gproc_pool:connect_worker(?SM_POOL, {?MODULE, Id}),
    {ok, #state{id = Id, statsfun = StatsFun}}.

%% persistent session
handle_call({start_session, {false, ClientId, ClientPid}}, _From, State) ->
    case lookup_session(ClientId) of
        undefined ->
            %% create session locally
            {reply, create_session(false, ClientId, ClientPid), State};
        Session ->
            {reply, resume_session(Session, ClientPid), State}
    end;

handle_call({start_session, {true, ClientId, ClientPid}}, _From, State) ->
    case lookup_session(ClientId) of
        undefined ->
            {reply, create_session(true, ClientId, ClientPid), State};
        Session ->
            case destroy_session(Session) of
                ok ->
                    {reply, create_session(true, ClientId, ClientPid), State};
                {error, Error} ->
                    {reply, {error, Error}, State}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% transient session
handle_cast({register, true, ClientId, Info}, State) ->
    ets:insert(mqtt_transient_session, {ClientId, Info}),
    {noreply, State};

handle_cast({register, false, ClientId, Info}, State) ->
    ets:insert(mqtt_persistent_session, {ClientId, Info}),
    {noreply, setstats(State)};

handle_cast({unregister, true, ClientId}, State) ->
    ets:delete(mqtt_transient_session, ClientId),
    {noreply, State};

handle_cast({unregister, false, ClientId}, State) ->
    ets:delete(mqtt_persistent_session, ClientId),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State) ->
    mnesia:transaction(fun() ->
                [mnesia:delete_object(session, Sess, write) || Sess 
                    <- mnesia:index_read(session, DownPid, #mqtt_session.sess_pid)]
        end),
    {noreply, setstats(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{id = Id}) ->
    gproc_pool:disconnect_worker(?SM_POOL, {?MODULE, Id}), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

create_session(CleanSess, ClientId, ClientPid) ->
    case emqttd_session_sup:start_session(CleanSess, ClientId, ClientPid) of
        {ok, SessPid} ->
            Session = #mqtt_session{client_id  = ClientId,
                                    sess_pid   = SessPid,
                                    persistent = not CleanSess,
                                    on_node    = node()},
            case insert_session(Session) of
                {aborted, {conflict, Node}} ->
                    %% conflict with othe node?
                    lager:critical("Session ~s conflict with node ~p!", [ClientId, Node]),
                    {error, conflict};
                {atomic, ok} ->
                    erlang:monitor(process, SessPid),
                    {ok, SessPid}
            end;
        {error, Error} ->
            {error, Error}
    end.

insert_session(Session = #mqtt_session{client_id = ClientId}) ->
    mnesia:transaction(fun() ->
                case mnesia:wread({session, ClientId}) of
                    [] ->
                        mnesia:write(session, Session, write);
                    [#mqtt_session{on_node = Node}] ->
                        mnesia:abort({conflict, Node})
                end
        end).

%% local node
resume_session(#mqtt_session{client_id = ClientId,
                             sess_pid  = SessPid,
                             on_node   = Node}, ClientPid)
        when Node =:= node() ->
    case is_process_alive(SessPid) of 
        true ->
            emqttd_session:resume(SessPid, ClientId, ClientPid),
            {ok, SessPid};
        false ->
            lager:critical("Session ~s@~p died unexpectedly!", [ClientId, SessPid]),
            {error, session_died}
    end;

%% remote node
resume_session(Session = #mqtt_session{client_id = ClientId,
                                       sess_pid = SessPid,
                                       on_node = Node}, ClientPid) ->
    case emqttd:is_running(Node) of
        true ->
            case rpc:call(Node, emqttd_session, resume, [SessPid, ClientId, ClientPid]) of
                ok ->
                    {ok, SessPid};
                {badrpc, Reason} ->
                    lager:critical("Resume session ~s on remote node ~p failed for ~p",
                                    [ClientId, Node, Reason]),
                    {error, list_to_atom("session_" ++ atom_to_list(Reason))}
            end;
        false ->
            lager:critical("Session ~s died for node ~p down!", [ClientId, Node]),
            remove_session(Session),
            {error, session_node_down}
    end.

%% local node
destroy_session(Session = #mqtt_session{client_id = ClientId,
                                        sess_pid  = SessPid,
                                        on_node   = Node}) when Node =:= node() ->
    case is_process_alive(SessPid) of
        true ->
            emqttd_session:destroy(SessPid, ClientId);
        false ->
            lager:critical("Session ~s@~p died unexpectedly!", [ClientId, SessPid])
    end,
    case remove_session(Session) of
        {atomic, ok} -> ok;
        {aborted, Error} -> {error, Error}
    end;

%% remote node
destroy_session(Session = #mqtt_session{client_id = ClientId,
                                        sess_pid  = SessPid,
                                        on_node   = Node}) ->
    case emqttd:is_running(Node) of
        true ->
            case rpc:call(Node, emqttd_session, destroy, [SessPid, ClientId]) of
                ok ->
                    case remove_session(Session) of
                        {atomic, ok} -> ok;
                        {aborted, Error} -> {error, Error}
                    end;
                {badrpc, Reason} ->
                    lager:critical("Destroy session ~s on remote node ~p failed for ~p",
                                    [ClientId, Node, Reason]),
                    {error, list_to_atom("session_" ++ atom_to_list(Reason))}
             end;
        false ->
            lager:error("Session ~s died for node ~p down!", [ClientId, Node]),
            case remove_session(Session) of
                {atomic, ok} -> ok;
                {aborted, Error} -> {error, Error}
            end
    end.

remove_session(Session) ->
    mnesia:transaction(fun() ->
            mnesia:delete_object(session, Session, write)
        end).

setstats(State = #state{statsfun = _StatsFun}) ->
    State.


