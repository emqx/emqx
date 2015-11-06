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
-export([start_link/1, pool/0]).

-export([start_session/2, lookup_session/1]).

-export([register_session/3, unregister_session/2]).

-behaviour(gen_server2).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 priorities
-export([prioritise_call/4, prioritise_cast/3, prioritise_info/3]).

-record(state, {id}).

-define(SM_POOL, ?MODULE).

-define(TIMEOUT, 60000).

-define(LOG(Level, Format, Args, Session),
            lager:Level("SM(~s): " ++ Format, [Session#mqtt_session.client_id | Args])).

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
-spec start_link(Id :: pos_integer()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Id) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id], []).

name(Id) ->
    list_to_atom("emqttd_sm_" ++ integer_to_list(Id)).

%%------------------------------------------------------------------------------
%% @doc Pool name.
%% @end
%%------------------------------------------------------------------------------
pool() -> ?SM_POOL.

%%------------------------------------------------------------------------------
%% @doc Start a session
%% @end
%%------------------------------------------------------------------------------
-spec start_session(CleanSess :: boolean(), binary()) -> {ok, pid(), boolean()} | {error, any()}.
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
        []        -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Register a session with info.
%% @end
%%------------------------------------------------------------------------------
-spec register_session(CleanSess, ClientId, Info) -> ok when
    CleanSess :: boolean(),
    ClientId  :: binary(),
    Info      :: [tuple()].
register_session(CleanSess, ClientId, Info) ->
    ets:insert(sesstab(CleanSess), {{ClientId, self()}, Info}).

%%------------------------------------------------------------------------------
%% @doc Unregister a session.
%% @end
%%------------------------------------------------------------------------------
-spec unregister_session(CleanSess, ClientId) -> ok when
    CleanSess :: boolean(),
    ClientId  :: binary().
unregister_session(CleanSess, ClientId) ->
    ets:delete(sesstab(CleanSess), {ClientId, self()}).

sesstab(true)  -> mqtt_transient_session;
sesstab(false) -> mqtt_persistent_session.

call(SM, Req) ->
    gen_server2:call(SM, Req, ?TIMEOUT). %%infinity).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id]) ->
    gproc_pool:connect_worker(?SM_POOL, {?MODULE, Id}),
    {ok, #state{id = Id}}.

prioritise_call(_Msg, _From, _Len, _State) ->
    1.

prioritise_cast(_Msg, _Len, _State) ->
    0.

prioritise_info(_Msg, _Len, _State) ->
    2.

%% persistent session
handle_call({start_session, {false, ClientId, ClientPid}}, _From, State) ->
    case lookup_session(ClientId) of
        undefined ->
            %% create session locally
            reply(create_session(false, ClientId, ClientPid), false, State);
        Session ->
            reply(resume_session(Session, ClientPid), true, State)
    end;

%% transient session
handle_call({start_session, {true, ClientId, ClientPid}}, _From, State) ->
    case lookup_session(ClientId) of
        undefined ->
            reply(create_session(true, ClientId, ClientPid), false, State);
        Session ->
            case destroy_session(Session) of
                ok ->
                    reply(create_session(true, ClientId, ClientPid), false, State);
                {error, Error} ->
                    {reply, {error, Error}, State}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:error("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State) ->
    mnesia:transaction(fun() ->
                [mnesia:delete_object(session, Sess, write) || Sess 
                    <- mnesia:index_read(session, DownPid, #mqtt_session.sess_pid)]
        end),
    {noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
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
                                    persistent = not CleanSess},
            case insert_session(Session) of
                {aborted, {conflict, ConflictPid}} ->
                    %% Conflict with othe node?
                    lager:error("SM(~s): Conflict with ~p", [ClientId, ConflictPid]),
                    {error, mnesia_conflict};
                {atomic, ok} ->
                    erlang:monitor(process, SessPid),
                    {ok, SessPid}
            end;
        {error, Error} ->
            {error, Error}
    end.

insert_session(Session = #mqtt_session{client_id = ClientId}) ->
    mnesia:transaction(
      fun() ->
        case mnesia:wread({session, ClientId}) of
            [] ->
                mnesia:write(session, Session, write);
            [#mqtt_session{sess_pid = SessPid}] ->
                mnesia:abort({conflict, SessPid})
        end
      end).

%% Local node
resume_session(Session = #mqtt_session{client_id = ClientId,
                                       sess_pid  = SessPid}, ClientPid)
    when node(SessPid) =:= node() ->

    case is_process_alive(SessPid) of
        true ->
            emqttd_session:resume(SessPid, ClientId, ClientPid),
            {ok, SessPid};
        false ->
            ?LOG(error, "Cannot resume ~p which seems already dead!", [SessPid], Session),
            {error, session_died}
    end;

%% Remote node
resume_session(Session = #mqtt_session{client_id = ClientId, sess_pid = SessPid}, ClientPid) ->
    Node = node(SessPid),
    case rpc:call(Node, emqttd_session, resume, [SessPid, ClientId, ClientPid]) of
        ok ->
            {ok, SessPid};
        {badrpc, nodedown} ->
            ?LOG(error, "Session died for node '~s' down", [Node], Session),
            remove_session(Session),
            {error, session_nodedown};
        {badrpc, Reason} ->
            ?LOG(error, "Failed to resume from node ~s for ~p", [Node, Reason], Session),
            {error, Reason}
    end.

%% Local node
destroy_session(Session = #mqtt_session{client_id = ClientId, sess_pid  = SessPid})
    when node(SessPid) =:= node() ->
    emqttd_session:destroy(SessPid, ClientId),
    remove_session(Session);

%% Remote node
destroy_session(Session = #mqtt_session{client_id = ClientId,
                                        sess_pid  = SessPid}) ->
    Node = node(SessPid),
    case rpc:call(Node, emqttd_session, destroy, [SessPid, ClientId]) of
        ok ->
            remove_session(Session);
        {badrpc, nodedown} ->
            ?LOG(error, "Node '~s' down", [Node], Session),
            remove_session(Session); 
        {badrpc, Reason} ->
            ?LOG(error, "Failed to destory ~p on remote node ~p for ~s",
                 [SessPid, Node, Reason], Session),
            {error, Reason}
     end.

remove_session(Session) ->
    case mnesia:transaction(fun mnesia:delete_object/3, [session, Session, write]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end.

reply({ok, SessPid}, SP, State) ->
    {reply, {ok, SessPid, SP}, State};
reply({error, Error}, _SP, State) ->
    {reply, {error, Error}, State}.

