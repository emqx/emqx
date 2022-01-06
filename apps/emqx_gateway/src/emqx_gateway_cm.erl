%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The Gateway Connection-Manager
%%
%% For a certain type of protocol, this is a single instance of the manager.
%% It means that no matter how many instances of the stomp gateway are created,
%% they all share a single this Connection-Manager
-module(emqx_gateway_cm).

-behaviour(gen_server).

-include("include/emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").


%% APIs
-export([start_link/1]).

-export([ open_session/5
        , open_session/6
        , kick_session/2
        , kick_session/3
        , register_channel/4
        , unregister_channel/2
        , insert_channel_info/4
        , set_chan_info/3
        , set_chan_info/4
        , get_chan_info/2
        , get_chan_info/3
        , set_chan_stats/3
        , set_chan_stats/4
        , get_chan_stats/2
        , get_chan_stats/3
        , connection_closed/2
        ]).

-export([ with_channel/3
        , lookup_channels/2
        ]).

%% Internal funcs for getting tabname by GatewayId
-export([cmtabs/1, tabname/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          gwname    :: gateway_name(), %% Gateway Name
          locker    :: pid(),          %% ClientId Locker for CM
          registry  :: pid(),          %% ClientId Registry server
          chan_pmon :: emqx_pmon:pmon()
         }).

-type option() :: {gwname, gateway_name()}.
-type options() :: list(option()).

-define(T_TAKEOVER, 15000).
-define(DEFAULT_BATCH_SIZE, 10000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(options()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Options) ->
    GwName = proplists:get_value(gwname, Options),
    gen_server:start_link({local, procname(GwName)}, ?MODULE, Options, []).

procname(GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_cm'])).

-spec cmtabs(GwName :: gateway_name())
    -> {ChanTab :: atom(),
        ConnTab :: atom(),
        ChannInfoTab :: atom()}.
cmtabs(GwName) ->
    { tabname(chan, GwName)   %% Client Tabname; Record: {ClientId, Pid}
    , tabname(conn, GwName)   %% Client ConnMod; Recrod: {{ClientId, Pid}, ConnMod}
    , tabname(info, GwName)   %% ClientInfo Tabname; Record: {{ClientId, Pid}, ClientInfo, ClientStats}
    }.

tabname(chan, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel']));
tabname(conn, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel_conn']));
tabname(info, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel_info'])).

lockername(GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_locker'])).

-spec register_channel(gateway_name(),
                       emqx_types:clientid(),
                       pid(),
                       emqx_types:conninfo()) -> ok.
register_channel(GwName, ClientId, ChanPid, #{conn_mod := ConnMod}) when is_pid(ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(tabname(chan, GwName), Chan),
    true = ets:insert(tabname(conn, GwName), {Chan, ConnMod}),
    ok = emqx_gateway_cm_registry:register_channel(GwName, Chan),
    cast(procname(GwName), {registered, Chan}).

%% @doc Unregister a channel.
-spec unregister_channel(gateway_name(), emqx_types:clientid()) -> ok.
unregister_channel(GwName, ClientId) when is_binary(ClientId) ->
    true = do_unregister_channel(GwName, {ClientId, self()}, cmtabs(GwName)),
    ok.

%% @doc Insert/Update the channel info and stats
-spec insert_channel_info(gateway_name(),
                          emqx_types:clientid(),
                          emqx_types:infos(),
                          emqx_types:stats()) -> ok.
insert_channel_info(GwName, ClientId, Info, Stats) ->
    Chan = {ClientId, self()},
    true = ets:insert(tabname(info, GwName), {Chan, Info, Stats}),
    %%?tp(debug, insert_channel_info, #{client_id => ClientId}),
    ok.

%% @doc Get info of a channel.
-spec get_chan_info(gateway_name(), emqx_types:clientid())
      -> emqx_types:infos() | undefined.
get_chan_info(GwName, ClientId) ->
    with_channel(GwName, ClientId,
        fun(ChanPid) ->
            get_chan_info(GwName, ClientId, ChanPid)
        end).

-spec get_chan_info(gateway_name(), emqx_types:clientid(), pid())
      -> emqx_types:infos() | undefined.
get_chan_info(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:lookup_element(tabname(info, GwName), Chan, 2)
    catch
        error:badarg -> undefined
    end;
get_chan_info(GwName, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_info, [GwName, ClientId, ChanPid]).

%% @doc Update infos of the channel.
-spec set_chan_info(gateway_name(),
                    emqx_types:clientid(),
                    emqx_types:infos()) -> boolean().
set_chan_info(GwName, ClientId, Infos) ->
    set_chan_info(GwName, ClientId, self(), Infos).

-spec set_chan_info(gateway_name(),
                    emqx_types:clientid(),
                    pid(),
                    emqx_types:infos()) -> boolean().
set_chan_info(GwName, ClientId, ChanPid, Infos) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:update_element(tabname(info, GwName), Chan, {2, Infos})
    catch
        error:badarg -> false
    end;
set_chan_info(GwName, ClientId, ChanPid, Infos) ->
    rpc_call(node(ChanPid), set_chan_info, [GwName, ClientId, ChanPid, Infos]).

%% @doc Get channel's stats.
-spec get_chan_stats(gateway_name(), emqx_types:clientid())
      -> emqx_types:stats() | undefined.
get_chan_stats(GwName, ClientId) ->
    with_channel(GwName, ClientId,
        fun(ChanPid) ->
            get_chan_stats(GwName, ClientId, ChanPid)
        end).

-spec get_chan_stats(gateway_name(), emqx_types:clientid(), pid())
      -> emqx_types:stats() | undefined.
get_chan_stats(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:lookup_element(tabname(info, GwName), Chan, 3)
    catch
        error:badarg -> undefined
    end;
get_chan_stats(GwName, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_stats, [GwName, ClientId, ChanPid]).

-spec set_chan_stats(gateway_name(),
                     emqx_types:clientid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(GwName, ClientId, Stats) ->
    set_chan_stats(GwName, ClientId, self(), Stats).

-spec set_chan_stats(gateway_name(),
                     emqx_types:clientid(),
                     pid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(GwName, ClientId, ChanPid, Stats)  when node(ChanPid) == node() ->
    Chan = {ClientId, self()},
    try ets:update_element(tabname(info, GwName), Chan, {3, Stats})
    catch
        error:badarg -> false
    end;
set_chan_stats(GwName, ClientId, ChanPid, Stats) ->
    rpc_call(node(ChanPid), set_chan_stats, [GwName, ClientId, ChanPid, Stats]).

-spec connection_closed(gateway_name(), emqx_types:clientid()) -> true.
connection_closed(GwName, ClientId) ->
    %% XXX: Why we need to delete conn_mod tab ???
    Chan = {ClientId, self()},
    ets:delete_object(tabname(conn, GwName), Chan).

-spec open_session(GwName :: gateway_name(),
                   CleanStart :: boolean(),
                   ClientInfo :: emqx_types:clientinfo(),
                   ConnInfo :: emqx_types:conninfo(),
                   CreateSessionFun :: fun((emqx_types:clientinfo(),
                                            emqx_types:conninfo()) -> Session
                                       ))
    -> {ok, #{session := Session,
              present := boolean(),
              pendings => list()
          }}
     | {error, any()}.

open_session(GwName, CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    open_session(GwName, CleanStart, ClientInfo, ConnInfo, CreateSessionFun, emqx_session).

open_session(GwName, true = _CleanStart, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    Self = self(),
    ClientId = maps:get(clientid, ClientInfo),
    Fun = fun(_) ->
                  ok = discard_session(GwName, ClientId),
                  Session = create_session(GwName,
                                           ClientInfo,
                                           ConnInfo,
                                           CreateSessionFun,
                                           SessionMod
                                          ),
                  register_channel(GwName, ClientId, Self, ConnInfo),
                  {ok, #{session => Session, present => false}}
          end,
    locker_trans(GwName, ClientId, Fun);

open_session(_Type, false = _CleanStart,
             _ClientInfo, _ConnInfo, _CreateSessionFun, _SessionMod) ->
    %% TODO:
    {error, not_supported_now}.

%% @private
create_session(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    try
        Session = emqx_gateway_utils:apply(
                    CreateSessionFun,
                    [ClientInfo, ConnInfo]
                   ),
        ok = emqx_gateway_metrics:inc(GwName, 'session.created'),
        SessionInfo = case is_tuple(Session)
                           andalso element(1, Session) == session of
                          true -> SessionMod:info(Session);
                          _ ->
                              case is_map(Session) of
                                  false ->
                                      throw(session_structure_should_be_map);
                                  _ ->
                                      Session
                              end
                      end,
        ok = emqx_hooks:run('session.created', [ClientInfo, SessionInfo]),
        Session
    catch
        Class : Reason : Stk ->
            ?SLOG(error, #{ msg => "failed_create_session"
                          , clientid => maps:get(clientid, ClientInfo, undefined)
                          , username => maps:get(username, ClientInfo, undefined)
                          , reason => {Class, Reason}
                          , stacktrace => Stk
                          }),
        throw(Reason)
    end.

%% @doc Discard all the sessions identified by the ClientId.
-spec discard_session(GwName :: gateway_name(), binary()) -> ok.
discard_session(GwName, ClientId) when is_binary(ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] -> ok;
        ChanPids -> lists:foreach(fun(Pid) -> do_discard_session(GwName, ClientId, Pid) end, ChanPids)
    end.

%% @private
do_discard_session(GwName, ClientId, Pid) ->
    try
        discard_session(GwName, ClientId, Pid)
    catch
        _ : noproc -> % emqx_ws_connection: call
            %?tp(debug, "session_already_gone", #{pid => Pid}),
            ok;
        _ : {noproc, _} -> % emqx_connection: gen_server:call
            %?tp(debug, "session_already_gone", #{pid => Pid}),
            ok;
        _ : {{shutdown, _}, _} ->
            %?tp(debug, "session_already_shutdown", #{pid => Pid}),
            ok;
        _ : _Error : _St ->
            %?tp(error, "failed_to_discard_session",
            %    #{pid => Pid, reason => Error, stacktrace=>St})
            ok
    end.

%% @private
discard_session(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined -> ok;
        ConnMod when is_atom(ConnMod) ->
            ConnMod:call(ChanPid, discard, ?T_TAKEOVER)
    end;

%% @private
discard_session(GwName, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), discard_session, [GwName, ClientId, ChanPid]).

-spec kick_session(gateway_name(), emqx_types:clientid())
    -> {error, any()}
     | ok.
kick_session(GwName, ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] -> {error, not_found};
        [ChanPid] ->
            kick_session(GwName, ClientId, ChanPid);
        ChanPids ->
            [ChanPid|StalePids] = lists:reverse(ChanPids),
            ?SLOG(error, #{ msg => "more_than_one_channel_found"
                          , chan_pids => ChanPids
                          }),
            lists:foreach(fun(StalePid) ->
                              catch discard_session(GwName, ClientId, StalePid)
                          end, StalePids),
            kick_session(GwName, ClientId, ChanPid)
    end.

kick_session(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chan_info(GwName, ClientId, ChanPid) of
        #{conninfo := #{conn_mod := ConnMod}} ->
            ConnMod:call(ChanPid, kick, ?T_TAKEOVER);
        undefined ->
            {error, not_found}
    end;

kick_session(GwName, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), kick_session, [GwName, ClientId, ChanPid]).

with_channel(GwName, ClientId, Fun) ->
    case lookup_channels(GwName, ClientId) of
        []    -> undefined;
        [Pid] -> Fun(Pid);
        Pids  -> Fun(lists:last(Pids))
    end.

%% @doc Lookup channels.
-spec(lookup_channels(gateway_name(), emqx_types:clientid()) -> list(pid())).
lookup_channels(GwName, ClientId) ->
    emqx_gateway_cm_registry:lookup_channels(GwName, ClientId).

get_chann_conn_mod(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try [ConnMod] = ets:lookup_element(tabname(conn, GwName), Chan, 2), ConnMod
    catch
        error:badarg -> undefined
    end;
get_chann_conn_mod(GwName, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chann_conn_mod, [GwName, ClientId, ChanPid]).

%% Locker

locker_trans(_Type, undefined, Fun) ->
    Fun([]);
locker_trans(GwName, ClientId, Fun) ->
    Locker = lockername(GwName),
    case locker_lock(Locker, ClientId) of
        {true, Nodes} ->
            try Fun(Nodes) after locker_unlock(Locker, ClientId) end;
        {false, _Nodes} ->
            {error, client_id_unavailable}
    end.

locker_lock(Locker, ClientId) ->
    ekka_locker:acquire(Locker, ClientId, quorum).

locker_unlock(Locker, ClientId) ->
    ekka_locker:release(Locker, ClientId, quorum).

%% @private
rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> error(Reason);
        Res -> Res
    end.

cast(Name, Msg) ->
    gen_server:cast(Name, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(Options) ->
    GwName = proplists:get_value(gwname, Options),

    TabOpts = [public, {write_concurrency, true}],

    {ChanTab, ConnTab, InfoTab} = cmtabs(GwName),
    ok = emqx_tables:new(ChanTab, [bag, {read_concurrency, true}|TabOpts]),
    ok = emqx_tables:new(ConnTab, [bag | TabOpts]),
    ok = emqx_tables:new(InfoTab, [set, compressed | TabOpts]),

    %% Start link cm-registry process
    %% XXX: Should I hang it under a higher level supervisor?
    {ok, Registry} = emqx_gateway_cm_registry:start_link(GwName),

    %% Start locker process
    {ok, Locker} = ekka_locker:start_link(lockername(GwName)),

    %% Interval update stats
    %% TODO: v0.2
    %ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),

    {ok, #state{gwname = GwName,
                locker = Locker,
                registry = Registry,
                chan_pmon = emqx_pmon:new()}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({registered, {ClientId, ChanPid}}, State = #state{chan_pmon = PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, ClientId, PMon),
    {noreply, State#state{chan_pmon = PMon1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{gwname = GwName, chan_pmon = PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(?DEFAULT_BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),

    CmTabs = cmtabs(GwName),
    ok = emqx_pool:async_submit(fun do_unregister_channel_task/3, [Items, GwName, CmTabs]),
    {noreply, State#state{chan_pmon = PMon1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_unregister_channel_task(Items, GwName, CmTabs) ->
    lists:foreach(
      fun({ChanPid, ClientId}) ->
          do_unregister_channel(GwName, {ClientId, ChanPid}, CmTabs)
      end, Items).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_unregister_channel(GwName, Chan, {ChanTab, ConnTab, InfoTab}) ->
    ok = emqx_gateway_cm_registry:unregister_channel(GwName, Chan),
    true = ets:delete(ConnTab, Chan),
    true = ets:delete(InfoTab, Chan),
    ets:delete_object(ChanTab, Chan).
