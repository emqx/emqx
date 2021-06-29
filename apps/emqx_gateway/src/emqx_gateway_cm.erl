%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The gateway connection management
-module(emqx_gateway_cm).

-behaviour(gen_server).

-logger_header("[PGW-CM]").

%% APIs
-export([start_link/1]).

-export([ open_session/5
        , register_channel/4
        , unregister_channel/2
        , insert_channel_info/4
        , set_chan_info/3
        , set_chan_stats/3
        , connection_closed/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          gwid      :: atom(),    %% Gateway Id
          locker    :: pid(),     %% ClientId Locker for CM
          registry  :: pid(),     %% ClientId Registry server
          chan_pmon :: emqx_pmon:pmon()
         }).

-define(T_TAKEOVER, 15000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% XXX: Options for cm process
start_link(Options) ->
    GwId = proplists:get_value(gwid, Options),
    gen_server:start_link({local, procname(GwId)}, ?MODULE, Options, []).

procname(GwId) ->
    list_to_atom(lists:concat([emqx_gateway_, GwId, '_cm'])).

-spec cmtabs(GwId :: atom()) -> {ChanTab :: atom(),
                                 ConnTab :: atom(),
                                 ChannInfoTab :: atom()}.
cmtabs(GwId) ->
    { tabname(chan, GwId)   %% Client Tabname; Record: {ClientId, Pid}
    , tabname(conn, GwId)   %% Client ConnMod; Recrod: {{ClientId, Pid}, ConnMod}
    , tabname(info, GwId)   %% ClientInfo Tabname; Record: {{ClientId, Pid}, ClientInfo, ClientStats}
    }.

tabname(chan, GwId) ->
    list_to_atom(lists:concat([emqx_gateway_, GwId, '_channel']));
tabname(conn, GwId) ->
    list_to_atom(lists:concat([emqx_gateway_, GwId, '_channel_conn']));
tabname(info, GwId) ->
    list_to_atom(lists:concat([emqx_gateway_, GwId, '_channel_info'])).

lockername(GwId) ->
    list_to_atom(lists:concat([emqx_gateway_, GwId, '_locker'])).

-spec register_channel(atom(), binary(), pid(), emqx_types:conninfo()) -> ok.
register_channel(GwId, ClientId, ChanPid, #{conn_mod := ConnMod}) when is_pid(ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(tabname(chan, GwId), Chan),
    true = ets:insert(tabname(conn, GwId), {Chan, ConnMod}),
    ok = emqx_gateway_cm_registry:register_channel(GwId, Chan),
    cast(procname(GwId), {registered, Chan}).

%% @doc Unregister a channel.
-spec unregister_channel(atom(), emqx_types:clientid()) -> ok.
unregister_channel(GwId, ClientId) when is_binary(ClientId) ->
    true = do_unregister_channel(GwId, {ClientId, self()}, cmtabs(GwId)),
    ok.

%% @doc Insert/Update the channel info and stats
-spec insert_channel_info(atom(),
                          emqx_types:clientid(),
                          emqx_types:infos(),
                          emqx_types:stats()) -> ok.
insert_channel_info(GwId, ClientId, Info, Stats) ->
    Chan = {ClientId, self()},
    true = ets:insert(tabname(info, GwId), {Chan, Info, Stats}),
    %%?tp(debug, insert_channel_info, #{client_id => ClientId}),
    ok.

-spec set_chan_info(gateway_id(),
                    emqx_types:clientid(),
                    emqx_types:infos()) -> boolean()
set_chan_info(GwId, ClientId, Infos) ->
    Chan = {ClientId, self()},
    try ets:update_element(tabname(info, GwId), Chan, {2, Infos})
    catch
        error:badarg -> false
    end.

-spec set_chan_stats(gateway_id(),
                     emqx_types:clientid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(GwId, ClientId, Stats) ->
    Chan = {ClientId, self()},
    try ets:update_element(tabname(info, GwId), Chan, {3, Stats})
    catch
        error:badarg -> false
    end.

-spec connection_closed(gateway_id(), emqx_types:clientid()) -> true.
connection_closed(GwId, ClientId) ->
    %% XXX: ???
    Chan = {ClientId, self()},
    ets:delete_object(tabname(conn,GwId), {ClientId, ChanPid}).

-spec open_session(GwId :: atom(), CleanStart :: boolean(),
                   ClientInfo :: emqx_types:clientinfo(),
                   ConnInfo :: emqx_types:conninfo(),
                   CreateSessionFun :: function())
    -> {ok, #{session := map(),
              present := boolean(),
              pendings => list()
          }}
     | {error, any()}.

open_session(GwId, true = _CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    Self = self(),
    ClientId = maps:get(clientid, ClientInfo),
    Fun = fun(_) ->
              ok = discard_session(GwId, ClientId),
              Session = create_session(GwId,
                                       ClientInfo,
                                       ConnInfo,
                                       CreateSessionFun
                                      ),
              register_channel(GwId, ClientId, Self, ConnInfo),
              {ok, #{session => Session, present => false}}
          end,
    locker_trans(GwId, ClientId, Fun);

open_session(_GwId, false = _CleanStart,
             _ClientInfo, _ConnInfo, _CreateSessionFun) ->
    {error, not_supported_now}.

%% @private
create_session(_GwId, ClientInfo, ConnInfo, CreateSessionFun) ->
    try
        Session = emqx_gateway_utils:apply(
                    CreateSessionFun,
                    [ClientInfo, ConnInfo]
                   ),
        %% TODO: v0.2 session metrics & hooks
        %ok = emqx_metrics:inc('session.created'),
        %ok = emqx_hooks:run('session.created', [ClientInfo, emqx_session:info(Session)]),
        Session
    catch
        Class : Reason : Stk ->
            logger:error("Failed to create a session: ~p, ~p "
                         "Stacktrace:~0p", [Class, Reason, Stk]),
        throw(Reason)
    end.

%% @doc Discard all the sessions identified by the ClientId.
-spec discard_session(GwId :: atom(), binary()) -> ok.
discard_session(GwId, ClientId) when is_binary(ClientId) ->
    case lookup_channels(GwId, ClientId) of
        [] -> ok;
        ChanPids -> lists:foreach(fun(Pid) -> do_discard_session(GwId, ClientId, Pid) end, ChanPids)
    end.

%% @private
do_discard_session(GwId, ClientId, Pid) ->
    try
        discard_session(GwId, ClientId, Pid)
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
discard_session(GwId, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chann_conn_mod(GwId, ClientId, ChanPid) of
        undefined -> ok;
        ConnMod when is_atom(ConnMod) ->
            ConnMod:call(ChanPid, discard, ?T_TAKEOVER)
    end;

%% @private
discard_session(GwId, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), discard_session, [GwId, ClientId, ChanPid]).

%% @doc Lookup channels.
-spec(lookup_channels(atom(), emqx_types:clientid()) -> list(pid())).
lookup_channels(GwId, ClientId) ->
    emqx_gateway_cm_registry:lookup_channels(GwId, ClientId).

get_chann_conn_mod(GwId, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try [ConnMod] = ets:lookup_element(tabname(conn, GwId), Chan, 2), ConnMod
    catch
        error:badarg -> undefined
    end;
get_chann_conn_mod(GwId, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chann_conn_mod, [GwId, ClientId, ChanPid]).

%% Locker

locker_trans(_GwId, undefined, Fun) ->
    Fun([]);
locker_trans(GwId, ClientId, Fun) ->
    Locker = lockername(GwId),
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
    GwId = proplists:get_value(gwid, Options),

    TabOpts = [public, {write_concurrency, true}],

    {ChanTab, ConnTab, InfoTab} = cmtabs(GwId),
    ok = emqx_tables:new(ChanTab, [bag, {read_concurrency, true}|TabOpts]),
    ok = emqx_tables:new(ConnTab, [bag | TabOpts]),
    ok = emqx_tables:new(InfoTab, [set, compressed | TabOpts]),

    %% Start link cm-registry process
    Registry = emqx_gateway_cm_registry:start_link(GwId),

    %% Start locker process
    {ok, Locker} = ekka_locker:start_link(lockername(GwId)),

    %% Interval update stats
    %% TODO: v0.2
    %ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),

    {ok, #state{gwid = GwId,
                locker = Locker,
                registry = Registry,
                chan_pmon = emqx_pmon:new()}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({registered, {ClientId, ChanPid}}, State = #{chan_pmon := PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, ClientId, PMon),
    {noreply, State#state{chan_pmon = PMon1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{gwid = GwId, chan_pmon = PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(10000)],  %% XXX: Fixed BATCH_SIZE
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),

    CmTabs = cmtabs(GwId),
    ok = emqx_pool:async_submit(
           lists:foreach(
             fun({ChanPid, ClientId}) ->
                 do_unregister_channel(GwId, {ClientId, ChanPid}, CmTabs)
             end, Items)
          ),
    {noreply, State#{chan_pmon := PMon1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_unregister_channel(GwId, Chan, {ChanTab, ConnTab, InfoTab}) ->
    ok = emqx_gateway_cm_registry:unregister_channel(GwId, Chan),

    true = ets:delete(ConnTab, Chan),
    true = ets:delete(InfoTab, Chan),
    ets:delete_object(ChanTab, Chan).
