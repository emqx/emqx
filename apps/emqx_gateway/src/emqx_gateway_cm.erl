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

-include("include/emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[PGW-CM]").

%% APIs
-export([start_link/1]).

-export([ open_session/5
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
          type      :: atom(),    %% Gateway Id
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
    Type = proplists:get_value(type, Options),
    gen_server:start_link({local, procname(Type)}, ?MODULE, Options, []).

procname(Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_cm'])).

-spec cmtabs(Type :: atom()) -> {ChanTab :: atom(),
                                 ConnTab :: atom(),
                                 ChannInfoTab :: atom()}.
cmtabs(Type) ->
    { tabname(chan, Type)   %% Client Tabname; Record: {ClientId, Pid}
    , tabname(conn, Type)   %% Client ConnMod; Recrod: {{ClientId, Pid}, ConnMod}
    , tabname(info, Type)   %% ClientInfo Tabname; Record: {{ClientId, Pid}, ClientInfo, ClientStats}
    }.

tabname(chan, Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_channel']));
tabname(conn, Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_channel_conn']));
tabname(info, Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_channel_info'])).

lockername(Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_locker'])).

-spec register_channel(atom(), binary(), pid(), emqx_types:conninfo()) -> ok.
register_channel(Type, ClientId, ChanPid, #{conn_mod := ConnMod}) when is_pid(ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(tabname(chan, Type), Chan),
    true = ets:insert(tabname(conn, Type), {Chan, ConnMod}),
    ok = emqx_gateway_cm_registry:register_channel(Type, Chan),
    cast(procname(Type), {registered, Chan}).

%% @doc Unregister a channel.
-spec unregister_channel(atom(), emqx_types:clientid()) -> ok.
unregister_channel(Type, ClientId) when is_binary(ClientId) ->
    true = do_unregister_channel(Type, {ClientId, self()}, cmtabs(Type)),
    ok.

%% @doc Insert/Update the channel info and stats
-spec insert_channel_info(atom(),
                          emqx_types:clientid(),
                          emqx_types:infos(),
                          emqx_types:stats()) -> ok.
insert_channel_info(Type, ClientId, Info, Stats) ->
    Chan = {ClientId, self()},
    true = ets:insert(tabname(info, Type), {Chan, Info, Stats}),
    %%?tp(debug, insert_channel_info, #{client_id => ClientId}),
    ok.

%% @doc Get info of a channel.
-spec get_chan_info(gateway_type(), emqx_types:clientid())
      -> emqx_types:infos() | undefined.
get_chan_info(Type, ClientId) ->
    with_channel(Type, ClientId,
        fun(ChanPid) ->
            get_chan_info(Type, ClientId, ChanPid)
        end).

-spec get_chan_info(gateway_type(), emqx_types:clientid(), pid())
      -> emqx_types:infos() | undefined.
get_chan_info(Type, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:lookup_element(tabname(info, Type), Chan, 2)
    catch
        error:badarg -> undefined
    end;
get_chan_info(Type, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_info, [Type, ClientId, ChanPid]).

%% @doc Update infos of the channel.
-spec set_chan_info(gateway_type(),
                    emqx_types:clientid(),
                    emqx_types:infos()) -> boolean().
set_chan_info(Type, ClientId, Infos) ->
    set_chan_info(Type, ClientId, self(), Infos).

-spec set_chan_info(gateway_type(),
                    emqx_types:clientid(),
                    pid(),
                    emqx_types:infos()) -> boolean().
set_chan_info(Type, ClientId, ChanPid, Infos) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:update_element(tabname(info, Type), Chan, {2, Infos})
    catch
        error:badarg -> false
    end;
set_chan_info(Type, ClientId, ChanPid, Infos) ->
    rpc_call(node(ChanPid), set_chan_info, [Type, ClientId, ChanPid, Infos]).

%% @doc Get channel's stats.
-spec get_chan_stats(gateway_type(), emqx_types:clientid())
      -> emqx_types:stats() | undefined.
get_chan_stats(Type, ClientId) ->
    with_channel(Type, ClientId,
        fun(ChanPid) ->
            get_chan_stats(Type, ClientId, ChanPid)
        end).

-spec get_chan_stats(gateway_type(), emqx_types:clientid(), pid())
      -> emqx_types:stats() | undefined.
get_chan_stats(Type, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try ets:lookup_element(tabname(info, Type), Chan, 3)
    catch
        error:badarg -> undefined
    end;
get_chan_stats(Type, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_stats, [Type, ClientId, ChanPid]).

-spec set_chan_stats(gateway_type(),
                     emqx_types:clientid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(Type, ClientId, Stats) ->
    set_chan_stats(Type, ClientId, self(), Stats).

-spec set_chan_stats(gateway_type(),
                     emqx_types:clientid(),
                     pid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(Type, ClientId, ChanPid, Stats)  when node(ChanPid) == node() ->
    Chan = {ClientId, self()},
    try ets:update_element(tabname(info, Type), Chan, {3, Stats})
    catch
        error:badarg -> false
    end;
set_chan_stats(Type, ClientId, ChanPid, Stats) ->
    rpc_call(node(ChanPid), set_chan_stats, [Type, ClientId, ChanPid, Stats]).

-spec connection_closed(gateway_type(), emqx_types:clientid()) -> true.
connection_closed(Type, ClientId) ->
    %% XXX: Why we need to delete conn_mod tab ???
    Chan = {ClientId, self()},
    ets:delete_object(tabname(conn, Type), Chan).

-spec open_session(Type :: atom(), CleanStart :: boolean(),
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

open_session(Type, true = _CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    Self = self(),
    ClientId = maps:get(clientid, ClientInfo),
    Fun = fun(_) ->
              ok = discard_session(Type, ClientId),
              Session = create_session(Type,
                                       ClientInfo,
                                       ConnInfo,
                                       CreateSessionFun
                                      ),
              register_channel(Type, ClientId, Self, ConnInfo),
              {ok, #{session => Session, present => false}}
          end,
    locker_trans(Type, ClientId, Fun);

open_session(_Type, false = _CleanStart,
             _ClientInfo, _ConnInfo, _CreateSessionFun) ->
    {error, not_supported_now}.

%% @private
create_session(_Type, ClientInfo, ConnInfo, CreateSessionFun) ->
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
            ?LOG(error, "Failed to create a session: ~p, ~p "
                        "Stacktrace:~0p", [Class, Reason, Stk]),
        throw(Reason)
    end.

%% @doc Discard all the sessions identified by the ClientId.
-spec discard_session(Type :: atom(), binary()) -> ok.
discard_session(Type, ClientId) when is_binary(ClientId) ->
    case lookup_channels(Type, ClientId) of
        [] -> ok;
        ChanPids -> lists:foreach(fun(Pid) -> do_discard_session(Type, ClientId, Pid) end, ChanPids)
    end.

%% @private
do_discard_session(Type, ClientId, Pid) ->
    try
        discard_session(Type, ClientId, Pid)
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
discard_session(Type, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chann_conn_mod(Type, ClientId, ChanPid) of
        undefined -> ok;
        ConnMod when is_atom(ConnMod) ->
            ConnMod:call(ChanPid, discard, ?T_TAKEOVER)
    end;

%% @private
discard_session(Type, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), discard_session, [Type, ClientId, ChanPid]).

-spec kick_session(gateway_type(), emqx_types:clientid())
    -> {error, any()}
     | ok.
kick_session(Type, ClientId) ->
    case lookup_channels(Type, ClientId) of
        [] -> {error, not_found};
        [ChanPid] ->
            kick_session(Type, ClientId, ChanPid);
        ChanPids ->
            [ChanPid|StalePids] = lists:reverse(ChanPids),
            ?LOG(error, "More than one channel found: ~p", [ChanPids]),
            lists:foreach(fun(StalePid) ->
                              catch discard_session(Type, ClientId, StalePid)
                          end, StalePids),
            kick_session(Type, ClientId, ChanPid)
    end.

kick_session(Type, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chan_info(Type, ClientId, ChanPid) of
        #{conninfo := #{conn_mod := ConnMod}} ->
            ConnMod:call(ChanPid, kick, ?T_TAKEOVER);
        undefined ->
            {error, not_found}
    end;

kick_session(Type, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), kick_session, [Type, ClientId, ChanPid]).

with_channel(Type, ClientId, Fun) ->
    case lookup_channels(Type, ClientId) of
        []    -> undefined;
        [Pid] -> Fun(Pid);
        Pids  -> Fun(lists:last(Pids))
    end.

%% @doc Lookup channels.
-spec(lookup_channels(atom(), emqx_types:clientid()) -> list(pid())).
lookup_channels(Type, ClientId) ->
    emqx_gateway_cm_registry:lookup_channels(Type, ClientId).

get_chann_conn_mod(Type, ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    try [ConnMod] = ets:lookup_element(tabname(conn, Type), Chan, 2), ConnMod
    catch
        error:badarg -> undefined
    end;
get_chann_conn_mod(Type, ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chann_conn_mod, [Type, ClientId, ChanPid]).

%% Locker

locker_trans(_Type, undefined, Fun) ->
    Fun([]);
locker_trans(Type, ClientId, Fun) ->
    Locker = lockername(Type),
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
    Type = proplists:get_value(type, Options),

    TabOpts = [public, {write_concurrency, true}],

    {ChanTab, ConnTab, InfoTab} = cmtabs(Type),
    ok = emqx_tables:new(ChanTab, [bag, {read_concurrency, true}|TabOpts]),
    ok = emqx_tables:new(ConnTab, [bag | TabOpts]),
    ok = emqx_tables:new(InfoTab, [set, compressed | TabOpts]),

    %% Start link cm-registry process
    %% XXX: Should I hang it under a higher level supervisor?
    {ok, Registry} = emqx_gateway_cm_registry:start_link(Type),

    %% Start locker process
    {ok, Locker} = ekka_locker:start_link(lockername(Type)),

    %% Interval update stats
    %% TODO: v0.2
    %ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),

    {ok, #state{type = Type,
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
            State = #state{type = Type, chan_pmon = PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(10000)],  %% XXX: Fixed BATCH_SIZE
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),

    CmTabs = cmtabs(Type),
    ok = emqx_pool:async_submit(fun do_unregister_channel_task/3, [Items, Type, CmTabs]),
    {noreply, State#state{chan_pmon = PMon1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_unregister_channel_task(Items, Type, CmTabs) ->
    lists:foreach(
      fun({ChanPid, ClientId}) ->
          do_unregister_channel(Type, {ClientId, ChanPid}, CmTabs)
      end, Items).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_unregister_channel(Type, Chan, {ChanTab, ConnTab, InfoTab}) ->
    ok = emqx_gateway_cm_registry:unregister_channel(Type, Chan),
    true = ets:delete(ConnTab, Chan),
    true = ets:delete(InfoTab, Chan),
    ets:delete_object(ChanTab, Chan).
