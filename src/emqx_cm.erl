%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Channel Manager
-module(emqx_cm).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[CM]").

-export([start_link/0]).

-export([ register_channel/1
        ]).

-export([ get_chan_attrs/1
        , get_chan_attrs/2
        , set_chan_attrs/2
        ]).

-export([ get_chan_stats/1
        , get_chan_stats/2
        , set_chan_stats/2
        ]).

-export([ open_session/3
        , discard_session/1
        , takeover_session/1
        ]).

-export([ lookup_channels/1
        , lookup_channels/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% Internal export
-export([stats_fun/0]).

-type(chan_pid() :: pid()).

%% Tables for channel management.
-define(CHAN_TAB, emqx_channel).
-define(CHAN_P_TAB, emqx_channel_p).
-define(CHAN_ATTRS_TAB, emqx_channel_attrs).
-define(CHAN_STATS_TAB, emqx_channel_stats).

-define(CHAN_STATS,
        [{?CHAN_TAB, 'channels.count', 'channels.max'},
         {?CHAN_TAB, 'connections.count', 'connections.max'},
         {?CHAN_TAB, 'sessions.count', 'sessions.max'},
         {?CHAN_P_TAB, 'sessions.persistent.count', 'sessions.persistent.max'}
        ]).

%% Batch drain
-define(BATCH_SIZE, 100000).

%% Server name
-define(CM, ?MODULE).

%% @doc Start the channel manager.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Register a channel.
%% Channel will be unregistered automatically when the channel process dies
-spec(register_channel(emqx_types:client_id()) -> ok).
register_channel(ClientId) when is_binary(ClientId) ->
    register_channel(ClientId, self()).

%% @doc Register a channel with pid.
-spec(register_channel(emqx_types:client_id(), chan_pid()) -> ok).
register_channel(ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(?CHAN_TAB, Chan),
    ok = emqx_cm_registry:register_channel(Chan),
    cast({registered, Chan}).

%% @private
do_unregister_channel(Chan) ->
    ok = emqx_cm_registry:unregister_channel(Chan),
    true = ets:delete_object(?CHAN_P_TAB, Chan),
    true = ets:delete(?CHAN_ATTRS_TAB, Chan),
    true = ets:delete(?CHAN_STATS_TAB, Chan),
    ets:delete_object(?CHAN_TAB, Chan).

%% @doc Get attrs of a channel.
-spec(get_chan_attrs(emqx_types:client_id()) -> maybe(emqx_types:attrs())).
get_chan_attrs(ClientId) ->
    with_channel(ClientId, fun(ChanPid) -> get_chan_attrs(ClientId, ChanPid) end).

-spec(get_chan_attrs(emqx_types:client_id(), chan_pid()) -> maybe(emqx_types:attrs())).
get_chan_attrs(ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    emqx_tables:lookup_value(?CHAN_ATTRS_TAB, Chan);
get_chan_attrs(ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_attrs, [ClientId, ChanPid]).

%% @doc Set attrs of a channel.
-spec(set_chan_attrs(emqx_types:client_id(), emqx_types:attrs()) -> ok).
set_chan_attrs(ClientId, Attrs) when is_binary(ClientId) ->
    Chan = {ClientId, self()},
    true = ets:insert(?CHAN_ATTRS_TAB, {Chan, Attrs}),
    ok.

%% @doc Get channel's stats.
-spec(get_chan_stats(emqx_types:client_id()) -> maybe(emqx_types:stats())).
get_chan_stats(ClientId) ->
    with_channel(ClientId, fun(ChanPid) -> get_chan_stats(ClientId, ChanPid) end).

-spec(get_chan_stats(emqx_types:client_id(), chan_pid()) -> maybe(emqx_types:stats())).
get_chan_stats(ClientId, ChanPid) when node(ChanPid) == node() ->
    Chan = {ClientId, ChanPid},
    emqx_tables:lookup_value(?CHAN_STATS_TAB, Chan);
get_chan_stats(ClientId, ChanPid) ->
    rpc_call(node(ChanPid), get_chan_stats, [ClientId, ChanPid]).

%% @doc Set channel's stats.
-spec(set_chan_stats(emqx_types:client_id(), emqx_types:stats()) -> ok).
set_chan_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_chan_stats(ClientId, self(), Stats).

-spec(set_chan_stats(emqx_types:client_id(), chan_pid(), emqx_types:stats()) -> ok).
set_chan_stats(ClientId, ChanPid, Stats) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(?CHAN_STATS_TAB, {Chan, Stats}),
    ok.

%% @doc Open a session.
-spec(open_session(boolean(), emqx_types:client(), map())
      -> {ok, emqx_session:session()} | {error, Reason :: term()}).
open_session(true, Client = #{client_id := ClientId}, Options) ->
    CleanStart = fun(_) ->
                     ok = discard_session(ClientId),
                     {ok, emqx_session:init(true, Client, Options), false}
                 end,
    emqx_cm_locker:trans(ClientId, CleanStart);

open_session(false, Client = #{client_id := ClientId}, Options) ->
    ResumeStart = fun(_) ->
                      case takeover_session(ClientId) of
                          {ok, ConnMod, ChanPid, Session} ->
                              {ok, NSession} = emqx_session:resume(ClientId, Session),
                              {ok, Pendings} = ConnMod:takeover(ChanPid, 'end'),
                              io:format("Pending Delivers: ~p~n", [Pendings]),
                              {ok, NSession, true};
                          {error, not_found} ->
                              {ok, emqx_session:init(false, Client, Options), false}
                      end
                  end,
    emqx_cm_locker:trans(ClientId, ResumeStart).

%% @doc Try to takeover a session.
-spec(takeover_session(emqx_types:client_id())
      -> {ok, emqx_session:session()} | {error, Reason :: term()}).
takeover_session(ClientId) ->
    case lookup_channels(ClientId) of
        [] -> {error, not_found};
        [ChanPid] ->
            takeover_session(ClientId, ChanPid);
        ChanPids ->
            [ChanPid|StalePids] = lists:reverse(ChanPids),
            ?LOG(error, "More than one channel found: ~p", [ChanPids]),
            lists:foreach(fun(StalePid) ->
                                  catch discard_session(ClientId, StalePid)
                          end, StalePids),
            takeover_session(ClientId, ChanPid)
    end.

takeover_session(ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chan_attrs(ClientId, ChanPid) of
        #{client := #{conn_mod := ConnMod}} ->
            {ok, Session} = ConnMod:takeover(ChanPid, 'begin'),
            {ok, ConnMod, ChanPid, Session};
        undefined ->
            {error, not_found}
    end;

takeover_session(ClientId, ChanPid) ->
    rpc_call(node(ChanPid), takeover_session, [ClientId, ChanPid]).

%% @doc Discard all the sessions identified by the ClientId.
-spec(discard_session(emqx_types:client_id()) -> ok).
discard_session(ClientId) when is_binary(ClientId) ->
    case lookup_channels(ClientId) of
        [] -> ok;
        ChanPids ->
            lists:foreach(
              fun(ChanPid) ->
                      try
                          discard_session(ClientId, ChanPid)
                      catch
                          _:Error:_Stk ->
                              ?LOG(error, "Failed to discard ~p: ~p", [ChanPid, Error])
                      end
              end, ChanPids)
    end.

discard_session(ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chan_attrs(ClientId, ChanPid) of
        #{conn_mod := ConnMod} ->
            ConnMod:discard(ChanPid);
        undefined -> ok
    end;

discard_session(ClientId, ChanPid) ->
    rpc_call(node(ChanPid), discard_session, [ClientId, ChanPid]).

%% @doc Is clean start?
% is_clean_start(#{clean_start := false}) -> false;
% is_clean_start(_Attrs) -> true.

with_channel(ClientId, Fun) ->
    case lookup_channels(ClientId) of
        []    -> undefined;
        [Pid] -> Fun(Pid);
        Pids  -> Fun(lists:last(Pids))
    end.

%% @doc Lookup channels.
-spec(lookup_channels(emqx_types:client_id()) -> list(chan_pid())).
lookup_channels(ClientId) ->
    lookup_channels(global, ClientId).

%% @doc Lookup local or global channels.
-spec(lookup_channels(local | global, emqx_types:client_id()) -> list(chan_pid())).
lookup_channels(global, ClientId) ->
    case emqx_cm_registry:is_enabled() of
        true ->
            emqx_cm_registry:lookup_channels(ClientId);
        false ->
            lookup_channels(local, ClientId)
    end;

lookup_channels(local, ClientId) ->
    [ChanPid || {_, ChanPid} <- ets:lookup(?CHAN_TAB, ClientId)].

%% @private
rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> error(Reason);
        Res -> Res
    end.

%% @private
cast(Msg) -> gen_server:cast(?CM, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    TabOpts = [public, {write_concurrency, true}],
    ok = emqx_tables:new(?CHAN_TAB, [bag, {read_concurrency, true} | TabOpts]),
    ok = emqx_tables:new(?CHAN_P_TAB, [bag | TabOpts]),
    ok = emqx_tables:new(?CHAN_ATTRS_TAB, [set, compressed | TabOpts]),
    ok = emqx_tables:new(?CHAN_STATS_TAB, [set | TabOpts]),
    ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),
    {ok, #{chan_pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({registered, {ClientId, ChanPid}}, State = #{chan_pmon := PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, ClientId, PMon),
    {noreply, State#{chan_pmon := PMon1}};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{chan_pmon := PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),
    ok = emqx_pool:async_submit(fun lists:foreach/2, [fun clean_down/1, Items]),
    {noreply, State#{chan_pmon := PMon1}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(chan_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

clean_down({ChanPid, ClientId}) ->
    do_unregister_channel({ClientId, ChanPid}).

stats_fun() ->
    lists:foreach(fun update_stats/1, ?CHAN_STATS).

update_stats({Tab, Stat, MaxStat}) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.
