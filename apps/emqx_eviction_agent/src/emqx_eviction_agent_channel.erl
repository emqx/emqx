%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT Channel
-module(emqx_eviction_agent_channel).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[Evicted Channel]").

-export([start_link/1,
         start_supervised/1,
         call/2,
         call/3,
         cast/2,
         stop/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-type opts() :: #{conninfo := emqx_types:conninfo(),
                  clientinfo := emqx_types:clientinfo()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_supervised(opts()) -> startlink_ret().
start_supervised(#{clientinfo := #{clientid := ClientId}} = Opts) ->
    RandomId = integer_to_binary(erlang:unique_integer([positive])),
    Id = <<ClientId/binary, "-", RandomId/binary>>,
    ChildSpec = #{id => Id,
                  start => {?MODULE, start_link, [Opts]},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [?MODULE]
                 },
    supervisor:start_child(
        emqx_eviction_agent_conn_sup,
        ChildSpec).

-spec start_link(opts()) -> startlink_ret().
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec cast(pid(), term()) -> ok.
cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

-spec call(pid(), term()) -> term().
call(Pid, Req) ->
    call(Pid, Req, infinity).

-spec call(pid(), term(), timeout()) -> term().
call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% gen_server API
%%--------------------------------------------------------------------

init([#{conninfo := OldConnInfo, clientinfo := #{clientid := ClientId} = OldClientInfo}]) ->
    process_flag(trap_exit, true),
    ClientInfo = clientinfo(OldClientInfo),
    ConnInfo = conninfo(OldConnInfo),
    case open_session(ConnInfo, ClientInfo) of
        {ok, Channel0} ->
            case set_expiry_timer(Channel0) of
                {ok, Channel1} ->
                    ?LOG(
                       info,
                       "Channel initialized for client=~p on node=~p",
                       [ClientId, node()]),
                    {ok, Channel1, hibernate};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(kick, _From, Channel) ->
    {stop, kicked, ok, Channel};

handle_call(discard, _From, Channel) ->
    {stop, discarded, ok, Channel};

handle_call({takeover, 'begin'}, _From, #{session := Session} = Channel) ->
    {reply, Session, Channel#{takeover => true}};

handle_call({takeover, 'end'}, _From, #{session := Session,
                                        clientinfo := #{clientid := ClientId},
                                        pendings := Pendings} = Channel) ->
    ok = emqx_session:takeover(Session),
    %% TODO: Should not drain deliver here (side effect)
    Delivers = emqx_misc:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    ?tp(debug,
        emqx_channel_takeover_end,
        #{clientid => ClientId}),
    {stop, normal, AllPendings, Channel};

handle_call(list_acl_cache, _From, Channel) ->
    {reply, [], Channel};

handle_call({quota, _Policy}, _From, Channel) ->
    {reply, ok, Channel};

handle_call(Req, _From, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, Channel}.

handle_info(Deliver = {deliver, _Topic, _Msg}, Channel) ->
    Delivers = [Deliver | emqx_misc:drain_deliver()],
    {noreply, handle_deliver(Delivers, Channel)};

handle_info(expire_session, Channel) ->
    {stop, expired, Channel};

handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, Channel}.

handle_cast(Msg, Channel) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, Channel}.

terminate(Reason, #{clientinfo := ClientInfo, session := Session} = Channel) ->
    ok = cancel_expiry_timer(Channel),
    emqx_session:terminate(ClientInfo, Reason, Session).

code_change(_OldVsn, Channel, _Extra) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_deliver(Delivers,
               #{takeover := true,
                 pendings := Pendings,
                 session := Session,
                 clientinfo := #{clientid := ClientId} = ClientInfo} = Channel) ->
    %% NOTE: Order is important here. While the takeover is in
    %% progress, the session cannot enqueue messages, since it already
    %% passed on the queue to the new connection in the session state.
    NPendings = lists:append(
                  Pendings,
                  ignore_local(ClientInfo, maybe_nack(Delivers), ClientId, Session)),
    Channel#{pendings => NPendings};

handle_deliver(Delivers,
               #{takeover := false,
                 session := Session,
                 clientinfo := #{clientid := ClientId} = ClientInfo} = Channel) ->
    NSession = emqx_session:enqueue(
                 ClientInfo,
                 ignore_local(ClientInfo, maybe_nack(Delivers), ClientId, Session),
                 Session),
    Channel#{session => NSession}.

cancel_expiry_timer(#{expiry_timer := TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok;
cancel_expiry_timer(_) ->
    ok.

set_expiry_timer(#{conninfo := ConnInfo} = Channel) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?UINT_MAX -> {ok, Channel};
        I when I > 0 ->
            Timer = erlang:send_after(timer:seconds(I), self(), expire_session),
            {ok, Channel#{expiry_timer => Timer}};
        _ ->
            {error, should_be_expired}
    end.

open_session(ConnInfo, #{clientid := ClientId} = ClientInfo) ->
    Channel = channel(ConnInfo, ClientInfo),
    case emqx_cm:open_session(false, ClientInfo, ConnInfo) of
        {ok, #{present := false}} ->
            ?LOG(info, "No session for clientid=~p", [ClientId]),
            {error, no_session};
        {ok, #{session := Session, present := true, pendings := Pendings0}} ->
            ?LOG(info, "Session opened for client=~p on node=~p", [ClientId, node()]),
            Pendings1 = lists:usort(lists:append(Pendings0, emqx_misc:drain_deliver())),
            NSession = emqx_session:enqueue(
                         ClientInfo,
                         ignore_local(
                           ClientInfo,
                           maybe_nack(Pendings1),
                           ClientId,
                           Session),
                         Session),
            NChannel = Channel#{session => NSession},
            ok = emqx_cm:insert_channel_info(ClientId, info(NChannel), []),
            ?LOG(info, "Channel info updated for client=~p on node=~p", [ClientId, node()]),
            {ok, NChannel};
        {error, Reason} = Error ->
            ?LOG(error, "Failed to open session due to ~p", [Reason]),
            Error
    end.

conninfo(OldConnInfo) ->
    DisconnectedAt = maps:get(disconnected_at, OldConnInfo, erlang:system_time(millisecond)),
    ConnInfo0 = maps:with(
                  [socktype,
                   sockname,
                   peername,
                   peercert,
                   clientid,
                   clean_start,
                   receive_maximum,
                   expiry_interval],
                  OldConnInfo),
    ConnInfo0#{
      conn_mod => ?MODULE,
      connected => false,
      disconnected_at => DisconnectedAt
     }.

clientinfo(OldClientInfo) ->
    maps:with(
      [zone,
       protocol,
       peerhost,
       sockport,
       clientid,
       username,
       is_bridge,
       is_superuser,
       mountpoint],
      OldClientInfo).

channel(ConnInfo, ClientInfo) ->
    #{conninfo => ConnInfo,
      clientinfo => ClientInfo,
      expiry_timer => undefined,
      takeover => false,
      resuming => false,
      pendings => []
     }.

info(Channel) ->
    #{conninfo => maps:get(conninfo, Channel, undefined),
      clientinfo => maps:get(clientinfo, Channel, undefined),
      session => maps:get(session, Channel, undefined),
      conn_state => disconnected
     }.

ignore_local(ClientInfo, Delivers, Subscriber, Session) ->
    Subs = emqx_session:info(subscriptions, Session),
    lists:dropwhile(fun({deliver, Topic, #message{from = Publisher} = Msg}) ->
                        case maps:find(Topic, Subs) of
                            {ok, #{nl := 1}} when Subscriber =:= Publisher ->
                                ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, no_local]),
                                ok = emqx_metrics:inc('delivery.dropped'),
                                ok = emqx_metrics:inc('delivery.dropped.no_local'),
                                true;
                            _ ->
                                false
                        end
                    end, Delivers).

maybe_nack(Delivers) ->
    lists:filter(fun not_nacked/1, Delivers).

not_nacked({deliver, _Topic, Msg}) ->
    not (emqx_shared_sub:is_ack_required(Msg)
         andalso (ok == emqx_shared_sub:nack_no_connection(Msg))).
