%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_channel).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProto Channel]").

-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_deliver/2
        , handle_timeout/3
        , handle_call/2
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% Driver name
          driver :: atom(),
          %% Conn info
          conninfo :: emqx_types:conninfo(),
          %% Client info from `register` function
          clientinfo :: maybe(map()),
          %% Registered
          registered = false :: boolean(),
          %% Connection state
          conn_state :: conn_state(),
          %% Subscription
          subscriptions = #{},
          %% Driver level state
          state :: any()
         }).

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-type(reply() :: {outgoing, binary()}
               | {outgoing, [binary()]}
               | {close, Reason :: atom()}).

-type(replies() :: emqx_types:packet() | reply() | [reply()]).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

-define(SESSION_STATS_KEYS,
        [subscriptions_cnt,
         subscriptions_max,
         inflight_cnt,
         inflight_max,
         mqueue_len,
         mqueue_max,
         mqueue_dropped,
         next_pkt_id,
         awaiting_rel_cnt,
         awaiting_rel_max
        ]).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec(info(channel()) -> emqx_types:infos()).
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec(info(list(atom())|atom(), channel()) -> term()).
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{subscriptions = Subs,
                       conninfo = ConnInfo}) ->
    #{subscriptions => Subs,
      upgrade_qos => false,
      retry_interval => 0,
      await_rel_timeout => 0,
      created_at => maps:get(connected_at, ConnInfo)};
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(will_msg, _) ->
    undefined.

-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{subscriptions = Subs}) ->
    [{subscriptions_cnt, maps:size(Subs)},
     {subscriptions_max, 0},
     {inflight_cnt, 0},
     {inflight_max, 0},
     {mqueue_len, 0},
     {mqueue_max, 0},
     {mqueue_dropped, 0},
     {next_pkt_id, 0},
     {awaiting_rel_cnt, 0},
     {awaiting_rel_max, 0}].

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_exproto_types:conninfo(), proplists:proplist()) -> channel()).
init(ConnInfo, Options) ->
    Driver = proplists:get_value(driver, Options),
    case cb_init(ConnInfo, Driver) of
            {ok, DState} ->
                NConnInfo = default_conninfo(ConnInfo),
                ClientInfo = default_clientinfo(ConnInfo),
                #channel{driver = Driver,
                         state = DState,
                         conninfo = NConnInfo,
                         clientinfo = ClientInfo,
                         conn_state = connected};
            {error, Reason} ->
                exit({init_channel_failed, Reason})
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(binary(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_in(Data, Channel) ->
    case cb_received(Data, Channel) of
        {ok, NChannel} ->
            {ok, NChannel};
        {error, Reason} ->
            {shutdown, Reason, Channel}
    end.

-spec(handle_deliver(list(emqx_types:deliver()), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_deliver(Delivers, Channel) ->
    %% TODO: ?? Nack delivers from shared subscriptions
    case cb_deliver(Delivers, Channel) of
        {ok, NChannel} ->
            {ok, NChannel};
        {error, Reason} ->
            {shutdown, Reason, Channel}
    end.

-spec(handle_timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_timeout(_TRef, Msg, Channel) ->
    ?WARN("Unexpected timeout: ~p", [Msg]),
    {ok, Channel}.

-spec(handle_call(any(), channel())
     -> {reply, Reply :: term(), channel()}
      | {shutdown, Reason :: term(), Reply :: term(), channel()}).
handle_call(kick, Channel) ->
    {shutdown, kicked, ok, Channel};

handle_call(Req, Channel) ->
    ?WARN("Unexpected call: ~p", [Req]),
    {reply, ok, Channel}.

-spec(handle_cast(any(), channel())
     -> {ok, channel()}
      | {ok, replies(), channel()}
      | {shutdown, Reason :: term(), channel()}).
handle_cast({send, Data}, Channel) ->
    {ok, [{outgoing, Data}], Channel};

handle_cast(close, Channel) ->
    {ok, [{close, normal}], Channel};

handle_cast({register, ClientInfo}, Channel = #channel{registered = true}) ->
    ?WARN("Duplicated register command, dropped ~p", [ClientInfo]),
    {ok, Channel};
handle_cast({register, ClientInfo0}, Channel = #channel{conninfo = ConnInfo,
                                                        clientinfo = ClientInfo}) ->
    ClientInfo1 = maybe_assign_clientid(ClientInfo0),
    NConnInfo = enrich_conninfo(ClientInfo1, ConnInfo),
    NClientInfo = enrich_clientinfo(ClientInfo1, ClientInfo),
    case emqx_cm:open_session(true, NClientInfo, NConnInfo) of
        {ok, _Session} ->
            NChannel = Channel#channel{registered = true,
                                       conninfo = NConnInfo,
                                       clientinfo = NClientInfo},
            {ok, [{event, registered}], NChannel};
        {error, Reason} ->
            ?ERROR("Register failed, reason: ~p", [Reason]),
            {shutdown, Reason, {error, Reason}, Channel}
    end;

handle_cast({subscribe, TopicFilter, Qos}, Channel) ->
    do_subscribe([{TopicFilter, #{qos => Qos}}], Channel);

handle_cast({unsubscribe, TopicFilter}, Channel) ->
    do_unsubscribe([{TopicFilter, #{}}], Channel);

handle_cast({publish, Msg}, Channel) ->
    emqx:publish(enrich_msg(Msg, Channel)),
    {ok, Channel};

handle_cast(Req, Channel) ->
    ?WARN("Unexpected call: ~p", [Req]),
    {ok, Channel}.

-spec(handle_info(any(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_info({subscribe, TopicFilters}, Channel) ->
    do_subscribe(TopicFilters, Channel);

handle_info({unsubscribe, TopicFilters}, Channel) ->
    do_unsubscribe(TopicFilters, Channel);

handle_info({sock_closed, Reason}, Channel) ->
    {shutdown, {sock_closed, Reason}, Channel};
handle_info(Info, Channel) ->
    ?WARN("Unexpected info: ~p", [Info]),
    {ok, Channel}.

-spec(terminate(any(), channel()) -> ok).
terminate(Reason, Channel) ->
    cb_terminated(Reason, Channel), ok.

%%--------------------------------------------------------------------
%% Sub/UnSub
%%--------------------------------------------------------------------

do_subscribe(TopicFilters, Channel) ->
    NChannel = lists:foldl(
        fun({TopicFilter, SubOpts}, ChannelAcc) ->
            do_subscribe(TopicFilter, SubOpts, ChannelAcc)
        end, Channel, parse_topic_filters(TopicFilters)),
    {ok, NChannel}.

%% @private
do_subscribe(TopicFilter, SubOpts, Channel =
             #channel{clientinfo = ClientInfo = #{mountpoint := Mountpoint},
                      subscriptions = Subs}) ->
    NTopicFilter = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    NSubOpts = maps:merge(?DEFAULT_SUBOPTS, SubOpts),
    SubId = maps:get(clientid, ClientInfo, undefined),
    _ = emqx:subscribe(NTopicFilter, SubId, NSubOpts),
    Channel#channel{subscriptions = Subs#{NTopicFilter => SubOpts}}.

do_unsubscribe(TopicFilters, Channel) ->
    NChannel = lists:foldl(
        fun({TopicFilter, SubOpts}, ChannelAcc) ->
            do_unsubscribe(TopicFilter, SubOpts, ChannelAcc)
        end, Channel, parse_topic_filters(TopicFilters)),
    {ok, NChannel}.

%% @private
do_unsubscribe(TopicFilter, _SubOpts, Channel =
               #channel{clientinfo = #{mountpoint := Mountpoint},
                        subscriptions = Subs}) ->
    TopicFilter1 = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    _ = emqx:unsubscribe(TopicFilter1),
    Channel#channel{subscriptions = maps:remove(TopicFilter1, Subs)}.

%% @private
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Cbs for driver
%%--------------------------------------------------------------------

cb_init(ConnInfo, Driver) ->
    Args = [self(), emqx_exproto_types:serialize(conninfo, ConnInfo)],
    emqx_exproto_driver_mngr:call(Driver, {'init', Args}).

cb_received(Data, Channel = #channel{state = DState}) ->
    Args = [self(), Data, DState],
    do_call_cb('received', Args, Channel).

cb_terminated(Reason, Channel = #channel{state = DState}) ->
    Args = [self(), stringfy(Reason), DState],
    do_call_cb('terminated', Args, Channel).

cb_deliver(Delivers, Channel = #channel{state = DState}) ->
    Msgs = [emqx_exproto_types:serialize(message, Msg) || {_, _, Msg} <- Delivers],
    Args = [self(), Msgs, DState],
    do_call_cb('deliver', Args, Channel).

%% @private
do_call_cb(Fun, Args, Channel = #channel{driver = D}) ->
    case emqx_exproto_driver_mngr:call(D, {Fun, Args}) of
        ok ->
            {ok, Channel};
        {ok, NDState} ->
            {ok, Channel#channel{state = NDState}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Format
%%--------------------------------------------------------------------

maybe_assign_clientid(ClientInfo) ->
    case maps:get(clientid, ClientInfo, undefined) of
        undefined ->
            ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())};
        _ ->
            ClientInfo
    end.

enrich_msg(Msg, #channel{clientinfo = ClientInfo = #{mountpoint := Mountpoint}}) ->
    NMsg = emqx_mountpoint:mount(Mountpoint, Msg),
    case maps:get(clientid, ClientInfo, undefined) of
        undefined -> NMsg;
        ClientId -> NMsg#message{from = ClientId}
    end.

enrich_conninfo(InClientInfo, ConnInfo) ->
    maps:merge(ConnInfo, maps:with([proto_name, proto_ver, clientid, username, keepalive], InClientInfo)).

enrich_clientinfo(InClientInfo = #{proto_name := ProtoName}, ClientInfo) ->
    NClientInfo = maps:merge(ClientInfo, maps:with([clientid, username, mountpoint], InClientInfo)),
    NClientInfo#{protocol => lowcase_atom(ProtoName)}.

default_conninfo(ConnInfo) ->
    ConnInfo#{proto_name => undefined,
              proto_ver => undefined,
              clean_start => true,
              clientid => undefined,
              username => undefined,
              conn_props => [],
              connected => true,
              connected_at => erlang:system_time(millisecond),
              keepalive => undefined,
              receive_maximum => 0,
              expiry_interval => 0}.

default_clientinfo(#{peername := {PeerHost, _},
                      sockname := {_, SockPort}}) ->
    #{zone         => undefined,
      protocol     => undefined,
      peerhost     => PeerHost,
      sockport     => SockPort,
      clientid     => default_clientid(),
      username     => undefined,
      is_bridge    => false,
      is_superuser => false,
      mountpoint   => undefined}.

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

lowcase_atom(undefined) ->
    undefined;
lowcase_atom(S) ->
    binary_to_atom(string:lowercase(S), utf8).

default_clientid() ->
    <<"exproto_client_", (list_to_binary(pid_to_list(self())))/binary>>.
