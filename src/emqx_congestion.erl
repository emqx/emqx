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

-module(emqx_congestion).

-export([ maybe_alarm_port_busy/3
        , maybe_alarm_too_many_publish/5
        , cancel_alarms/1
        ]).

-define(ALARM_CONN_CONGEST(Channel, Reason),
        list_to_binary(io_lib:format("mqtt_conn/congested/~s/~s/~s", [emqx_channel:info(clientid, Channel),
         maps:get(username, emqx_channel:info(clientinfo, Channel), <<"undefined">>),
         Reason]))).

-define(ALARM_CONN_INFO_KEYS, [
    socktype, sockname, peername,
    clientid, username, proto_name, proto_ver, connected_at
]).
-define(ALARM_SOCK_STATS_KEYS, [send_pend, recv_cnt, recv_oct, send_cnt, send_oct]).
-define(ALARM_SOCK_OPTS_KEYS, [high_watermark, high_msgq_watermark, sndbuf, recbuf, buffer]).
-define(PROC_INFO_KEYS, [message_queue_len, memory, reductions]).

maybe_alarm_port_busy(Socket, Transport, Channel) ->
    case is_tcp_congested(Socket, Transport) of
        true -> alarm_congestion(Socket, Transport, Channel, port_busy);
        false -> cancel_alarm_congestion(Channel, port_busy)
    end.

maybe_alarm_too_many_publish(Socket, Transport, Channel, PubMsgCount,
        PubMsgCount = MaxBatchSize) when MaxBatchSize > 1 ->
    %% we only alarm it when the process is really "too busy"
    alarm_congestion(Socket, Transport, Channel, too_many_publish);
maybe_alarm_too_many_publish(_Socket, _Transport, Channel, PubMsgCount,
        _MaxBatchSize) when PubMsgCount == 1 ->
    %% but we clear the alarm until it is "idle", to avoid sending
    %% alarms and clears too frequently
    cancel_alarm_congestion(Channel, too_many_publish);
maybe_alarm_too_many_publish(_Socket, _Transport, _Channel, _PubMsgCount,
        _MaxBatchSize) ->
    ok.

cancel_alarms(Channel) ->
    [cancel_alarm_congestion(Channel, Reason)
     || Reason <- [port_busy, too_many_publish]].

alarm_congestion(Socket, Transport, Channel, Reason) ->
    case {is_alarm_sent(Reason), is_alarm_confirmed(Reason)} of
        {false, true} ->
            ok = mark_alarm_sent(Reason),
            do_alarm_congestion(Socket, Transport, Channel, Reason);
        {false, _} ->
            ok = confirm_alarm_again(Reason);
        {true, _} -> ok
    end.

cancel_alarm_congestion(Channel, Reason) ->
    case is_alarm_sent(Reason) of
        true ->
            ok = unmark_alarm_sent(Reason),
            ok = reset_confirm_counter(Reason),
            do_cancel_alarm_congestion(Channel, Reason);
        {false, _} -> ok
    end.

do_alarm_congestion(Socket, Transport, Channel, Reason) ->
    AlarmDetails = tcp_congestion_alarm_details(Socket, Transport, Channel),
    emqx_alarm:activate(?ALARM_CONN_CONGEST(Channel, Reason), AlarmDetails),
    ok.

do_cancel_alarm_congestion(Channel, Reason) ->
    emqx_alarm:deactivate(?ALARM_CONN_CONGEST(Channel, Reason)),
    ok.

is_tcp_congested(Socket, Transport) ->
    case Transport:getstat(Socket, [send_pend]) of
        {ok, [{send_pend, N}]} when N > 0 -> true;
        _ -> false
    end.

-define(ALARM_SENT(REASON), {alarm_sent, REASON}).
is_alarm_sent(Reason) ->
    case erlang:get(?ALARM_SENT(Reason)) of
        true -> true;
        _ -> false
    end.
mark_alarm_sent(Reason) ->
    erlang:put(?ALARM_SENT(Reason), true),
    ok.
unmark_alarm_sent(Reason) ->
    erlang:erase(?ALARM_SENT(Reason)),
    ok.

-define(ALARM_CONFIRMED(REASON), {alarm_confirmed, REASON}).
-define(MAX_ALARM_CONFIRM_COUNT, 3).
is_alarm_confirmed(Reason) ->
    case get_alarm_confirm_counter(Reason) of
        Counter when Counter >= ?MAX_ALARM_CONFIRM_COUNT -> true;
        _ -> false
    end.
confirm_alarm_again(Reason) ->
    erlang:put(?ALARM_CONFIRMED(Reason), get_alarm_confirm_counter(Reason) + 1),
    ok.
get_alarm_confirm_counter(Reason) ->
    case erlang:get(?ALARM_CONFIRMED(Reason)) of
        Count when is_integer(Count) -> Count;
        _ -> 0
    end.
reset_confirm_counter(Reason) ->
    erlang:erase(?ALARM_CONFIRMED(Reason)).

tcp_congestion_alarm_details(Socket, Transport, Channel) ->
    {ok, Stat} = Transport:getstat(Socket, ?ALARM_SOCK_STATS_KEYS),
    {ok, Opts} = Transport:getopts(Socket, ?ALARM_SOCK_OPTS_KEYS),
    SockInfo = maps:from_list(Stat ++ Opts),
    ConnInfo = maps:from_list([conn_info(Key, Channel) || Key <- ?ALARM_CONN_INFO_KEYS]),
    BasicInfo = maps:from_list(process_info(self(), ?PROC_INFO_KEYS)),
    maps:merge(BasicInfo, maps:merge(ConnInfo, SockInfo)).

conn_info(Key, Channel) when Key =:= sockname; Key =:= peername ->
    {IPStr, Port} = emqx_channel:info(Key, Channel),
    {Key, iolist_to_binary([inet:ntoa(IPStr),":",integer_to_list(Port)])};
conn_info(Key, Channel) ->
    {Key, emqx_channel:info(Key, Channel)}.
