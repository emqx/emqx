%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_congestion).

-export([
    maybe_alarm_conn_congestion/2,
    cancel_alarms/2
]).

-elvis([{elvis_style, invalid_dynamic_call, #{ignore => [emqx_congestion]}}]).

-define(ALARM_CONN_INFO_KEYS, [
    socktype,
    sockname,
    peername,
    clientid,
    username,
    proto_name,
    proto_ver,
    connected_at,
    conn_state
]).
-define(ALARM_SOCK_STATS_KEYS, [send_pend, recv_cnt, recv_oct, send_cnt, send_oct]).
-define(ALARM_SOCK_OPTS_KEYS, [high_watermark, high_msgq_watermark, sndbuf, recbuf, buffer]).
-define(PROC_INFO_KEYS, [message_queue_len, memory, reductions]).
-define(ALARM_SENT(REASON), {alarm_sent, REASON}).
-define(ALL_ALARM_REASONS, [conn_congestion]).

maybe_alarm_conn_congestion(ConnMod, State) ->
    Zone = ConnMod:info({channel, zone}, State),
    Opts = emqx_config:get_zone_conf(Zone, [conn_congestion]),
    case Opts of
        #{enable_alarm := true} ->
            case is_tcp_congested(ConnMod, State) of
                true -> alarm_congestion(ConnMod, State, conn_congestion);
                false -> cancel_alarm_congestion(ConnMod, State, conn_congestion, Opts)
            end;
        #{enable_alarm := false} ->
            ok
    end.

cancel_alarms(ConnMod, State) ->
    lists:foreach(
        fun(Reason) ->
            case has_alarm_sent(Reason) of
                true -> do_cancel_alarm_congestion(ConnMod, State, Reason);
                false -> ok
            end
        end,
        ?ALL_ALARM_REASONS
    ).

alarm_congestion(ConnMod, State, Reason) ->
    case has_alarm_sent(Reason) of
        false ->
            do_alarm_congestion(ConnMod, State, Reason);
        true ->
            %% pretend we have sent an alarm again
            update_alarm_sent_at(Reason)
    end.

cancel_alarm_congestion(ConnMod, State, Reason, Opts) ->
    #{min_alarm_sustain_duration := WontClearIn} = Opts,
    case has_alarm_sent(Reason) andalso long_time_since_last_alarm(Reason, WontClearIn) of
        true -> do_cancel_alarm_congestion(ConnMod, State, Reason);
        false -> ok
    end.

do_alarm_congestion(ConnMod, State, Reason) ->
    ok = update_alarm_sent_at(Reason),
    Name = tcp_congestion_alarm_name(Reason, ConnMod, State),
    Details = tcp_congestion_alarm_details(ConnMod, State),
    Message = io_lib:format("connection congested: ~0p", [Details]),
    emqx_alarm:activate(Name, Details, Message),
    ok.

do_cancel_alarm_congestion(ConnMod, State, Reason) ->
    ok = remove_alarm_sent_at(Reason),
    Name = tcp_congestion_alarm_name(Reason, ConnMod, State),
    Details = tcp_congestion_alarm_details(ConnMod, State),
    Message = io_lib:format("connection congested: ~0p", [Details]),
    emqx_alarm:ensure_deactivated(Name, Details, Message),
    ok.

is_tcp_congested(ConnMod, State) ->
    case ConnMod:sockstats([send_pend], State) of
        [{send_pend, N}] ->
            N > 0;
        _ ->
            false
    end.

has_alarm_sent(Reason) ->
    case get_alarm_sent_at(Reason) of
        0 -> false;
        _ -> true
    end.

update_alarm_sent_at(Reason) ->
    erlang:put(?ALARM_SENT(Reason), timenow()),
    ok.

remove_alarm_sent_at(Reason) ->
    erlang:erase(?ALARM_SENT(Reason)),
    ok.

get_alarm_sent_at(Reason) ->
    case erlang:get(?ALARM_SENT(Reason)) of
        undefined -> 0;
        LastSentAt -> LastSentAt
    end.

long_time_since_last_alarm(Reason, WontClearIn) ->
    %% only sent clears when the alarm was not triggered in the last
    %% WontClearIn time
    case timenow() - get_alarm_sent_at(Reason) of
        Elapse when Elapse >= WontClearIn -> true;
        _ -> false
    end.

timenow() ->
    erlang:system_time(millisecond).

%%==============================================================================
%% Alarm message
%%==============================================================================

tcp_congestion_alarm_name(Reason, ConnMod, State) ->
    ClientId = ConnMod:info({channel, clientid}, State),
    ClientInfo = ConnMod:info({channel, clientinfo}, State),
    emqx_utils:format("~ts/~ts/~ts", [
        Reason,
        ClientId,
        maps:get(username, ClientInfo, <<"unknown_user">>)
    ]).

tcp_congestion_alarm_details(ConnMod, State) ->
    ProcInfo = process_info(self(), ?PROC_INFO_KEYS),
    BasicInfo = [{pid, list_to_binary(pid_to_list(self()))} | ProcInfo],
    Stat = ConnMod:sockstats(?ALARM_SOCK_STATS_KEYS, State),
    Opts = ConnMod:sockopts(?ALARM_SOCK_OPTS_KEYS, State),
    ConnInfo = [conn_info(Key, ConnMod, State) || Key <- ?ALARM_CONN_INFO_KEYS],
    maps:from_list(BasicInfo ++ ConnInfo ++ Stat ++ Opts).

conn_info(Key, ConnMod, State) when Key =:= sockname; Key =:= peername ->
    {Addr, Port} = ConnMod:info(Key, State),
    {Key, iolist_to_binary([inet:ntoa(Addr), ":", integer_to_binary(Port)])};
conn_info(Key, ConnMod, State) ->
    {Key, ConnMod:info({channel, Key}, State)}.
