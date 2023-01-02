%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_coap_server).

-include("emqx_lwm2m.hrl").

-export([ start/1
        , stop/1
        ]).

-export([ start_listener/1
        , start_listener/3
        , stop_listener/1
        , stop_listener/2
        ]).

-define(LOG(Level, Format, Args),
    logger:Level("LwM2M: " ++ Format, Args)).

start(Envs) ->
    start_listeners(Envs).

stop(Envs) ->
    stop_listeners(Envs).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listeners(Envs) ->
    lists:foreach(fun start_listener/1, listeners_confs(Envs)).

stop_listeners(Envs) ->
    lists:foreach(fun stop_listener/1, listeners_confs(Envs)).

start_listener({Proto, ListenOn, Opts}) ->
    case start_listener(Proto, ListenOn, Opts) of
        {ok, _Pid} ->
            io:format("Start lwm2m:~s listener on ~s successfully.~n",
                      [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start lwm2m:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason]),
            error(Reason)
    end.

start_listener(udp, ListenOn, Opts) ->
    lwm2m_coap_server:start_udp('lwm2m:udp', ListenOn, Opts);
start_listener(dtls, ListenOn, Opts) ->
    lwm2m_coap_server:start_dtls('lwm2m:dtls', ListenOn, Opts).

stop_listener({Proto, ListenOn, _Opts}) ->
    Ret = stop_listener(Proto, ListenOn),
    case Ret of
        ok -> io:format("Stop lwm2m:~s listener on ~s successfully.~n",
                        [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop lwm2m:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason])
    end,
    Ret.

stop_listener(udp, ListenOn) ->
    lwm2m_coap_server:stop_udp('lwm2m:udp', ListenOn);
stop_listener(dtls, ListenOn) ->
    lwm2m_coap_server:stop_dtls('lwm2m:dtls', ListenOn).

listeners_confs(Envs) ->
    listeners_confs(udp, Envs) ++ listeners_confs(dtls, Envs).

listeners_confs(udp, Envs) ->
    Udps = proplists:get_value(bind_udp, Envs, []),
    Opts = proplists:get_value(options, Envs, []),
    [{udp, Port, [{udp_options, InetOpts ++ Opts}] ++ get_lwm2m_opts(Envs)} || {Port, InetOpts} <- Udps];

listeners_confs(dtls, Envs) ->
    Dtls = proplists:get_value(bind_dtls, Envs, []),
    Opts = proplists:get_value(dtls_opts, Envs, []),
    [{dtls, Port, [{dtls_options, InetOpts ++ Opts}] ++ get_lwm2m_opts(Envs)} || {Port, InetOpts} <- Dtls].

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).

get_lwm2m_opts(Envs) ->
    LifetimeMax = proplists:get_value(lifetime_max, Envs, 315360000),
    LifetimeMin = proplists:get_value(lifetime_min, Envs, 0),
    Mountpoint = proplists:get_value(mountpoint, Envs, ""),
    Sockport = proplists:get_value(port, Envs, 5683),
    AutoObserve = proplists:get_value(auto_observe, Envs, []),
    QmodeTimeWindow = proplists:get_value(qmode_time_window, Envs, []),
    Topics = proplists:get_value(topics, Envs, []),
    PublishCondition = proplists:get_value(update_msg_publish_condition, Envs, contains_object_list),
    [{lifetime_max, LifetimeMax},
     {lifetime_min, LifetimeMin},
     {mountpoint, list_to_binary(Mountpoint)},
     {port, Sockport},
     {auto_observe, AutoObserve},
     {qmode_time_window, QmodeTimeWindow},
     {update_msg_publish_condition, PublishCondition},
     {topics, Topics}].
