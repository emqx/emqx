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

%% @doc The MQTT-SN Gateway Implement interface
-module(emqx_sn_impl).

-behavior(emqx_gateway_impl).

%% APIs
-export([ load/0
        , unload/0
        ]).

-export([]).

-export([ init/1
        , on_insta_create/3
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

-define(UDP_SOCKOPTS, []).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

load() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    YourOptions = [params1, params2],
    emqx_gateway_registry:load(mqttsn, RegistryOptions, YourOptions).

unload() ->
    emqx_gateway_registry:unload(mqttsn).

init(_) ->
    GwState = #{},
    {ok, GwState}.

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_insta_create(_Insta = #{ id := InstaId,
                            rawconf := RawConf
                          }, Ctx, _GwState) ->

    %% We Also need to start `emqx_sn_broadcast` &
    %% `emqx_sn_registry` process
    SnGwId = maps:get(gateway_id, RawConf),
    case maps:get(broadcast, RawConf) of
        false ->
            ok;
        true ->
            %% FIXME:
            Port = 1884,
            _ = emqx_sn_broadcast:start_link(SnGwId, Port)
    end,

    PredefTopics = maps:get(predefined, RawConf),
    {ok, RegistrySvr} = emqx_sn_registry:start_link(PredefTopics),

    NRawConf = maps:without(
                 [gateway_id, broadcast, predefined],
                 RawConf#{registry => RegistrySvr}
                ),
    Listeners = emqx_gateway_utils:normalize_rawconf(NRawConf),

    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(InstaId, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _InstaState = #{ctx => Ctx}}.

on_insta_update(NewInsta, OldInsta, GwInstaState = #{ctx := Ctx}, GwState) ->
    InstaId = maps:get(id, NewInsta),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_insta_destroy(OldInsta, GwInstaState, GwState),
        on_insta_create(NewInsta, Ctx, GwState)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update stomp instance ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [InstaId, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_insta_destroy(_Insta = #{ id := InstaId,
                             rawconf := RawConf
                           }, _GwInstaState, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(InstaId, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(InstaId, Ctx, {Type, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            io:format("Start mqttsn ~s:~s listener on ~s successfully.~n",
                      [InstaId, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to start mqttsn ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(InstaId, Type),
    esockd:open_udp(Name, ListenOn, merge_default(SocketOpts),
                    {emqx_sn_gateway, start_link, [Cfg#{ctx => Ctx}]}).

name(InstaId, Type) ->
    list_to_atom(lists:concat([InstaId, ":", Type])).

merge_default(Options) ->
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(?UDP_SOCKOPTS, TcpOpts)} | Options1];
        false ->
            [{udp_options, ?UDP_SOCKOPTS} | Options]
    end.

stop_listener(InstaId, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(InstaId, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> io:format("Stop mqttsn ~s:~s listener on ~s successfully.~n",
                        [InstaId, Type, ListenOnStr]);
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to stop mqttsn ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]
                     )
    end,
    StopRet.

stop_listener(InstaId, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(InstaId, Type),
    esockd:close(Name, ListenOn).
