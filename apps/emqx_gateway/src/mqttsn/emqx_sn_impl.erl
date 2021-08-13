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
-export([ reg/0
        , unreg/0
        ]).

-export([ on_gateway_load/2
        , on_gateway_update/3
        , on_gateway_unload/2
        ]).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

reg() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:reg(mqttsn, RegistryOptions).

unreg() ->
    emqx_gateway_registry:unreg(mqttsn).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(_Gateway = #{ type := GwType,
                              rawconf := RawConf
                            }, Ctx) ->

    %% We Also need to start `emqx_sn_broadcast` &
    %% `emqx_sn_registry` process
    SnGwId = maps:get(gateway_id, RawConf),
    case maps:get(broadcast, RawConf) of
        false ->
            ok;
        true ->
            %% FIXME:
            Port = 1884,
            _ = emqx_sn_broadcast:start_link(SnGwId, Port), ok
    end,

    PredefTopics = maps:get(predefined, RawConf),
    {ok, RegistrySvr} = emqx_sn_registry:start_link(GwType, PredefTopics),

    NRawConf = maps:without(
                 [broadcast, predefined],
                 RawConf#{registry => emqx_sn_registry:lookup_name(RegistrySvr)}
                ),
    Listeners = emqx_gateway_utils:normalize_rawconf(NRawConf),

    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(GwType, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _InstaState = #{ctx => Ctx}}.

on_gateway_update(NewGateway = #{type := GwType}, OldGateway,
                  GwState = #{ctx := Ctx}) ->
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(OldGateway, GwState),
        on_gateway_load(NewGateway, Ctx)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [GwType, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_gateway_unload(_Insta = #{ type := GwType,
                              rawconf := RawConf
                            }, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(GwType, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(GwType, Ctx, {Type, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(GwType, Ctx, Type, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            ?ULOG("Start ~s:~s listener on ~s successfully.~n",
                  [GwType, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start ~s:~s listener on ~s: ~0p~n",
                  [GwType, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(GwType, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(GwType, Type),
    NCfg = Cfg#{
             ctx => Ctx,
             frame_mod => emqx_sn_frame,
             chann_mod => emqx_sn_channel
            },
    esockd:open_udp(Name, ListenOn, merge_default(SocketOpts),
                    {emqx_gateway_conn, start_link, [NCfg]}).

name(GwType, Type) ->
    list_to_atom(lists:concat([GwType, ":", Type])).

merge_default(Options) ->
    Default = emqx_gateway_utils:default_udp_options(),
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{udp_options, Default} | Options]
    end.

stop_listener(GwType, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(GwType, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> ?ULOG("Stop ~s:~s listener on ~s successfully.~n",
                    [GwType, Type, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop ~s:~s listener on ~s: ~0p~n",
                  [GwType, Type, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(GwType, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(GwType, Type),
    esockd:close(Name, ListenOn).
