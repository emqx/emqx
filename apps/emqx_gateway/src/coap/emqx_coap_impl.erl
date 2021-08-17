%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_impl).

-include_lib("emqx_gateway/include/emqx_gateway.hrl").

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
    emqx_gateway_registry:reg(coap, RegistryOptions).

unreg() ->
    emqx_gateway_registry:unreg(coap).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(_Gateway = #{name := GwName,
                             rawconf := RawConf
                            }, Ctx) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    ListenerPids = lists:map(fun(Lis) ->
                                     start_listener(GwName, Ctx, Lis)
                             end, Listeners),

    {ok, ListenerPids,  #{ctx => Ctx}}.

on_gateway_update(NewGateway, OldGateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, NewGateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(OldGateway, GwState),
        on_gateway_load(NewGateway, Ctx)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [GwName, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_gateway_unload(_Gateway = #{ name := GwName,
                                rawconf := RawConf
                              }, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(GwName, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(GwName, Ctx, {Type, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(GwName, Ctx, Type, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            ?ULOG("Start ~s:~s listener on ~s successfully.~n",
                  [GwName, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start ~s:~s listener on ~s: ~0p~n",
                  [GwName, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(GwName, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(GwName, Type),
    NCfg = Cfg#{
                ctx => Ctx,
                frame_mod => emqx_coap_frame,
                chann_mod => emqx_coap_channel
               },
    MFA = {emqx_gateway_conn, start_link, [NCfg]},
    do_start_listener(Type, Name, ListenOn, SocketOpts, MFA).

do_start_listener(udp, Name, ListenOn, SocketOpts, MFA) ->
    esockd:open_udp(Name, ListenOn, SocketOpts, MFA);

do_start_listener(dtls, Name, ListenOn, SocketOpts, MFA) ->
    esockd:open_dtls(Name, ListenOn, SocketOpts, MFA).

name(GwName, Type) ->
    list_to_atom(lists:concat([GwName, ":", Type])).

stop_listener(GwName, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(GwName, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> ?ULOG("Stop ~s:~s listener on ~s successfully.~n",
                        [GwName, Type, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop ~s:~s listener on ~s: ~0p~n",
                  [GwName, Type, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(GwName, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(GwName, Type),
    esockd:close(Name, ListenOn).
