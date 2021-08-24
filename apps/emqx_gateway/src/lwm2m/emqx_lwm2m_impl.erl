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

%% @doc The LwM2M Gateway Implement interface
-module(emqx_lwm2m_impl).

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
    emqx_gateway_registry:reg(lwm2m, RegistryOptions).

unreg() ->
   emqx_gateway_registry:unreg(lwm2m).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(_Gateway = #{ name := GwName,
                              rawconf := RawConf
                            }, Ctx) ->

    %% Handler
    _ = lwm2m_coap_server:start_registry(),
    lwm2m_coap_server_registry:add_handler(
      [<<"rd">>],
      emqx_lwm2m_coap_resource, undefined
     ),
    %% Xml registry
    {ok, _} = emqx_lwm2m_xml_object_db:start_link(maps:get(xml_dir, RawConf)),

    %% XXX: Self managed table?
    %% TODO: Improve it later
    {ok, _} = emqx_lwm2m_cm:start_link(),

    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(GwName, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _GwState = #{ctx => Ctx}}.

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
    %% XXX:
    lwm2m_coap_server_registry:remove_handler(
      [<<"rd">>],
      emqx_lwm2m_coap_resource, undefined
     ),

    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(GwName, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(GwName, Ctx, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            ?ULOG("Start ~s:~s:~s listener on ~s successfully.~n",
                  [GwName, Type, LisName, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start ~s:~s:~s listener on ~s: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) ->
    Name = name(GwName, LisName, udp),
    NCfg = Cfg#{ctx => Ctx},
    NSocketOpts = merge_default(SocketOpts),
    Options = [{config, NCfg}|NSocketOpts],
    case Type of
        udp ->
            lwm2m_coap_server:start_udp(Name, ListenOn, Options);
        dtls ->
            lwm2m_coap_server:start_dtls(Name, ListenOn, Options)
    end.

name(GwName, LisName, Type) ->
    list_to_atom(lists:concat([GwName, ":", LisName, ":", Type])).

merge_default(Options) ->
    Default = emqx_gateway_utils:default_udp_options(),
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{udp_options, Default} | Options]
    end.

stop_listener(GwName, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(GwName, Type, LisName, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> ?ULOG("Stop ~s:~s:~s listener on ~s successfully.~n",
                    [GwName, Type, LisName, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop ~s:~s:~s listener on ~s: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(GwName, Type, LisName, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(GwName, LisName, Type),
    case Type of
        udp ->
            lwm2m_coap_server:stop_udp(Name, ListenOn);
        dtls ->
            lwm2m_coap_server:stop_dtls(Name, ListenOn)
    end.
