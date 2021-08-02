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
-export([ load/0
        , unload/0
        ]).

-export([]).

-export([ init/1
        , on_insta_create/3
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

load() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:load(lwm2m, RegistryOptions, []).

unload() ->
    %% XXX:
    lwm2m_coap_server_registry:remove_handler(
      [<<"rd">>],
      emqx_lwm2m_coap_resource, undefined
     ),
    emqx_gateway_registry:unload(lwm2m).

init(_) ->
    %% Handler
    _ = lwm2m_coap_server:start_registry(),
    lwm2m_coap_server_registry:add_handler(
      [<<"rd">>],
      emqx_lwm2m_coap_resource, undefined
     ),
    %% Xml registry
    {ok, _} = emqx_lwm2m_xml_object_db:start_link(
                emqx_config:get([gateway, lwm2m_xml_dir])
              ),

    %% XXX: Self managed table?
    %% TODO: Improve it later
    {ok, _} = emqx_lwm2m_cm:start_link(),

    GwState = #{},
    {ok, GwState}.

%% TODO: deinit

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_insta_create(_Insta = #{ id := InstaId,
                            rawconf := RawConf
                          }, Ctx, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
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
            io:format("Start lwm2m ~s:~s listener on ~s successfully.~n",
                      [InstaId, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to start lwm2m ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(InstaId, udp),
    NCfg = Cfg#{ctx => Ctx},
    NSocketOpts = merge_default(SocketOpts),
    Options = [{config, NCfg}|NSocketOpts],
    case Type of
        udp ->
            lwm2m_coap_server:start_udp(Name, ListenOn, Options);
        dtls ->
            lwm2m_coap_server:start_dtls(Name, ListenOn, Options)
    end.

name(InstaId, Type) ->
    list_to_atom(lists:concat([InstaId, ":", Type])).

merge_default(Options) ->
    Default = emqx_gateway_utils:default_udp_options(),
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{udp_options, Default} | Options]
    end.

stop_listener(InstaId, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(InstaId, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> io:format("Stop lwm2m ~s:~s listener on ~s successfully.~n",
                        [InstaId, Type, ListenOnStr]);
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to stop lwm2m ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]
                     )
    end,
    StopRet.

stop_listener(InstaId, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(InstaId, Type),
    case Type of
        udp ->
            lwm2m_coap_server:stop_udp(Name, ListenOn);
        dtls ->
            lwm2m_coap_server:stop_dtls(Name, ListenOn)
    end.
