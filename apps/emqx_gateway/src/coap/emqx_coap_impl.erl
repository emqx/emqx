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
-export([ load/0
        , unload/0
        ]).

-export([ init/1
        , on_insta_create/3
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

-include_lib("emqx/include/logger.hrl").

-dialyzer({nowarn_function, [load/0]}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

load() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    Options = [],
    emqx_gateway_registry:load(coap, RegistryOptions, Options).

unload() ->
    emqx_gateway_registry:unload(coap).

init([]) ->
    GwState = #{},
    {ok, GwState}.

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_insta_create(_Insta = #{id := InstaId,
                           rawconf := RawConf
                          }, Ctx, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    ListenerPids = lists:map(fun(Lis) ->
                                     start_listener(InstaId, Ctx, Lis)
                             end, Listeners),

    {ok, ListenerPids,  #{ctx => Ctx}}.

on_insta_update(NewInsta, OldInsta, GwInstaState = #{ctx := Ctx}, GwState) ->
    InstaId = maps:get(id, NewInsta),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_insta_destroy(OldInsta, GwInstaState, GwState),
        on_insta_create(NewInsta, Ctx, GwState)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update coap instance ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [InstaId, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_insta_destroy(_Insta = #{ id := InstaId,
                             rawconf := RawConf
                           },
                  _GwInstaState,
                 _GWState) ->
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
            ?ULOG("Start coap ~s:~s listener on ~s successfully.~n",
                  [InstaId, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start coap ~s:~s listener on ~s: ~0p~n",
                  [InstaId, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(InstaId, Type),
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

name(InstaId, Type) ->
    list_to_atom(lists:concat([InstaId, ":", Type])).

stop_listener(InstaId, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(InstaId, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> ?ULOG("Stop coap ~s:~s listener on ~s successfully.~n",
                        [InstaId, Type, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop coap ~s:~s listener on ~s: ~0p~n",
                  [InstaId, Type, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(InstaId, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(InstaId, Type),
    esockd:close(Name, ListenOn).
