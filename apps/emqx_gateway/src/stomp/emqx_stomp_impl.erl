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

-module(emqx_stomp_impl).

-behavior(emqx_gateway_impl).

%% APIs
-export([ reg/0
        , unreg/0
        ]).

-export([ on_gateway_load/2
        , on_gateway_update/3
        , on_gateway_unload/2
        ]).

-include_lib("emqx_gateway/include/emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec reg() -> ok | {error, any()}.
reg() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:reg(stomp, RegistryOptions).

-spec unreg() -> ok | {error, any()}.
unreg() ->
    emqx_gateway_registry:unreg(stomp).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(_Gateway = #{ name := GwName,
                              rawconf := RawConf
                            }, Ctx) ->
    %% Step1. Fold the rawconfs to listeners
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    %% Step2. Start listeners or escokd:specs
    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(GwName, Ctx, Lis)
                   end, Listeners),
    %% FIXME: How to throw an exception to interrupt the restart logic ?
    %% FIXME: Assign ctx to GwState
    {ok, ListenerPids, _GwState = #{ctx => Ctx}}.

on_gateway_update(NewGateway, OldGateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, NewGateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old state???
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
    Name = name(GwName, LisName, Type),
    NCfg = Cfg#{
             ctx => Ctx,
             frame_mod => emqx_stomp_frame,
             chann_mod => emqx_stomp_channel
            },
    esockd:open(Name, ListenOn, merge_default(SocketOpts),
                {emqx_gateway_conn, start_link, [NCfg]}).

name(GwName, LisName, Type) ->
    list_to_atom(lists:concat([GwName, ":", LisName, ":", Type])).

merge_default(Options) ->
    Default = emqx_gateway_utils:default_tcp_options(),
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{tcp_options, Default} | Options]
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
    esockd:close(Name, ListenOn).
