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

%% @doc Utils funcs for emqx-gateway
-module(emqx_gateway_utils).

-export([ childspec/2
        , childspec/3
        , childspec/4
        , supervisor_ret/1
        , find_sup_child/2
        ]).

-export([ apply/2
        ]).

-export([ normalize_rawconf/1
        ]).

%% Common Envs
-export([ active_n/1
        , ratelimit/1
        , frame_options/1
        , init_gc_state/1
        , stats_timer/1
        , idle_timeout/1
        , oom_policy/1
        ]).

-define(ACTIVE_N, 100).
-define(DEFAULT_IDLE_TIMEOUT, 30000).

-spec childspec(supervisor:worker(), Mod :: atom())
    -> supervisor:child_spec().
childspec(Type, Mod) ->
    childspec(Mod, Type, Mod, []).

-spec childspec(supervisor:worker(), Mod :: atom(), Args :: list())
    -> supervisor:child_spec().
childspec(Type, Mod, Args) ->
    childspec(Mod, Type, Mod, Args).

-spec childspec(atom(), supervisor:worker(), Mod :: atom(), Args :: list())
    -> supervisor:child_spec().
childspec(Id, Type, Mod, Args) ->
    #{ id => Id
     , start => {Mod, start_link, Args}
     , type => Type
     }.

-spec supervisor_ret(supervisor:startchild_ret())
    -> {ok, pid()}
     | {error, supervisor:startchild_err()}.
supervisor_ret({ok, Pid, _Info}) -> {ok, Pid};
supervisor_ret(Ret) -> Ret.

-spec find_sup_child(SupPid :: pid(), ChildId :: supervisor:child_id())
    -> false
     | {ok, pid()}.
find_sup_child(SupPid, ChildId) ->
    case lists:keyfind(ChildId, 1, supervisor:which_children(SupPid)) of
        false -> false;
        {_Id, Pid, _Type, _Mods} -> {ok, Pid}
    end.

apply({M, F, A}, A2) when is_atom(M),
                          is_atom(M),
                          is_list(A),
                          is_list(A2) ->
    erlang:apply(M, F, A ++ A2);
apply({F, A}, A2) when is_function(F),
                       is_list(A),
                       is_list(A2) ->
    erlang:apply(F, A ++ A2);
apply(F, A2) when is_function(F),
                  is_list(A2) ->
    erlang:apply(F, A2).

-type listener() :: #{}.

-type rawconf() ::
        #{ clientinfo_override => #{}
         , authenticators      := #{}
         , listeners           => listener()
         , atom()              => any()
         }.

-spec normalize_rawconf(rawconf())
    -> list({ Type :: udp | tcp | ssl | dtls
            , ListenOn :: esockd:listen_on()
            , SocketOpts :: esockd:option()
            , Cfg :: map()
            }).
normalize_rawconf(RawConf = #{listeners := Liss}) ->
    Cfg0 = maps:without([listeners], RawConf),
    lists:foldr(fun(Lis, Acc) ->
        Type       = maps:get(type, Lis),
        ListenOn   = maps:get(listen_on, Lis),
        SocketOpts = esockd:parse_opt(maps:to_list(Lis)),
        RemainCfgs = maps:without(
                       [type, listen_on] ++ proplists:get_keys(SocketOpts),
                       Lis
                      ),
        Cfg = maps:merge(Cfg0, RemainCfgs),
        [{Type, ListenOn, SocketOpts, Cfg}|Acc]
    end, [], Liss).

%%--------------------------------------------------------------------
%% Envs

active_n(Options) ->
    maps:get(
      active_n,
      maps:get(listener, Options, #{active_n => ?ACTIVE_N}),
      ?ACTIVE_N
     ).

-spec idle_timeout(map()) -> pos_integer().
idle_timeout(Options) ->
    maps:get(idle_timeout, Options, ?DEFAULT_IDLE_TIMEOUT).

-spec ratelimit(map()) -> esockd_rate_limit:config() | undefined.
ratelimit(Options) ->
    maps:get(ratelimit, Options).

-spec frame_options(map()) -> map().
frame_options(Options) ->
    maps:get(frame, Options, #{}).

-spec init_gc_state(map()) -> emqx_gc:gc_state() | undefined.
init_gc_state(Options) ->
    emqx_misc:maybe_apply(fun emqx_gc:init/1, force_gc_policy(Options)).

-spec force_gc_policy(map()) -> emqx_gc:opts() | undefined.
force_gc_policy(Options) ->
    maps:get(force_gc_policy, Options).

-spec oom_policy(map()) -> emqx_types:oom_policy().
oom_policy(Options) ->
    maps:get(force_shutdown_policy, Options).

-spec stats_timer(map()) -> undefined | disabled.
stats_timer(Options) ->
    case enable_stats(Options) of true -> undefined; false -> disabled end.

-spec enable_stats(map()) -> boolean().
enable_stats(Options) ->
    maps:get(enable_stats, Options, true).
