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
        , supervisor_ret/1
        , find_sup_child/2
        ]).

-export([ apply/2
        ]).

-export([ normalize_rawconf/1
        ]).

-spec childspec(supervisor:worker(), Mod :: atom()) -> supervisor:child_spec().
childspec(Type, Mod) ->
    childspec(Type, Mod, []).

-spec childspec(supervisor:worker(), Mod :: atom(), Args :: list())
    -> supervisor:child_spec().
childspec(Type, Mod, Args) ->
    #{ id => Mod
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

-spec normalize_rawconf(map()) -> list(map()).
normalize_rawconf(RawConf = #{listeners := Ls}) ->
    NRawConf = maps:remove(listeners, RawConf),
    lists:reverse(lists:foldl(fun(Listener, Acc) ->
        L1 = esockd:parse_opt(Listener),
       [|Acc] 
    end, [], Ls)).

