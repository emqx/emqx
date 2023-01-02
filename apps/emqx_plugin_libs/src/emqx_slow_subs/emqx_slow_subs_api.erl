%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_api).

-rest_api(#{name   => clear_history,
            method => 'DELETE',
            path   => "/slow_subscriptions",
            func   => clear_history,
            descr  => "Clear current data and re count slow topic"}).

-rest_api(#{name   => get_history,
            method => 'GET',
            path   => "/slow_subscriptions",
            func   => get_history,
            descr  => "Get slow topics statistics record data"}).

-export([ clear_history/2
        , get_history/2
        , get_history/0
        ]).

-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-define(DEFAULT_RPC_TIMEOUT, timer:seconds(5)).

-import(minirest, [return/1]).

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

clear_history(_Bindings, _Params) ->
    Nodes = ekka_mnesia:running_nodes(),
    _ = [rpc_call(Node, emqx_slow_subs, clear_history, [], ok, ?DEFAULT_RPC_TIMEOUT)
         || Node <- Nodes],
    return(ok).

get_history(_Bindings, _Params) ->
    execute_when_enabled(fun do_get_history/0).

get_history() ->
    Node = node(),
    RankL = ets:tab2list(?TOPK_TAB),
    ConvFun = fun(#top_k{index = ?TOPK_INDEX(TimeSpan, ?ID(ClientId, Topic)),
                         last_update_time = LastUpdateTime
                        }) ->
                      #{ clientid => ClientId
                       , node => Node
                       , topic => Topic
                       , timespan => TimeSpan
                       , last_update_time => LastUpdateTime
                       }
              end,

    lists:map(ConvFun, RankL).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
do_get_history() ->
    Nodes = ekka_mnesia:running_nodes(),
    Fun = fun(Node, Acc) ->
                  NodeRankL = rpc_call(Node,
                                       ?MODULE,
                                       get_history,
                                       [],
                                       [],
                                       ?DEFAULT_RPC_TIMEOUT),
                  NodeRankL ++ Acc
          end,

    RankL = lists:foldl(Fun, [], Nodes),

    SortFun = fun(#{timespan := A}, #{timespan := B}) ->
                      A > B
              end,

    SortedL = lists:sort(SortFun, RankL),
    SortedL2 = lists:sublist(SortedL, ?MAX_SIZE),

    return({ok, SortedL2}).

rpc_call(Node, M, F, A, _ErrorR, _T) when Node =:= node() ->
    erlang:apply(M, F, A);

rpc_call(Node, M, F, A, ErrorR, T) ->
    case rpc:call(Node, M, F, A, T) of
        {badrpc, _} -> ErrorR;
        Res -> Res
    end.

-ifdef(EMQX_ENTERPRISE).
execute_when_enabled(Fun) ->
    Fun().
-else.
%% this code from emqx_mod_api_topics_metrics:execute_when_enabled
execute_when_enabled(Fun) ->
    case emqx_modules:find_module(emqx_mod_slow_subs) of
        [{_, true}] -> Fun();
        _ -> return({error, module_not_loaded})
    end.
-endif.
