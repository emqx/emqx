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

-module(emqx_slow_subs_api).

-rest_api(#{name   => clear_history,
            method => 'DELETE',
            path   => "/slow_topic",
            func   => clear_history,
            descr  => "Clear current data and re count slow topic"}).

-rest_api(#{name   => get_history,
            method => 'GET',
            path   => "/slow_topic",
            func   => get_history,
            descr  => "Get slow topics statistics record data"}).

-export([ clear_history/2
        , get_history/2
        ]).

-include("include/emqx_slow_subs.hrl").

-import(minirest, [return/1]).

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

clear_history(_Bindings, _Params) ->
    ok = emqx_slow_subs:clear_history(),
    return(ok).

get_history(_Bindings, Params) ->
    RowFun = fun(#top_k{index = ?INDEX(Elapsed, ClientId),
                        type = Type,
                        timestamp = Ts}) ->
                 [{clientid, ClientId},
                  {elapsed, Elapsed},
                  {type, Type},
                  {timestamp, Ts}]
             end,
    Return = emqx_mgmt_api:paginate({?TOPK_TAB, [{traverse, last_prev}]}, Params, RowFun),
    return({ok, Return}).
