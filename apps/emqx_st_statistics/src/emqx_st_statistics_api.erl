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

-module(emqx_st_statistics_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_st_statistics/include/emqx_st_statistics.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([slow_topic/2]).

-import(hoconsc, [mk/2, ref/1]).

namespace() -> "slow_topics_statistics".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/slow_topic"].

schema(("/slow_topic")) ->
    #{
      'operationId' => slow_topic,
      delete => #{tags => [<<"slow topics">>],
                  description => <<"Clear current data and re count slow topic">>,
                  parameters => [],
                  'requestBody' => [],
                  responses => #{204 => <<"No Content">>}
                 },
      get => #{tags => [<<"slow topics">>],
               description => <<"Get slow topics statistics record data">>,
               parameters => [ {page, mk(integer(), #{in => query})}
                             , {limit, mk(integer(), #{in => query})}
                             ],
               'requestBody' => [],
               responses => #{200 => [{data, mk(hoconsc:array(ref(record)), #{})}]}
              }
     }.

fields(record) ->
    [
     {rank, mk(integer(), #{desc => <<"the rank of this record">>})},
     {topic, mk(string(), #{})},
     {count, mk(number(), #{desc => <<"average number of occurrences">>})},
     {elapsed, mk(number(), #{desc => <<"average elapsed">>})}
    ].

slow_topic(delete, _) ->
    ok = emqx_st_statistics:clear_history(),
    {204};

slow_topic(get, #{query_string := QS}) ->
    #{<<"page">> := PageT, <<"limit">> := LimitT} = QS,
    Page = erlang:binary_to_integer(PageT),
    Limit = erlang:binary_to_integer(LimitT),
    Start = (Page - 1) * Limit + 1,
    Size = ets:info(?TOPK_TAB, size),
    End = Start + Limit - 1,
    Data = get_history(Start, End, Size),
    {200, #{data => Data}}.

get_history(Start, _End, Size) when Start > Size ->
    [];

get_history(Start, End, Size) when End > Size ->
    get_history(Start, Size, Size);

get_history(Start, End, _Size) ->
    Fold = fun(Rank, Acc) ->
               [#top_k{topic = Topic
                      , average_count = Count
                      , average_elapsed = Elapsed}] = ets:lookup(?TOPK_TAB, Rank),

               Info =[ {rank, Rank}
                     , {topic, Topic}
                     , {count, Count}
                     , {elapsed, Elapsed}],

               [Info | Acc]
           end,
    lists:foldl(Fold, [], lists:seq(Start, End)).
