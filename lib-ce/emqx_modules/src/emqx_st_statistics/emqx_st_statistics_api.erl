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

-include("include/emqx_st_statistics.hrl").

-import(minirest, [return/1]).

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

clear_history(_Bindings, _Params) ->
    ok = emqx_mod_st_statistics:clear_history(),
    return(ok).

get_history(_Bindings, Params) ->
    PageT = proplists:get_value(<<"_page">>, Params),
    LimitT = proplists:get_value(<<"_limit">>, Params),
    Page = erlang:binary_to_integer(PageT),
    Limit = erlang:binary_to_integer(LimitT),
    Start = (Page - 1) * Limit + 1,
    Size = ets:info(?TOPK_TAB, size),
    EndT = Start + Limit - 1,
    End = erlang:min(EndT, Size),
    Infos = lists:foldl(fun(Rank, Acc) ->
                                [#top_k{topic = Topic
                                       , average_count = Count
                                       , average_elapsed = Elapsed}] = ets:lookup(?TOPK_TAB, Rank),

                                Info =[ {rank, Rank}
                                      , {topic, Topic}
                                      , {count, Count}
                                      , {elapsed, Elapsed}],

                                [Info | Acc]
                        end,
                        [],
                        lists:seq(Start, End)),

    return({ok, #{meta => #{page => Page,
                            limit => Limit,
                            hasnext => End < Size,
                            count => End - Start + 1},
                  data => Infos}}).
