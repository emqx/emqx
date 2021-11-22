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

-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-import(minirest, [return/1]).

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

clear_history(_Bindings, _Params) ->
    ok = emqx_slow_subs:clear_history(),
    return(ok).

get_history(_Bindings, Params) ->
    PageT = proplists:get_value(<<"_page">>, Params),
    LimitT = proplists:get_value(<<"_limit">>, Params),
    Page = erlang:binary_to_integer(PageT),
    Limit = erlang:binary_to_integer(LimitT),
    Start = (Page - 1) * Limit + 1,
    %%    Size = ets:info(?TOPK_TAB, size),
    Size = 0,
    End = Start + Limit - 1,
    {HasNext, Count, Infos} = get_history(Start, End, Size),
    return({ok, #{meta => #{page => Page,
                            limit => Limit,
                            hasnext => HasNext,
                            count => Count},
                  data => Infos}}).


get_history(Start, _End, Size) when Start > Size ->
    {false, 0, []};

get_history(Start, End, Size) when End > Size ->
    get_history(Start, Size, Size);

get_history(Start, End, Size) ->
    Fold = fun(_Rank, Acc) ->
               %% [#top_k{topic = Topic
               %%        , average_count = Count
               %%        , average_elapsed = Elapsed}] = ets:lookup(?TOPK_TAB, Rank),

               %% Info = [ {rank, Rank}
               %%        , {topic, Topic}
               %%        , {count, Count}
               %%        , {elapsed, Elapsed}],

               %% [Info | Acc]
               Acc
           end,
    Infos = lists:foldl(Fold, [], lists:seq(Start, End)),
    {End < Size, End - Start + 1, Infos}.
