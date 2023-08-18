%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Route topic index
%%
%% We maintain "compacted" index here, this is why index entries has no relevant IDs
%% associated with them. Records are mapsets of route destinations. Basically:
%% ```
%% {<<"t/route/topic/#">>, _ID = []} => #{'node1@emqx' => [], 'node2@emqx' => [], ...}
%% ```
%%
%% This layout implies that we cannot make changes concurrently, since insert and delete
%% are not atomic operations.

-module(emqx_route_index).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").

-export([match/2]).

-export([
    insert/2,
    insert/3,
    delete/2,
    delete/3,
    clean/1
]).

-export([all/1]).

-spec match(emqx_types:topic(), ets:table()) -> [emqx_types:route()].
match(Topic, Tab) ->
    Matches = emqx_topic_index:matches(Topic, Tab, []),
    lists:flatmap(fun(M) -> expand_match(M, Tab) end, Matches).

expand_match(Match, Tab) ->
    Topic = emqx_topic_index:get_topic(Match),
    [
        #route{topic = Topic, dest = Dest}
     || Ds <- emqx_topic_index:get_record(Match, Tab),
        Dest <- maps:keys(Ds)
    ].

-spec insert(emqx_types:route(), ets:table()) -> boolean().
insert(#route{topic = Topic, dest = Dest}, Tab) ->
    insert(Topic, Dest, Tab).

-spec insert(emqx_types:topic(), _Dest, ets:table()) -> boolean().
insert(Topic, Dest, Tab) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            case emqx_topic_index:lookup(Words, ID = [], Tab) of
                [Ds = #{}] ->
                    emqx_topic_index:insert(Words, ID, Ds#{Dest => []}, Tab);
                [] ->
                    emqx_topic_index:insert(Words, ID, #{Dest => []}, Tab)
            end;
        false ->
            false
    end.

-spec delete(emqx_types:route(), ets:table()) -> boolean().
delete(#route{topic = Topic, dest = Dest}, Tab) ->
    delete(Topic, Dest, Tab).

-spec delete(emqx_types:topic(), _Dest, ets:table()) -> boolean().
delete(Topic, Dest, Tab) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            case emqx_topic_index:lookup(Words, ID = [], Tab) of
                [Ds = #{Dest := _}] when map_size(Ds) =:= 1 ->
                    emqx_topic_index:delete(Words, ID, Tab);
                [Ds = #{Dest := _}] ->
                    NDs = maps:remove(Dest, Ds),
                    emqx_topic_index:insert(Words, ID, NDs, Tab);
                [_] ->
                    true;
                [] ->
                    true
            end;
        false ->
            false
    end.

-spec clean(ets:table()) -> true.
clean(Tab) ->
    emqx_topic_index:clean(Tab).

-spec all(ets:table()) -> [emqx_types:route()].
all(Tab) ->
    % NOTE: Useful for testing, assumes particular record layout
    [
        #route{topic = emqx_topic_index:get_topic(M), dest = Dest}
     || {M, Ds} <- ets:tab2list(Tab),
        Dest <- maps:keys(Ds)
    ].
