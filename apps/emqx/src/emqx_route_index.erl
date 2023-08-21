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
%% This layout implies that we cannot in general make changes concurrently, so there's
%% transactional operation mode for that, employing mria transaction mechanism.

-module(emqx_route_index).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").

-export([init/1]).

-export([match/2]).

-export([
    insert/3,
    insert/4,
    delete/3,
    delete/4,
    clean/1
]).

-export([all/1]).

-record(routeidx, {
    key :: emqx_topic_index:key(nil()),
    dests :: #{emqx_router:dest() => nil()}
}).

%%

-spec init(atom()) -> ok.
init(TabName) ->
    mria:create_table(
        TabName,
        [
            {type, ordered_set},
            {storage, ram_copies},
            {local_content, true},
            {record_name, routeidx},
            {attributes, record_info(fields, routeidx)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}
        ]
    ).

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

-spec insert(emqx_types:route(), ets:table(), _TODO) -> boolean().
insert(#route{topic = Topic, dest = Dest}, Tab, Mode) ->
    insert(Topic, Dest, Tab, Mode).

-spec insert(emqx_types:topic(), _Dest, ets:table(), _TODO) -> boolean().
insert(Topic, Dest, Tab, Mode) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true when Mode == unsafe ->
            mria:async_dirty(mria:local_content_shard(), fun do_insert/3, [Words, Dest, Tab]);
        true when Mode == transactional ->
            mria:transaction(mria:local_content_shard(), fun do_insert/3, [Words, Dest, Tab]);
        false ->
            false
    end.

do_insert(Words, Dest, Tab) ->
    K = emqx_topic_index:mk_key(Words, []),
    case mnesia:wread({Tab, K}) of
        [#routeidx{dests = Ds} = Entry] ->
            NEntry = Entry#routeidx{dests = Ds#{Dest => []}},
            ok = mnesia:write(Tab, NEntry, write),
            true;
        [] ->
            Entry = #routeidx{key = K, dests = #{Dest => []}},
            ok = mnesia:write(Tab, Entry, write),
            true
    end.

-spec delete(emqx_types:route(), ets:table(), _TODO) -> boolean().
delete(#route{topic = Topic, dest = Dest}, Tab, Mode) ->
    delete(Topic, Dest, Tab, Mode).

-spec delete(emqx_types:topic(), _Dest, ets:table(), _TODO) -> boolean().
delete(Topic, Dest, Tab, Mode) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true when Mode == unsafe ->
            mria:async_dirty(mria:local_content_shard(), fun do_delete/3, [Words, Dest, Tab]);
        true when Mode == transactional ->
            mria:transaction(mria:local_content_shard(), fun do_delete/3, [Words, Dest, Tab]);
        false ->
            false
    end.

do_delete(Words, Dest, Tab) ->
    K = emqx_topic_index:mk_key(Words, []),
    case mnesia:wread({Tab, K}) of
        [#routeidx{dests = Ds = #{Dest := _}}] when map_size(Ds) =:= 1 ->
            ok = mnesia:delete(Tab, K, write),
            true;
        [#routeidx{dests = Ds = #{Dest := _}} = Entry] ->
            NEntry = Entry#routeidx{dests = maps:remove(Dest, Ds)},
            ok = mnesia:write(Tab, NEntry, write),
            true;
        [_] ->
            true;
        [] ->
            true
    end.

-spec clean(ets:table()) -> true.
clean(Tab) ->
    mria:clear_table(Tab).

-spec all(ets:table()) -> [emqx_types:route()].
all(Tab) ->
    [
        #route{topic = emqx_topic_index:get_topic(K), dest = Dest}
     || #routeidx{key = K, dests = Ds} <- ets:tab2list(Tab),
        Dest <- maps:keys(Ds)
    ].
