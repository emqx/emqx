%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds_router).

-include("emqx.hrl").
-include("emqx_persistent_session_ds/emqx_ps_ds_int.hrl").

-export([init_tables/0]).

%% Route APIs
-export([
    do_add_route/2,
    do_delete_route/2,
    has_any_route/1,
    match_routes/1,
    lookup_routes/1,
    foldr_routes/2,
    foldl_routes/2
]).

-export([cleanup_routes/1]).
-export([print_routes/1]).
-export([topics/0]).

-ifdef(TEST).
-export([has_route/2]).
-endif.

-type dest() :: emqx_persistent_session_ds:id().

-export_type([dest/0]).

%%--------------------------------------------------------------------
%% Table Initialization
%%--------------------------------------------------------------------

init_tables() ->
    mria_config:set_dirty_shard(?PS_ROUTER_SHARD, true),
    ok = mria:create_table(?PS_ROUTER_TAB, [
        {type, bag},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_route},
        {attributes, record_info(fields, ps_route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria:create_table(?PS_FILTERS_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_routeidx},
        {attributes, record_info(fields, ps_routeidx)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, auto}
            ]}
        ]}
    ]),
    ok = mria:wait_for_tables([?PS_ROUTER_TAB, ?PS_FILTERS_TAB]),
    ok.

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec do_add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_add_route(Topic, Dest) when is_binary(Topic) ->
    case has_route(Topic, Dest) of
        true ->
            ok;
        false ->
            mria_insert_route(Topic, Dest)
    end.

-spec do_delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_delete_route(Topic, Dest) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria:dirty_delete(?PS_FILTERS_TAB, K);
        false ->
            mria_route_tab_delete(#ps_route{topic = Topic, dest = Dest})
    end.

%% @doc Takes a real topic (not filter) as input, and returns whether there is any
%% matching filters.
-spec has_any_route(emqx_types:topic()) -> boolean().
has_any_route(Topic) ->
    DirectTopicMatch = lookup_route_tab(Topic),
    WildcardMatch = emqx_topic_index:match(Topic, ?PS_FILTERS_TAB),
    case {DirectTopicMatch, WildcardMatch} of
        {[], false} ->
            false;
        {_, _} ->
            true
    end.

%% @doc Take a real topic (not filter) as input, return the matching topics and topic
%% filters associated with route destination.
-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    lookup_route_tab(Topic) ++
        [match_to_route(M) || M <- match_filters(Topic)].

%% @doc Take a topic or filter as input, and return the existing routes with exactly
%% this topic or filter.
-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            Pat = #ps_routeidx{entry = emqx_topic_index:make_key(Topic, '$1')},
            [Dest || [Dest] <- ets:match(?PS_FILTERS_TAB, Pat)];
        false ->
            lookup_route_tab(Topic)
    end.

-spec has_route(emqx_types:topic(), dest()) -> boolean().
has_route(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            ets:member(?PS_FILTERS_TAB, emqx_topic_index:make_key(Topic, Dest));
        false ->
            has_route_tab_entry(Topic, Dest)
    end.

-spec topics() -> list(emqx_types:topic()).
topics() ->
    Pat = #ps_routeidx{entry = '$1'},
    Filters = [emqx_topic_index:get_topic(K) || [K] <- ets:match(?PS_FILTERS_TAB, Pat)],
    list_route_tab_topics() ++ Filters.

%% @doc Print routes to a topic
-spec print_routes(emqx_types:topic()) -> ok.
print_routes(Topic) ->
    lists:foreach(
        fun(#ps_route{topic = To, dest = Dest}) ->
            io:format("~ts -> ~ts~n", [To, Dest])
        end,
        match_routes(Topic)
    ).

-spec cleanup_routes(emqx_persistent_session_ds:id()) -> ok.
cleanup_routes(DSSessionId) ->
    %% NOTE
    %% No point in transaction here because all the operations on filters table are dirty.
    ok = ets:foldl(
        fun(#ps_routeidx{entry = K}, ok) ->
            case get_dest_session_id(emqx_topic_index:get_id(K)) of
                DSSessionId ->
                    mria:dirty_delete(?PS_FILTERS_TAB, K);
                _ ->
                    ok
            end
        end,
        ok,
        ?PS_FILTERS_TAB
    ),
    ok = ets:foldl(
        fun(#ps_route{dest = Dest} = Route, ok) ->
            case get_dest_session_id(Dest) of
                DSSessionId ->
                    mria:dirty_delete_object(?PS_ROUTER_TAB, Route);
                _ ->
                    ok
            end
        end,
        ok,
        ?PS_ROUTER_TAB
    ).

-spec foldl_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldl_routes(FoldFun, AccIn) ->
    fold_routes(foldl, FoldFun, AccIn).

-spec foldr_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldr_routes(FoldFun, AccIn) ->
    fold_routes(foldr, FoldFun, AccIn).

%%--------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------

mria_insert_route(Topic, Dest) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria:dirty_write(?PS_FILTERS_TAB, #ps_routeidx{entry = K});
        false ->
            mria_route_tab_insert(#ps_route{topic = Topic, dest = Dest})
    end.

fold_routes(FunName, FoldFun, AccIn) ->
    FilterFoldFun = mk_filtertab_fold_fun(FoldFun),
    Acc = ets:FunName(FoldFun, AccIn, ?PS_ROUTER_TAB),
    ets:FunName(FilterFoldFun, Acc, ?PS_FILTERS_TAB).

mk_filtertab_fold_fun(FoldFun) ->
    fun(#ps_routeidx{entry = K}, Acc) -> FoldFun(match_to_route(K), Acc) end.

match_filters(Topic) ->
    emqx_topic_index:matches(Topic, ?PS_FILTERS_TAB, []).

get_dest_session_id({_, DSSessionId}) ->
    DSSessionId;
get_dest_session_id(DSSessionId) ->
    DSSessionId.

match_to_route(M) ->
    #ps_route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.

mria_route_tab_insert(Route) ->
    mria:dirty_write(?PS_ROUTER_TAB, Route).

lookup_route_tab(Topic) ->
    ets:lookup(?PS_ROUTER_TAB, Topic).

has_route_tab_entry(Topic, Dest) ->
    [] =/= ets:match(?PS_ROUTER_TAB, #ps_route{topic = Topic, dest = Dest}).

list_route_tab_topics() ->
    mnesia:dirty_all_keys(?PS_ROUTER_TAB).

mria_route_tab_delete(Route) ->
    mria:dirty_delete_object(?PS_ROUTER_TAB, Route).
