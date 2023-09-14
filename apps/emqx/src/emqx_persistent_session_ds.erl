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

-module(emqx_persistent_session_ds).

-include("emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/0]).

-export([
    persist_message/1,
    open_session/1,
    add_subscription/2,
    del_subscription/2
]).

-export([
    serialize_message/1,
    deserialize_message/1
]).

%% RPC
-export([
    ensure_iterator_closed_on_all_shards/1,
    ensure_all_iterators_closed/1
]).
-export([
    do_open_iterator/3,
    do_ensure_iterator_closed/1,
    do_ensure_all_iterators_closed/1
]).

%% FIXME
-define(DS_SHARD, <<"local">>).
-define(DEFAULT_KEYSPACE, <<"#">>).

-define(WHEN_ENABLED(DO),
    case is_store_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

%%

init() ->
    ?WHEN_ENABLED(begin
        ok = emqx_ds:ensure_shard(
            ?DS_SHARD,
            ?DEFAULT_KEYSPACE,
            #{
                dir => filename:join([emqx:data_dir(), ds, messages, ?DS_SHARD])
            }
        ),
        ok = emqx_persistent_session_ds_router:init_tables(),
        ok
    end).

%%

-spec persist_message(emqx_types:message()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
persist_message(Msg) ->
    ?WHEN_ENABLED(
        case needs_persistence(Msg) andalso has_subscribers(Msg) of
            true ->
                store_message(Msg);
            false ->
                {skipped, needs_no_persistence}
        end
    ).

needs_persistence(Msg) ->
    not (emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg)).

store_message(Msg) ->
    ID = emqx_message:id(Msg),
    Timestamp = emqx_guid:timestamp(ID),
    Topic = emqx_topic:words(emqx_message:topic(Msg)),
    emqx_ds_storage_layer:store(?DS_SHARD, ID, Timestamp, Topic, serialize_message(Msg)).

has_subscribers(#message{topic = Topic}) ->
    emqx_persistent_session_ds_router:has_any_route(Topic).

open_session(ClientID) ->
    ?WHEN_ENABLED(emqx_ds:session_open(ClientID)).

-spec add_subscription(emqx_types:topic(), emqx_ds:session_id()) ->
    {ok, emqx_ds:iterator_id(), IsNew :: boolean()} | {skipped, disabled}.
add_subscription(TopicFilterBin, DSSessionID) ->
    ?WHEN_ENABLED(
        begin
            %% N.B.: we chose to update the router before adding the subscription to the
            %% session/iterator table.  The reasoning for this is as follows:
            %%
            %% Messages matching this topic filter should start to be persisted as soon as
            %% possible to avoid missing messages.  If this is the first such persistent
            %% session subscription, it's important to do so early on.
            %%
            %% This could, in turn, lead to some inconsistency: if such a route gets
            %% created but the session/iterator data fails to be updated accordingly, we
            %% have a dangling route.  To remove such dangling routes, we may have a
            %% periodic GC process that removes routes that do not have a matching
            %% persistent subscription.  Also, route operations use dirty mnesia
            %% operations, which inherently have room for inconsistencies.
            %%
            %% In practice, we use the iterator reference table as a source of truth,
            %% since it is guarded by a transaction context: we consider a subscription
            %% operation to be successful if it ended up changing this table.  Both router
            %% and iterator information can be reconstructed from this table, if needed.
            ok = emqx_persistent_session_ds_router:do_add_route(TopicFilterBin, DSSessionID),
            TopicFilter = emqx_topic:words(TopicFilterBin),
            {ok, IteratorID, StartMS, IsNew} = emqx_ds:session_add_iterator(
                DSSessionID, TopicFilter
            ),
            Ctx = #{
                iterator_id => IteratorID,
                start_time => StartMS,
                is_new => IsNew
            },
            ?tp(persistent_session_ds_iterator_added, Ctx),
            ?tp_span(
                persistent_session_ds_open_iterators,
                Ctx,
                ok = open_iterator_on_all_shards(TopicFilter, StartMS, IteratorID)
            ),
            {ok, IteratorID, IsNew}
        end
    ).

-spec open_iterator_on_all_shards(emqx_topic:words(), emqx_ds:time(), emqx_ds:iterator_id()) -> ok.
open_iterator_on_all_shards(TopicFilter, StartMS, IteratorID) ->
    ?tp(persistent_session_ds_will_open_iterators, #{
        iterator_id => IteratorID,
        start_time => StartMS
    }),
    %% Note: currently, shards map 1:1 to nodes, but this will change in the future.
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:open_iterator(
        Nodes, TopicFilter, StartMS, IteratorID
    ),
    %% TODO: handle errors
    true = lists:all(fun(Res) -> Res =:= {ok, ok} end, Results),
    ok.

%% RPC target.
-spec do_open_iterator(emqx_topic:words(), emqx_ds:time(), emqx_ds:iterator_id()) -> ok.
do_open_iterator(TopicFilter, StartMS, IteratorID) ->
    Replay = {TopicFilter, StartMS},
    {ok, _It} = emqx_ds_storage_layer:ensure_iterator(?DS_SHARD, IteratorID, Replay),
    ok.

-spec del_subscription(emqx_types:topic(), emqx_ds:session_id()) ->
    ok | {skipped, disabled}.
del_subscription(TopicFilterBin, DSSessionID) ->
    ?WHEN_ENABLED(
        begin
            %% N.B.: see comments in `?MODULE:add_subscription' for a discussion about the
            %% order of operations here.
            TopicFilter = emqx_topic:words(TopicFilterBin),
            case emqx_ds:session_get_iterator_id(DSSessionID, TopicFilter) of
                {error, not_found} ->
                    %% already gone
                    ok;
                {ok, IteratorID} ->
                    ?tp_span(
                        persistent_session_ds_close_iterators,
                        #{iterator_id => IteratorID},
                        ok = ensure_iterator_closed_on_all_shards(IteratorID)
                    )
            end,
            ?tp_span(
                persistent_session_ds_iterator_delete,
                #{},
                emqx_ds:session_del_iterator(DSSessionID, TopicFilter)
            ),
            ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilterBin, DSSessionID),
            ok
        end
    ).

-spec ensure_iterator_closed_on_all_shards(emqx_ds:iterator_id()) -> ok.
ensure_iterator_closed_on_all_shards(IteratorID) ->
    %% Note: currently, shards map 1:1 to nodes, but this will change in the future.
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:close_iterator(Nodes, IteratorID),
    %% TODO: handle errors
    true = lists:all(fun(Res) -> Res =:= {ok, ok} end, Results),
    ok.

%% RPC target.
-spec do_ensure_iterator_closed(emqx_ds:iterator_id()) -> ok.
do_ensure_iterator_closed(IteratorID) ->
    ok = emqx_ds_storage_layer:discard_iterator(?DS_SHARD, IteratorID),
    ok.

-spec ensure_all_iterators_closed(emqx_ds:session_id()) -> ok.
ensure_all_iterators_closed(DSSessionID) ->
    %% Note: currently, shards map 1:1 to nodes, but this will change in the future.
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:close_all_iterators(Nodes, DSSessionID),
    %% TODO: handle errors
    true = lists:all(fun(Res) -> Res =:= {ok, ok} end, Results),
    ok.

%% RPC target.
-spec do_ensure_all_iterators_closed(emqx_ds:session_id()) -> ok.
do_ensure_all_iterators_closed(DSSessionID) ->
    ok = emqx_ds_storage_layer:discard_iterator_prefix(?DS_SHARD, DSSessionID),
    ok.

%%

serialize_message(Msg) ->
    term_to_binary(emqx_message:to_map(Msg)).

deserialize_message(Bin) ->
    emqx_message:from_map(binary_to_term(Bin)).

%%

is_store_enabled() ->
    emqx_config:get([persistent_session_store, ds]).
