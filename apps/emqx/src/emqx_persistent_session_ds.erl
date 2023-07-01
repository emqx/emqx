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

-include_lib("emqx/include/emqx.hrl").

-export([init/0]).

-export([persist_message/1]).

-export([
    lookup_session/1,
    persist_session/1,
    discard_session/1
]).

-export([
    register_subscription/2,
    unregister_subscription/2
]).

-type session_id() :: emqx_types:clientid().

%% FIXME
-define(DS_SHARD, <<"local">>).

-define(TAB_SESSION_ROUTER, emqx_session_router_ds).

-define(WHEN_ENABLED(DO),
    case is_store_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

%%

init() ->
    ?WHEN_ENABLED(
        begin
            ok = create_router_table(disc_copies),
            ok = emqx_trie:create_session_trie(disc),
            {ok, _ShardPid} = emqx_ds_storage_layer_sup:start_shard(?DS_SHARD),
            ok
        end
    ).

%%

-spec persist_message(emqx_types:message()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
persist_message(Msg) ->
    ?WHEN_ENABLED(
        case needs_persistence(Msg) andalso find_subscribers(Msg) of
            [_ | _] ->
                store_message(Msg);
            [] ->
                {skipped, no_subscribers};
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
    emqx_ds_storage_layer:store(?DS_SHARD, ID, Timestamp, Topic, Msg).

%%

-spec lookup_session(session_id()) ->
    none.
lookup_session(_SessionID) ->
    % TODO
    none.

-spec persist_session(emqx_session:session()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
persist_session(Session) ->
    ?WHEN_ENABLED(
        case emqx_ds_session:open(get_session_id(Session)) of
            {true, _, []} ->
                ok;
            {false, _, _Iterators} ->
                {skipped, already_persisted}
        end
    ).

-spec discard_session(emqx_session:session()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
discard_session(SessionID) when is_binary(SessionID) ->
    % TODO: transaction
    ?WHEN_ENABLED(
        case emqx_ds_session:find(SessionID) of
            {ok, _, Iterators} ->
                discard_session(SessionID, Iterators, true);
            {error, session_not_found} ->
                {skipped, session_not_found}
        end
    );
discard_session(Session) ->
    ?WHEN_ENABLED(
        discard_session(
            get_session_id(Session),
            emqx_session:info(subscriptions, Session),
            emqx_session:info(is_persistent, Session)
        )
    ).

-spec register_subscription(emqx_types:topic(), emqx_session:session()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
register_subscription(TopicFilter, Session) ->
    ?WHEN_ENABLED(
        register_subscription(
            get_session_id(Session),
            TopicFilter,
            emqx_session:info(is_persistent, Session)
        )
    ).

-spec unregister_subscription(emqx_types:topic(), emqx_session:session()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
unregister_subscription(TopicFilter, Session) ->
    ?WHEN_ENABLED(
        unregister_subscription(
            get_session_id(Session),
            TopicFilter,
            emqx_session:info(is_persistent, Session)
        )
    ).

register_subscription(SessionID, TopicFilter, true) ->
    % TODO: error handling
    ok = add_route(TopicFilter, SessionID),
    emqx_ds_session:add_iterator(SessionID, TopicFilter);
register_subscription(_SessionID, _TopicFilter, false) ->
    {skipped, needs_no_persistence}.

unregister_subscription(SessionID, TopicFilter, true) ->
    % TODO: error handling
    ok = delete_route(TopicFilter, SessionID),
    emqx_ds_session:del_iterator(SessionID, TopicFilter);
unregister_subscription(_SessionID, _TopicFilter, false) ->
    ok.

discard_session(SessionID, Subscriptions, true) ->
    ok = delete_routes(Subscriptions, SessionID),
    emqx_ds_session:drop(SessionID);
discard_session(_SessionID, _Subscriptions, false) ->
    ok.

find_subscribers(Msg) ->
    match_routes(emqx_message:topic(Msg)).

get_session_id(Session) ->
    % TODO naming
    emqx_session:info(clientid, Session).

%%

is_store_enabled() ->
    emqx_config:get([persistent_session_store, enabled]).

%% ----------------------------------------------------------------------------
%% emqx_session_router
%% Part of the module functions that aren't really reusable. Eventually, this
%% should probably become a separate module, maybe even part of `emqx_ds` app.
%% ----------------------------------------------------------------------------

create_router_table(Storage) ->
    ok = mria:create_table(?TAB_SESSION_ROUTER, [
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, Storage},
        {record_name, route},
        {attributes, record_info(fields, route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

-spec add_route(emqx_types:topic(), session_id()) -> ok | {error, term()}.
add_route(Topic, SessionID) when is_binary(Topic) ->
    Route = #route{topic = Topic, dest = SessionID},
    case lists:member(Route, lookup_routes(Topic)) of
        true ->
            ok;
        false ->
            case emqx_topic:wildcard(Topic) of
                true ->
                    % TODO: transaction over `emqx_ds_session` shard / table
                    % TODO: utilize `rocksdb_copies` to have ordered set on disk, and use
                    %       it instead of `emqx_trie` for wildcards
                    emqx_router_utils:maybe_trans(
                        fun emqx_router_utils:insert_session_trie_route/2,
                        [?TAB_SESSION_ROUTER, Route],
                        ?PERSISTENT_SESSION_SHARD,
                        fun emqx_trie:lock_session_tables/0
                    );
                false ->
                    emqx_router_utils:insert_direct_route(?TAB_SESSION_ROUTER, Route)
            end
    end.

-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched -> lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.

match_trie(Topic) ->
    case emqx_trie:empty_session() of
        true -> [];
        false -> emqx_trie:match_session(Topic)
    end.

-spec delete_route(emqx_types:topic(), session_id()) -> ok | {error, term()}.
delete_route(Topic, SessionID) ->
    Route = #route{topic = Topic, dest = SessionID},
    case emqx_topic:wildcard(Topic) of
        true ->
            emqx_router_utils:maybe_trans(
                fun emqx_router_utils:delete_session_trie_route/2,
                [?TAB_SESSION_ROUTER, Route],
                ?PERSISTENT_SESSION_SHARD,
                fun emqx_trie:lock_session_tables/0
            );
        false ->
            emqx_router_utils:delete_direct_route(?TAB_SESSION_ROUTER, Route)
    end.

delete_routes(Subscriptions, SessionID) ->
    % TODO: single transaction
    maps:foreach(fun(Topic, _) -> delete_route(Topic, SessionID) end, Subscriptions).

lookup_routes(Topic) ->
    ets:lookup(?TAB_SESSION_ROUTER, Topic).
