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

-include("emqx_mqtt.hrl").

%% Session API
-export([
    create/3,
    open/3,
    destroy/1
]).

-export([
    info/2,
    stats/1
]).

-export([
    subscribe/3,
    unsubscribe/2,
    get_subscription/2
]).

-export([
    publish/3,
    puback/3,
    pubrec/2,
    pubrel/2,
    pubcomp/3
]).

-export([
    deliver/3,
    replay/3,
    % handle_timeout/3,
    disconnect/1,
    terminate/2
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
-define(DS_SHARD_ID, <<"local">>).
-define(DEFAULT_KEYSPACE, default).
-define(DS_SHARD, {?DEFAULT_KEYSPACE, ?DS_SHARD_ID}).

-type id() :: emqx_ds:session_id().
-type iterator() :: emqx_ds:iterator().
-type session() :: #{
    %% Client ID
    id := id(),
    %% When the session was created
    created_at := timestamp(),
    %% When the session should expire
    expires_at := timestamp() | never,
    %% Client’s Subscriptions.
    iterators := #{topic() => iterator()},
    %%
    props := map()
}.

-type timestamp() :: emqx_utils_calendar:epoch_millisecond().
-type topic() :: emqx_types:topic().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().

%%

-spec create(clientinfo(), conninfo(), emqx_session:conf()) ->
    session().
create(#{clientid := ClientID}, _ConnInfo, Conf) ->
    % TODO: expiration
    {true, Session} = open_session(ClientID, Conf),
    Session.

-spec open(clientinfo(), conninfo(), emqx_session:conf()) ->
    {_IsPresent :: true, session(), []} | {_IsPresent :: false, session()}.
open(#{clientid := ClientID}, _ConnInfo, Conf) ->
    %% NOTE
    %% The fact that we need to concern about discarding all live channels here
    %% is essentially a consequence of the in-memory session design, where we
    %% have disconnected channels holding onto session state. Ideally, we should
    %% somehow isolate those idling not-yet-expired sessions into a separate process
    %% space, and move this call back into `emqx_cm` where it belongs.
    ok = emqx_cm:discard_session(ClientID),
    {IsNew, Session} = open_session(ClientID, Conf),
    IsPresent = not IsNew,
    case IsPresent of
        true ->
            {IsPresent, Session, []};
        false ->
            {IsPresent, Session}
    end.

open_session(ClientID, Conf) ->
    {IsNew, Session, Iterators} = emqx_ds:session_open(ClientID, Conf),
    {IsNew, Session#{
        iterators => maps:fold(
            fun(Topic, Iterator, Acc) -> Acc#{emqx_topic:join(Topic) => Iterator} end,
            #{},
            Iterators
        )
    }}.

-spec destroy(session() | clientinfo()) -> ok.
destroy(#{id := ClientID}) ->
    emqx_ds:session_drop(ClientID);
destroy(#{clientid := ClientID}) ->
    emqx_ds:session_drop(ClientID).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, #{id := ClientID}) ->
    ClientID;
info(clientid, #{id := ClientID}) ->
    ClientID;
info(created_at, #{created_at := CreatedAt}) ->
    CreatedAt;
info(is_persistent, #{}) ->
    true;
info(subscriptions, #{iterators := Iters}) ->
    maps:map(fun(_, #{props := SubOpts}) -> SubOpts end, Iters);
info(subscriptions_cnt, #{iterators := Iters}) ->
    maps:size(Iters);
info(subscriptions_max, #{props := Conf}) ->
    maps:get(max_subscriptions, Conf);
info(upgrade_qos, #{props := Conf}) ->
    maps:get(upgrade_qos, Conf);
% info(inflight, #sessmem{inflight = Inflight}) ->
%     Inflight;
% info(inflight_cnt, #sessmem{inflight = Inflight}) ->
%     emqx_inflight:size(Inflight);
% info(inflight_max, #sessmem{inflight = Inflight}) ->
%     emqx_inflight:max_size(Inflight);
info(retry_interval, #{props := Conf}) ->
    maps:get(retry_interval, Conf);
% info(mqueue, #sessmem{mqueue = MQueue}) ->
%     MQueue;
% info(mqueue_len, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:len(MQueue);
% info(mqueue_max, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:max_len(MQueue);
% info(mqueue_dropped, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:dropped(MQueue);
info(next_pkt_id, #{}) ->
    _PacketId = 'TODO';
% info(awaiting_rel, #sessmem{awaiting_rel = AwaitingRel}) ->
%     AwaitingRel;
% info(awaiting_rel_cnt, #sessmem{awaiting_rel = AwaitingRel}) ->
%     maps:size(AwaitingRel);
info(awaiting_rel_max, #{props := Conf}) ->
    maps:get(max_awaiting_rel, Conf);
info(await_rel_timeout, #{props := Conf}) ->
    maps:get(await_rel_timeout, Conf).

-spec stats(session()) -> emqx_types:stats().
stats(Session) ->
    % TODO: stub
    info([], Session).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE / UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec subscribe(topic(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, iterators := Iters}
) when is_map_key(TopicFilter, Iters) ->
    Iterator = maps:get(TopicFilter, Iters),
    NIterator = update_subscription(TopicFilter, Iterator, SubOpts, ID),
    {ok, Session#{iterators := Iters#{TopicFilter => NIterator}}};
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, iterators := Iters}
) ->
    % TODO: max_subscriptions
    Iterator = add_subscription(TopicFilter, SubOpts, ID),
    {ok, Session#{iterators := Iters#{TopicFilter => Iterator}}}.

-spec unsubscribe(topic(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    TopicFilter,
    Session = #{id := ID, iterators := Iters}
) when is_map_key(TopicFilter, Iters) ->
    Iterator = maps:get(TopicFilter, Iters),
    SubOpts = maps:get(props, Iterator),
    ok = del_subscription(TopicFilter, Iterator, ID),
    {ok, Session#{iterators := maps:remove(TopicFilter, Iters)}, SubOpts};
unsubscribe(
    _TopicFilter,
    _Session = #{}
) ->
    {error, ?RC_NO_SUBSCRIPTION_EXISTED}.

-spec get_subscription(topic(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(TopicFilter, #{iterators := Iters}) ->
    case maps:get(TopicFilter, Iters, undefined) of
        Iterator = #{} ->
            maps:get(props, Iterator);
        undefined ->
            undefined
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec publish(emqx_types:packet_id(), emqx_types:message(), session()) ->
    {ok, emqx_types:publish_result(), replies(), session()}
    | {error, emqx_types:reason_code()}.
publish(_PacketId, Msg, Session) ->
    % TODO: stub
    {ok, emqx_broker:publish(Msg), [], Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-spec puback(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
puback(_ClientInfo, _PacketId, _Session = #{}) ->
    % TODO: stub
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec pubrec(emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()}
    | {error, emqx_types:reason_code()}.
pubrec(_PacketId, _Session = #{}) ->
    % TODO: stub
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec pubrel(emqx_types:packet_id(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
pubrel(_PacketId, Session = #{}) ->
    % TODO: stub
    {ok, Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec pubcomp(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
pubcomp(_ClientInfo, _PacketId, _Session = #{}) ->
    % TODO: stub
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}.

%%--------------------------------------------------------------------

-spec deliver(clientinfo(), [emqx_types:deliver()], session()) ->
    no_return().
deliver(_ClientInfo, _Delivers, _Session = #{}) ->
    % TODO: ensure it's unreachable somehow
    error(unexpected).

-spec replay(clientinfo(), [], session()) ->
    {ok, replies(), session()}.
replay(_ClientInfo, [], Session = #{}) ->
    {ok, [], Session}.

%%--------------------------------------------------------------------

-spec disconnect(session()) -> {shutdown, session()}.
disconnect(Session = #{}) ->
    {shutdown, Session}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(_Reason, _Session = #{}) ->
    % TODO: close iterators
    ok.

%%--------------------------------------------------------------------

-spec add_subscription(topic(), emqx_types:subopts(), id()) ->
    emqx_ds:iterator().
add_subscription(TopicFilterBin, SubOpts, DSSessionID) ->
    % N.B.: we chose to update the router before adding the subscription to the
    % session/iterator table.  The reasoning for this is as follows:
    %
    % Messages matching this topic filter should start to be persisted as soon as
    % possible to avoid missing messages.  If this is the first such persistent
    % session subscription, it's important to do so early on.
    %
    % This could, in turn, lead to some inconsistency: if such a route gets
    % created but the session/iterator data fails to be updated accordingly, we
    % have a dangling route.  To remove such dangling routes, we may have a
    % periodic GC process that removes routes that do not have a matching
    % persistent subscription.  Also, route operations use dirty mnesia
    % operations, which inherently have room for inconsistencies.
    %
    % In practice, we use the iterator reference table as a source of truth,
    % since it is guarded by a transaction context: we consider a subscription
    % operation to be successful if it ended up changing this table.  Both router
    % and iterator information can be reconstructed from this table, if needed.
    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilterBin, DSSessionID),
    TopicFilter = emqx_topic:words(TopicFilterBin),
    {ok, Iterator, IsNew} = emqx_ds:session_add_iterator(
        DSSessionID, TopicFilter, SubOpts
    ),
    Ctx = #{iterator => Iterator, is_new => IsNew},
    ?tp(persistent_session_ds_iterator_added, Ctx),
    ?tp_span(
        persistent_session_ds_open_iterators,
        Ctx,
        ok = open_iterator_on_all_shards(TopicFilter, Iterator)
    ),
    Iterator.

-spec update_subscription(topic(), iterator(), emqx_types:subopts(), id()) ->
    iterator().
update_subscription(TopicFilterBin, Iterator, SubOpts, DSSessionID) ->
    TopicFilter = emqx_topic:words(TopicFilterBin),
    {ok, NIterator, false} = emqx_ds:session_add_iterator(
        DSSessionID, TopicFilter, SubOpts
    ),
    ok = ?tp(persistent_session_ds_iterator_updated, #{iterator => Iterator}),
    NIterator.

-spec open_iterator_on_all_shards(emqx_topic:words(), emqx_ds:iterator()) -> ok.
open_iterator_on_all_shards(TopicFilter, Iterator) ->
    ?tp(persistent_session_ds_will_open_iterators, #{iterator => Iterator}),
    %% Note: currently, shards map 1:1 to nodes, but this will change in the future.
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:open_iterator(
        Nodes,
        TopicFilter,
        maps:get(start_time, Iterator),
        maps:get(id, Iterator)
    ),
    %% TODO
    %% 1. Handle errors.
    %% 2. Iterator handles are rocksdb resources, it's doubtful they survive RPC.
    %%    Even if they do, we throw them away here anyway. All in all, we probably should
    %%    hold each of them in a process on the respective node.
    true = lists:all(fun(Res) -> element(1, Res) =:= ok end, Results),
    ok.

%% RPC target.
-spec do_open_iterator(emqx_topic:words(), emqx_ds:time(), emqx_ds:iterator_id()) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _Reason}.
do_open_iterator(TopicFilter, StartMS, IteratorID) ->
    Replay = {TopicFilter, StartMS},
    emqx_ds_storage_layer:ensure_iterator(?DS_SHARD, IteratorID, Replay).

-spec del_subscription(topic(), iterator(), id()) ->
    ok.
del_subscription(TopicFilterBin, #{id := IteratorID}, DSSessionID) ->
    % N.B.: see comments in `?MODULE:add_subscription' for a discussion about the
    % order of operations here.
    TopicFilter = emqx_topic:words(TopicFilterBin),
    Ctx = #{iterator_id => IteratorID},
    ?tp_span(
        persistent_session_ds_close_iterators,
        Ctx,
        ok = ensure_iterator_closed_on_all_shards(IteratorID)
    ),
    ?tp_span(
        persistent_session_ds_iterator_delete,
        Ctx,
        emqx_ds:session_del_iterator(DSSessionID, TopicFilter)
    ),
    ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilterBin, DSSessionID).

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

-spec ensure_all_iterators_closed(id()) -> ok.
ensure_all_iterators_closed(DSSessionID) ->
    %% Note: currently, shards map 1:1 to nodes, but this will change in the future.
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:close_all_iterators(Nodes, DSSessionID),
    %% TODO: handle errors
    true = lists:all(fun(Res) -> Res =:= {ok, ok} end, Results),
    ok.

%% RPC target.
-spec do_ensure_all_iterators_closed(id()) -> ok.
do_ensure_all_iterators_closed(DSSessionID) ->
    ok = emqx_ds_storage_layer:discard_iterator_prefix(?DS_SHARD, DSSessionID),
    ok.
