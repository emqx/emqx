%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session).

-export([
    is_store_enabled/0,
    init_db_backend/0,
    storage_backend/0,
    storage_type/0
]).

-export([
    discard/2,
    discard_if_present/1,
    lookup/1,
    persist/3,
    persist_message/1,
    pending/1,
    pending/2,
    resume/3
]).

-export([
    add_subscription/3,
    remove_subscription/3
]).

-export([
    mark_as_delivered/2,
    mark_resume_begin/1
]).

-export([
    pending_messages_in_db/2,
    delete_session_message/1,
    gc_session_messages/1,
    session_message_info/2
]).

-export([
    delete_message/1,
    first_message_id/0,
    next_message_id/1
]).

-export_type([sess_msg_key/0]).

-include("emqx.hrl").
-include("emqx_persistent_session.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile({inline, [is_store_enabled/0]}).

%% 16#FFFFFFFF * 1000
-define(MAX_EXPIRY_INTERVAL, 4294967295000).

%% NOTE: Order is significant because of traversal order of the table.
-define(MARKER, 3).
-define(DELIVERED, 2).
-define(UNDELIVERED, 1).
-define(ABANDONED, 0).

-type bin_timestamp() :: <<_:64>>.
-opaque sess_msg_key() ::
    {emqx_guid:guid(), emqx_guid:guid(), emqx_types:topic(), ?UNDELIVERED | ?DELIVERED}
    | {emqx_guid:guid(), emqx_guid:guid(), <<>>, ?MARKER}
    | {emqx_guid:guid(), <<>>, bin_timestamp(), ?ABANDONED}.

-type gc_traverse_fun() :: fun(('delete' | 'marker' | 'abandoned', sess_msg_key()) -> 'ok').

%% EMQX configuration keys
-define(conf_storage_backend, [persistent_session_store, backend, type]).

%%--------------------------------------------------------------------
%% Init
%%--------------------------------------------------------------------

init_db_backend() ->
    case is_store_enabled() of
        true ->
            StorageType = storage_type(),
            ok = emqx_trie:create_session_trie(StorageType),
            ok = emqx_session_router:create_router_tab(StorageType),
            case storage_backend() of
                builtin ->
                    emqx_persistent_session_backend_builtin:create_tables(),
                    persistent_term:put(?db_backend_key, emqx_persistent_session_backend_builtin)
            end,
            ok;
        false ->
            persistent_term:put(?db_backend_key, emqx_persistent_session_backend_dummy),
            ok
    end.

is_store_enabled() ->
    emqx_config:get(?is_enabled_key).

-spec storage_backend() -> builtin.
storage_backend() ->
    emqx_config:get(?conf_storage_backend).

%%--------------------------------------------------------------------
%% Session message ADT API
%%--------------------------------------------------------------------

-spec session_message_info('timestamp' | 'sessionID', sess_msg_key()) -> term().
session_message_info(timestamp, {_, <<>>, <<TS:64>>, ?ABANDONED}) -> TS;
session_message_info(timestamp, {_, GUID, _, _}) -> emqx_guid:timestamp(GUID);
session_message_info(sessionID, {SessionID, _, _, _}) -> SessionID.

%%--------------------------------------------------------------------
%% DB API
%%--------------------------------------------------------------------

first_message_id() ->
    ?db_backend:first_message_id().

next_message_id(Key) ->
    ?db_backend:next_message_id(Key).

delete_message(Key) ->
    ?db_backend:delete_message(Key).

first_session_message() ->
    ?db_backend:first_session_message().

next_session_message(Key) ->
    ?db_backend:next_session_message(Key).

delete_session_message(Key) ->
    ?db_backend:delete_session_message(Key).

put_session_store(#session_store{} = SS) ->
    ?db_backend:put_session_store(SS).

delete_session_store(ClientID) ->
    ?db_backend:delete_session_store(ClientID).

lookup_session_store(ClientID) ->
    ?db_backend:lookup_session_store(ClientID).

put_session_message({_, _, _, _} = Key) ->
    ?db_backend:put_session_message(#session_msg{key = Key}).

put_message(Msg) ->
    ?db_backend:put_message(Msg).

get_message(MsgId) ->
    ?db_backend:get_message(MsgId).

pending_messages_in_db(SessionID, MarkerIds) ->
    ?db_backend:ro_transaction(pending_messages_fun(SessionID, MarkerIds)).

%%--------------------------------------------------------------------
%% Session API
%%--------------------------------------------------------------------

%% The timestamp (TS) is the last time a client interacted with the session,
%% or when the client disconnected.
-spec persist(
    emqx_types:clientinfo(),
    emqx_types:conninfo(),
    emqx_session:session()
) -> emqx_session:session().

persist(#{clientid := ClientID}, ConnInfo, Session) ->
    case ClientID == undefined orelse not emqx_session:info(is_persistent, Session) of
        true ->
            Session;
        false ->
            SS = #session_store{
                client_id = ClientID,
                expiry_interval = maps:get(expiry_interval, ConnInfo),
                ts = timestamp_from_conninfo(ConnInfo),
                session = Session
            },
            case persistent_session_status(SS) of
                not_persistent ->
                    Session;
                expired ->
                    discard(ClientID, Session);
                persistent ->
                    put_session_store(SS),
                    Session
            end
    end.

timestamp_from_conninfo(ConnInfo) ->
    case maps:get(disconnected_at, ConnInfo, undefined) of
        undefined -> erlang:system_time(millisecond);
        Disconnect -> Disconnect
    end.

lookup(ClientID) when is_binary(ClientID) ->
    case is_store_enabled() of
        false ->
            none;
        true ->
            case lookup_session_store(ClientID) of
                none ->
                    none;
                {value, #session_store{session = S} = SS} ->
                    case persistent_session_status(SS) of
                        expired -> {expired, S};
                        persistent -> {persistent, S}
                    end
            end
    end.

-spec discard_if_present(binary()) -> 'ok'.
discard_if_present(ClientID) ->
    case lookup(ClientID) of
        none ->
            ok;
        {Tag, Session} when Tag =:= persistent; Tag =:= expired ->
            _ = discard(ClientID, Session),
            ok
    end.

-spec discard(binary(), emgx_session:session()) -> emgx_session:session().
discard(ClientID, Session) ->
    discard_opt(is_store_enabled(), ClientID, Session).

discard_opt(false, _ClientID, Session) ->
    emqx_session:set_field(is_persistent, false, Session);
discard_opt(true, ClientID, Session) ->
    delete_session_store(ClientID),
    SessionID = emqx_session:info(id, Session),
    put_session_message({SessionID, <<>>, <<(erlang:system_time(microsecond)):64>>, ?ABANDONED}),
    Subscriptions = emqx_session:info(subscriptions, Session),
    emqx_session_router:delete_routes(SessionID, Subscriptions),
    emqx_session:set_field(is_persistent, false, Session).

-spec mark_resume_begin(emqx_session:sessionID()) -> emqx_guid:guid().
mark_resume_begin(SessionID) ->
    MarkerID = emqx_guid:gen(),
    put_session_message({SessionID, MarkerID, <<>>, ?MARKER}),
    MarkerID.

add_subscription(TopicFilter, SessionID, true = _IsPersistent) ->
    case is_store_enabled() of
        true -> emqx_session_router:do_add_route(TopicFilter, SessionID);
        false -> ok
    end;
add_subscription(_TopicFilter, _SessionID, false = _IsPersistent) ->
    ok.

remove_subscription(TopicFilter, SessionID, true = _IsPersistent) ->
    case is_store_enabled() of
        true -> emqx_session_router:do_delete_route(TopicFilter, SessionID);
        false -> ok
    end;
remove_subscription(_TopicFilter, _SessionID, false = _IsPersistent) ->
    ok.

%%--------------------------------------------------------------------
%% Resuming from DB state
%%--------------------------------------------------------------------

%% Must be called inside a emqx_cm_locker transaction.
-spec resume(emqx_types:clientinfo(), emqx_types:conninfo(), emqx_session:session()) ->
    {emqx_session:session(), [emqx_types:deliver()]}.
resume(ClientInfo = #{clientid := ClientID}, ConnInfo, Session) ->
    SessionID = emqx_session:info(id, Session),
    ?tp(ps_resuming, #{from => db, sid => SessionID}),

    %% NOTE: Order is important!

    %% 1. Get pending messages from DB.
    ?tp(ps_initial_pendings, #{sid => SessionID}),
    Pendings1 = pending(SessionID),
    Pendings2 = emqx_session:ignore_local(ClientInfo, Pendings1, ClientID, Session),
    ?tp(ps_got_initial_pendings, #{
        sid => SessionID,
        msgs => Pendings1
    }),

    %% 2. Enqueue messages to mimic that the process was alive
    %%    when the messages were delivered.
    ?tp(ps_persist_pendings, #{sid => SessionID}),
    Session1 = emqx_session:enqueue(ClientInfo, Pendings2, Session),
    Session2 = persist(ClientInfo, ConnInfo, Session1),
    mark_as_delivered(SessionID, Pendings2),
    ?tp(ps_persist_pendings_msgs, #{
        msgs => Pendings2,
        sid => SessionID
    }),

    %% 3. Notify writers that we are resuming.
    %%    They will buffer new messages.
    ?tp(ps_notify_writers, #{sid => SessionID}),
    Nodes = mria_mnesia:running_nodes(),
    NodeMarkers = resume_begin(Nodes, SessionID),
    ?tp(ps_node_markers, #{sid => SessionID, markers => NodeMarkers}),

    %% 4. Subscribe to topics.
    ?tp(ps_resume_session, #{sid => SessionID}),
    ok = emqx_session:resume(ClientInfo, Session2),

    %% 5. Get pending messages from DB until we find all markers.
    ?tp(ps_marker_pendings, #{sid => SessionID}),
    MarkerIDs = [Marker || {_, Marker} <- NodeMarkers],
    Pendings3 = pending(SessionID, MarkerIDs),
    Pendings4 = emqx_session:ignore_local(ClientInfo, Pendings3, ClientID, Session),
    ?tp(ps_marker_pendings_msgs, #{
        sid => SessionID,
        msgs => Pendings4
    }),

    %% 6. Get pending messages from writers.
    ?tp(ps_resume_end, #{sid => SessionID}),
    WriterPendings = resume_end(Nodes, SessionID),
    ?tp(ps_writer_pendings, #{
        msgs => WriterPendings,
        sid => SessionID
    }),

    %% 7. Drain the inbox and usort the messages
    %%    with the pending messages. (Should be done by caller.)
    {Session2, Pendings4 ++ WriterPendings}.

resume_begin(Nodes, SessionID) ->
    Res = emqx_persistent_session_proto_v1:resume_begin(Nodes, self(), SessionID),
    [{Node, Marker} || {{ok, {ok, Marker}}, Node} <- lists:zip(Res, Nodes)].

resume_end(Nodes, SessionID) ->
    Res = emqx_persistent_session_proto_v1:resume_end(Nodes, self(), SessionID),
    ?tp(ps_erpc_multical_result, #{res => Res, sid => SessionID}),
    %% TODO: Should handle the errors
    [
        {deliver, STopic, M}
     || {ok, {ok, Messages}} <- Res,
        {{M, STopic}} <- Messages
    ].

%%--------------------------------------------------------------------
%% Messages API
%%--------------------------------------------------------------------

persist_message(Msg) ->
    case is_store_enabled() of
        true -> do_persist_message(Msg);
        false -> ok
    end.

do_persist_message(Msg) ->
    case emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg) of
        true ->
            ok;
        false ->
            case emqx_session_router:match_routes(emqx_message:topic(Msg)) of
                [] ->
                    ok;
                Routes ->
                    put_message(Msg),
                    MsgId = emqx_message:id(Msg),
                    persist_message_routes(Routes, MsgId, Msg)
            end
    end.

persist_message_routes([#route{dest = SessionID, topic = STopic} | Left], MsgId, Msg) ->
    ?tp(ps_persist_msg, #{sid => SessionID, payload => emqx_message:payload(Msg)}),
    put_session_message({SessionID, MsgId, STopic, ?UNDELIVERED}),
    emqx_session_router:buffer(SessionID, STopic, Msg),
    persist_message_routes(Left, MsgId, Msg);
persist_message_routes([], _MsgId, _Msg) ->
    ok.

mark_as_delivered(SessionID, List) ->
    case is_store_enabled() of
        true -> do_mark_as_delivered(SessionID, List);
        false -> ok
    end.

do_mark_as_delivered(SessionID, [{deliver, STopic, Msg} | Left]) ->
    MsgID = emqx_message:id(Msg),
    case next_session_message({SessionID, MsgID, STopic, ?ABANDONED}) of
        {SessionID, MsgID, STopic, ?UNDELIVERED} = Key ->
            %% We can safely delete this entry
            %% instead of marking it as delivered.
            delete_session_message(Key);
        _ ->
            put_session_message({SessionID, MsgID, STopic, ?DELIVERED})
    end,
    do_mark_as_delivered(SessionID, Left);
do_mark_as_delivered(_SessionID, []) ->
    ok.

-spec pending(emqx_session:sessionID()) ->
    [{emqx_types:message(), STopic :: binary()}].
pending(SessionID) ->
    pending_messages_in_db(SessionID, []).

-spec pending(emqx_session:sessionID(), MarkerIDs :: [emqx_guid:guid()]) ->
    [{emqx_types:message(), STopic :: binary()}].
pending(SessionID, MarkerIds) ->
    %% TODO: Handle lost MarkerIDs
    case emqx_session_router:pending(SessionID, MarkerIds) of
        incomplete ->
            timer:sleep(10),
            pending(SessionID, MarkerIds);
        Delivers ->
            Delivers
    end.

%%--------------------------------------------------------------------
%% Session internal functions
%%--------------------------------------------------------------------

%% @private [MQTT-3.1.2-23]
persistent_session_status(#session_store{expiry_interval = 0}) ->
    not_persistent;
persistent_session_status(#session_store{expiry_interval = ?MAX_EXPIRY_INTERVAL}) ->
    persistent;
persistent_session_status(#session_store{expiry_interval = E, ts = TS}) ->
    case E + TS > erlang:system_time(millisecond) of
        true -> persistent;
        false -> expired
    end.

%%--------------------------------------------------------------------
%% Pending messages internal functions
%%--------------------------------------------------------------------

pending_messages_fun(SessionID, MarkerIds) ->
    fun() ->
        case pending_messages({SessionID, <<>>, <<>>, ?DELIVERED}, [], MarkerIds) of
            {Pending, []} -> read_pending_msgs(Pending, []);
            {_Pending, [_ | _]} -> incomplete
        end
    end.

read_pending_msgs([{MsgId, STopic} | Left], Acc) ->
    Acc1 =
        try
            [{deliver, STopic, get_message(MsgId)} | Acc]
        catch
            error:{msg_not_found, _} ->
                HighwaterMark =
                    erlang:system_time(microsecond) -
                        emqx_config:get(?msg_retain) * 1000,
                case emqx_guid:timestamp(MsgId) < HighwaterMark of
                    %% Probably cleaned by GC
                    true -> Acc;
                    false -> error({msg_not_found, MsgId})
                end
        end,
    read_pending_msgs(Left, Acc1);
read_pending_msgs([], Acc) ->
    lists:reverse(Acc).

%% The keys are ordered by
%%     {sessionID(), <<>>, bin_timestamp(), ?ABANDONED} For abandoned sessions (clean started or expired).
%%     {sessionID(), emqx_guid:guid(), STopic :: binary(), ?DELIVERED | ?UNDELIVERED | ?MARKER}
%%  where
%%     <<>> < emqx_guid:guid()
%%     <<>> < bin_timestamp()
%%     emqx_guid:guid() is ordered in ts() and by node()
%%     ?ABANDONED < ?UNDELIVERED < ?DELIVERED < ?MARKER
%%
%% We traverse the table until we reach another session.
%% TODO: Garbage collect the delivered messages.
pending_messages({SessionID, PrevMsgId, PrevSTopic, PrevTag} = PrevKey, Acc, MarkerIds) ->
    case next_session_message(PrevKey) of
        {S, <<>>, _TS, ?ABANDONED} when S =:= SessionID ->
            {[], []};
        {S, MsgId, <<>>, ?MARKER} = Key when S =:= SessionID ->
            MarkerIds1 = MarkerIds -- [MsgId],
            case PrevTag =:= ?UNDELIVERED of
                false -> pending_messages(Key, Acc, MarkerIds1);
                true -> pending_messages(Key, [{PrevMsgId, PrevSTopic} | Acc], MarkerIds1)
            end;
        {S, MsgId, STopic, ?DELIVERED} = Key when
            S =:= SessionID,
            MsgId =:= PrevMsgId,
            STopic =:= PrevSTopic
        ->
            pending_messages(Key, Acc, MarkerIds);
        {S, _MsgId, _STopic, _Tag} = Key when S =:= SessionID ->
            case PrevTag =:= ?UNDELIVERED of
                false -> pending_messages(Key, Acc, MarkerIds);
                true -> pending_messages(Key, [{PrevMsgId, PrevSTopic} | Acc], MarkerIds)
            end;
        %% Next sessionID or '$end_of_table'
        _What ->
            case PrevTag =:= ?UNDELIVERED of
                false -> {lists:reverse(Acc), MarkerIds};
                true -> {lists:reverse([{PrevMsgId, PrevSTopic} | Acc]), MarkerIds}
            end
    end.

%%--------------------------------------------------------------------
%% Garbage collection
%%--------------------------------------------------------------------

-spec gc_session_messages(gc_traverse_fun()) -> 'ok'.
gc_session_messages(Fun) ->
    gc_traverse(first_session_message(), <<>>, false, Fun).

gc_traverse('$end_of_table', _SessionID, _Abandoned, _Fun) ->
    ok;
gc_traverse({S, <<>>, _TS, ?ABANDONED} = Key, _SessionID, _Abandoned, Fun) ->
    %% Only report the abandoned session if it has no messages.
    %% We want to keep the abandoned marker to last to make the GC reentrant.
    case next_session_message(Key) of
        '$end_of_table' = NextKey ->
            ok = Fun(abandoned, Key),
            gc_traverse(NextKey, S, true, Fun);
        {S2, _, _, _} = NextKey when S =:= S2 ->
            gc_traverse(NextKey, S, true, Fun);
        {_, _, _, _} = NextKey ->
            ok = Fun(abandoned, Key),
            gc_traverse(NextKey, S, true, Fun)
    end;
gc_traverse({S, _MsgID, <<>>, ?MARKER} = Key, SessionID, Abandoned, Fun) ->
    ok = Fun(marker, Key),
    NewAbandoned = S =:= SessionID andalso Abandoned,
    gc_traverse(next_session_message(Key), S, NewAbandoned, Fun);
gc_traverse({S, _MsgID, _STopic, _Tag} = Key, SessionID, Abandoned, Fun) when
    Abandoned andalso
        S =:= SessionID
->
    %% Delete all messages from an abandoned session.
    ok = Fun(delete, Key),
    gc_traverse(next_session_message(Key), S, Abandoned, Fun);
gc_traverse({S, MsgID, STopic, ?UNDELIVERED} = Key, SessionID, Abandoned, Fun) ->
    case next_session_message(Key) of
        {S1, M, ST, ?DELIVERED} = NextKey when
            S1 =:= S andalso
                MsgID =:= M andalso
                STopic =:= ST
        ->
            %% We have both markers for the same message/topic so it is safe to delete both.
            ok = Fun(delete, Key),
            ok = Fun(delete, NextKey),
            gc_traverse(next_session_message(NextKey), S, Abandoned, Fun);
        NextKey ->
            %% Something else is here, so let's just loop.
            NewAbandoned = S =:= SessionID andalso Abandoned,
            gc_traverse(NextKey, SessionID, NewAbandoned, Fun)
    end;
gc_traverse({S, _MsgID, _STopic, ?DELIVERED} = Key, SessionID, Abandoned, Fun) ->
    %% We have a message that is marked as ?DELIVERED, but the ?UNDELIVERED is missing.
    NewAbandoned = S =:= SessionID andalso Abandoned,
    gc_traverse(next_session_message(Key), S, NewAbandoned, Fun).

-spec storage_type() -> ram | disc.
storage_type() ->
    case emqx_config:get(?on_disc_key) of
        true -> disc;
        false -> ram
    end.
