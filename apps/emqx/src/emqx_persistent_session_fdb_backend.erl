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

-module(emqx_persistent_session_fdb_backend).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_persistent_session.hrl").

-define(FDB_SESSION_STORE, emqx_fdb_session_store).

-export([ create_tables/0
        , first_message_id/0
        , next_message_id/1
        , delete_message/1
        , first_session_message/0
        , next_session_message/1
        , delete_session_message/1
        , put_session_store/1
        , delete_session_store/1
        , lookup_session_store/1
        , put_session_message/1
        , put_message/1
        , get_message/1
        , ro_transaction/1
        ]).

create_tables() ->
    Db = erlfdb:open(),
    persistent_term:put(?FDB_SESSION_STORE, {Db}).

first_session_message() ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    Opts = [{limit, 1}],
    case erlfdb:get_range_startswith(Db, erlfdb_tuple:pack({<<"SESS_MSG_TAB">>}), Opts) of
        [] -> '$end_of_table';
        [{_, Binary}] -> 
            ?SLOG(debug, #{db => fdb, first_session_message => (binary_to_term(Binary))#session_msg.key}),
            (binary_to_term(Binary))#session_msg.key
    end.

next_session_message(Key) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    Opts = [{limit, 2}],
    case erlfdb:get_range(Db, erlfdb_tuple:pack({<<"SESS_MSG_TAB">>, term_to_binary(Key)}), erlfdb_key:strinc(erlfdb_tuple:pack({<<"SESS_MSG_TAB">>})), Opts) of
        [] -> '$end_of_table';
        [_] -> 
            ?SLOG(debug, #{db => fdb, key => Key, next_session_message => '$end_of_table'}),
            '$end_of_table';
        [_, {_, Binary}] -> 
            ?SLOG(debug, #{db => fdb, key => Key, next_session_message => (binary_to_term(Binary))#session_msg.key}),
            (binary_to_term(Binary))#session_msg.key
    end.

first_message_id() ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    Opts = [{limit, 1}],
    case erlfdb:get_range_startswith(Db, erlfdb_tuple:pack({<<"MSG_TAB">>}), Opts) of
        [] -> '$end_of_table';
        [{_, Binary}] -> 
            ?SLOG(debug, #{db => fdb, first_message_id => emqx_message:id(binary_to_term(Binary))}),
            emqx_message:id(binary_to_term(Binary))
    end.

next_message_id(Key) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    Opts = [{limit, 2}],
    case erlfdb:get_range(Db, erlfdb_tuple:pack({<<"MSG_TAB">>, term_to_binary(Key)}), erlfdb_key:strinc(erlfdb_tuple:pack({<<"MSG_TAB">>})), Opts) of
        [] -> '$end_of_table';
        [_] -> 
            ?SLOG(debug, #{db => fdb, key => Key, next_message_id => '$end_of_table'}),
            '$end_of_table';
        [_, {_, Binary}] -> 
            ?SLOG(debug, #{db => fdb, key => Key, next_message_id => emqx_message:id(binary_to_term(Binary))}),
            emqx_message:id(binary_to_term(Binary))
    end.

delete_message(MsgId) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    erlfdb:clear(Db, erlfdb_tuple:pack({<<"MSG_TAB">>, MsgId})).

delete_session_message(Key) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    erlfdb:clear(Db, erlfdb_tuple:pack({<<"SESS_MSG_TAB">>, term_to_binary(Key)})),
    ?SLOG(debug, #{key => Key}),
    mria:dirty_delete(?SESS_MSG_TAB, Key).

put_session_store(SS) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    ClientID = SS#session_store.client_id,
    _ = erlfdb:set(Db, erlfdb_tuple:pack({<<"SESSION_STORE">>, ClientID}), term_to_binary(SS)).

delete_session_store(ClientID) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    erlfdb:clear(Db, erlfdb_tuple:pack({<<"SESSION_STORE">>, ClientID})).

lookup_session_store(ClientID) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    case erlfdb:get(Db, erlfdb_tuple:pack({<<"SESSION_STORE">>, ClientID})) of
        not_found -> none;
        Binary -> {value, binary_to_term(Binary)}
    end.

put_session_message(SessMsg) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    Key = SessMsg#session_msg.key,
    ?SLOG(debug, #{key => Key, sess_msg => SessMsg}),
    _ = erlfdb:set(Db, erlfdb_tuple:pack({<<"SESS_MSG_TAB">>, term_to_binary(Key)}), term_to_binary(SessMsg)).

put_message(Msg) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    MsgId = emqx_message:id(Msg),
    _ = erlfdb:set(Db, erlfdb_tuple:pack({<<"MSG_TAB">>, MsgId}), term_to_binary(Msg)).

get_message(MsgId) ->
    {Db} = persistent_term:get(?FDB_SESSION_STORE),
    case erlfdb:get(Db, erlfdb_tuple:pack({<<"MSG_TAB">>, MsgId})) of
        not_found -> error({msg_not_found, MsgId});
        Binary -> binary_to_term(Binary)
    end.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?PERSISTENT_SESSION_SHARD, Fun),
    Res.

