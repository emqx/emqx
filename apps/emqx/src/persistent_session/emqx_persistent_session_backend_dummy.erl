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

-module(emqx_persistent_session_backend_dummy).

-include("emqx_persistent_session.hrl").

-export([
    first_message_id/0,
    next_message_id/1,
    delete_message/1,
    first_session_message/0,
    next_session_message/1,
    delete_session_message/1,
    put_session_store/1,
    delete_session_store/1,
    lookup_session_store/1,
    put_session_message/1,
    put_message/1,
    get_message/1,
    ro_transaction/1
]).

first_message_id() ->
    '$end_of_table'.

next_message_id(_) ->
    '$end_of_table'.

-spec delete_message(binary()) -> no_return().
delete_message(_Key) ->
    error(should_not_be_called).

first_session_message() ->
    '$end_of_table'.

next_session_message(_Key) ->
    '$end_of_table'.

delete_session_message(_Key) ->
    ok.

put_session_store(#session_store{}) ->
    ok.

delete_session_store(_ClientID) ->
    ok.

lookup_session_store(_ClientID) ->
    none.

put_session_message({_, _, _, _}) ->
    ok.

put_message(_Msg) ->
    ok.

-spec get_message(binary()) -> no_return().
get_message(_MsgId) ->
    error(should_not_be_called).

ro_transaction(Fun) ->
    Fun().
