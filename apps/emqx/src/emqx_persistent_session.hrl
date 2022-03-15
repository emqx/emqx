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

-define(SESSION_STORE_DISC, emqx_session_store_disc).
-define(SESSION_STORE_RAM, emqx_session_store_ram).

-define(SESS_MSG_TAB_DISC, emqx_session_msg_disc).
-define(SESS_MSG_TAB_RAM, emqx_session_msg_ram).

-define(MSG_TAB_DISC, emqx_persistent_msg_disc).
-define(MSG_TAB_RAM, emqx_persistent_msg_ram).

-record(session_store, {
    client_id :: binary(),
    expiry_interval :: non_neg_integer(),
    ts :: non_neg_integer(),
    session :: emqx_session:session()
}).

-record(session_msg, {
    key :: emqx_persistent_session:sess_msg_key(),
    val = [] :: []
}).

-define(db_backend_key, [persistent_session_store, db_backend]).
-define(is_enabled_key, [persistent_session_store, enabled]).
-define(storage_type_key, [persistent_session_store, storage_type]).
-define(msg_retain, [persistent_session_store, max_retain_undelivered]).

-define(db_backend, (persistent_term:get(?db_backend_key))).
