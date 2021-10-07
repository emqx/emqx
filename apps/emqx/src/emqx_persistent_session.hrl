%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(SESSION_STORE, emqx_session_store).
-define(SESS_MSG_TAB, emqx_session_msg).
-define(MSG_TAB, emqx_persistent_msg).

-record(session_store, { client_id        :: binary()
                       , expiry_interval  :: non_neg_integer()
                       , ts               :: non_neg_integer()
                       , session          :: emqx_session:session()}).

-record(session_msg, {key      :: emqx_persistent_session:sess_msg_key(),
                      val = [] :: []}).

-define(db_backend_key, [persistent_session_store, db_backend]).
-define(is_enabled_key, [persistent_session_store, enabled]).
-define(msg_retain, [persistent_session_store, max_retain_undelivered]).

-define(db_backend, (persistent_term:get(?db_backend_key))).
