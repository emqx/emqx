%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_auth_mnesia).

-type(login() :: {clientid, binary()}
              | {username, binary()}).

-type(acl_target() :: login() | all).

-type(acl_target_type() :: clientid | username | all).

-type(access():: allow | deny).
-type(action():: pub | sub).
-type(legacy_action():: action() | pubsub).
-type(created_at():: integer()).

-record(emqx_user, {
          login,
          password,
          created_at
        }).

-type(emqx_user() :: #emqx_user{
          login :: login(),
          password :: binary(),
          created_at :: created_at()
        }).

-define(ACL_TABLE, emqx_acl).

-define(MIGRATION_MARK_KEY, emqx_acl2_migration_started).

-record(?ACL_TABLE, {
          filter :: {acl_target(), emqx_topic:topic()} | ?MIGRATION_MARK_KEY,
          action :: legacy_action(),
          access :: access(),
          created_at :: created_at()
         }).

-define(MIGRATION_MARK_RECORD, #?ACL_TABLE{filter = ?MIGRATION_MARK_KEY, action = pub, access = deny, created_at = 0}).

-type(rule() :: {access(), action(), emqx_topic:topic(), created_at()}).

-define(ACL_TABLE2, emqx_acl2).

-record(?ACL_TABLE2, {
          who :: acl_target(),
          rules :: [ rule() ]
         }).

-type(acl_record() :: {acl_target(), emqx_topic:topic(), action(), access(), created_at()}).
