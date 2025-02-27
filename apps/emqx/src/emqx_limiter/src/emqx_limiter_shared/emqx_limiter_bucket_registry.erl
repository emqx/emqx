%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Registry for shared limiter buckets.

-module(emqx_limiter_bucket_registry).
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([
    find_bucket/1,
    insert_bucket/2,
    delete_bucket/1
]).

-type bucket_ref() :: emqx_limiter_shared:bucket_ref().
-type limiter_id() :: emqx_limiter:id().

-define(PT_KEY(LimiterId), {?MODULE, LimiterId}).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec find_bucket(limiter_id()) -> bucket_ref() | undefined.
find_bucket(LimiterId) ->
    persistent_term:get(?PT_KEY(LimiterId), undefined).

-spec insert_bucket(limiter_id(), bucket_ref()) -> ok.
insert_bucket(LimiterId, Bucket) ->
    persistent_term:put(?PT_KEY(LimiterId), Bucket).

-spec delete_bucket(limiter_id()) -> ok.
delete_bucket(LimiterId) ->
    _ = persistent_term:erase(?PT_KEY(LimiterId)),
    ok.
