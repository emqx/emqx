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

-define(TOPK_TAB, emqx_slow_subs_topk).
-define(BASE_FACTOR, 10000).
-define(TO_BASIS(X), erlang:floor((X) * ?BASE_FACTOR)).
-define(MAKE_INDEX(Basis, ClientId), {Basis, ClientId}).

-type basis_point() :: integer().
-type index() :: {basis_point(), emqx_types:clientid()}.

-record(slow_sub, { index :: index()
                  , last_stats :: emqx_slow_subs_stats:summary()
                  , mqueue_len :: integer()
                  , inflight_len :: integer()
                  , wait_ack :: integer()
                  , wait_rec :: integer()
                  , wait_comp :: integer()
                  }).

-type slow_sub() :: #slow_sub{}.

-type slow_subs_stats_args() :: emqx_channel:channel().
