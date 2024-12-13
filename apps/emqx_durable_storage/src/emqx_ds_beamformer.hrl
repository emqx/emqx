%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_DS_BEAMFORMER_HRL).
-define(EMQX_DS_BEAMFORMER_HRL, true).

-record(sub_state, {
    %% Unique ID of the subscription:
    req_id,
    %% PID of the subscribing process:
    client,
    %% Monitor reference that we use for monitoring the client. It's
    %% kept in the record for cleanup during normal unsubscribe:
    mref,
    %% Flow control:
    seqno = 0,
    acked_seqno = 0,
    max_unacked,
    %%
    stream,
    topic_filter,
    start_key,
    %% Information about the process that created the request:
    return_addr,
    %% Iterator:
    it,
    %% Callback that filters messages that belong to the request:
    msg_matcher,
    opts,
    deadline,
    %% PID of the beamformer worker process that is currently owning
    %% the subscription:
    owner
}).

%% Persistent term used to store reference to the subscription table
%% for the shard:
-define(ps_subtid(SHARD), {emqx_ds_beamformer_sub_tab, SHARD}).

%% Persistent term used to store callback module that implements
%% beamformer API for the backend:
-define(ps_cbm(DB), {emqx_ds_beamformer_cbm, DB}).

-endif.
