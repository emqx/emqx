%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    flowcontrol,
    %%
    rank,
    stream,
    topic_filter,
    start_key,
    %% Client-supplied subscription key that is returned verbatim:
    userdata,
    %% Iterator:
    it,
    %% Callback that filters messages that belong to the request:
    msg_matcher,
    deadline
}).

%% Persistent term used to store various global information about the
%% shard:
-define(pt_gvar(SHARD), {emqx_ds_beamformer_gvar, SHARD}).

-define(DESTINATION(CLIENT, SUBREF, ITKEY, SEQNO, MASK, FLAGS, ITERATOR),
    {CLIENT, SUBREF, ITKEY, SEQNO, MASK, FLAGS, ITERATOR}
).

%% Bit flags that encode various subscription metadata:
-define(DISPATCH_FLAG_STUCK, 1).
-define(DISPATCH_FLAG_LAGGING, 2).

-record(unsub_req, {id :: reference()}).

-endif.
