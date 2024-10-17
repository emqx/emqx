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

-record(poll_req, {
    req_id,
    stream,
    topic_filter,
    start_key,
    %% Node from which the poll request originates:
    node,
    %% Information about the process that created the request:
    return_addr,
    %% Iterator:
    it,
    %% Callback that filters messages that belong to the request:
    msg_matcher,
    opts,
    deadline,
    %% Counter that is increased every time the poll request is moved
    %% between the queues. Used to break the loop if backend is
    %% misbehaving:
    hops = 0
}).

-endif.
