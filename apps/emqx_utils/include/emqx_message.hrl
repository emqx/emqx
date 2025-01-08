%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_MESSAGE_HRL).
-define(EMQX_MESSAGE_HRL, true).

%% See 'Application Message' in MQTT Version 5.0
-record(message, {
    %% Global unique message ID
    id :: binary(),
    %% Message QoS
    qos = 0,
    %% Message from
    from :: atom() | binary(),
    %% Message flags
    flags = #{} :: emqx_types:flags(),
    %% Message headers. May contain any metadata. e.g. the
    %% protocol version number, username, peerhost or
    %% the PUBLISH properties (MQTT 5.0).
    headers = #{} :: emqx_types:headers(),
    %% Topic that the message is published to
    topic :: emqx_types:topic(),
    %% Message Payload
    payload :: emqx_types:payload(),
    %% Timestamp (Unit: millisecond)
    timestamp :: integer(),
    %% Miscellaneous extensions, currently used for OpenTelemetry context propagation
    %% and storing mqueue/inflight insertion timestamps.
    %% It was not used prior to 5.4.0 and defaulted to an empty list.
    %% Must be a map now.
    extra = #{} :: term()
}).

-endif.
