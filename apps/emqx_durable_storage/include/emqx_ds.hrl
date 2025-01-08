%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_DS_HRL).
-define(EMQX_DS_HRL, true).

-record(dsbatch, {
    operations :: [emqx_ds:operation()],
    preconditions = [] :: [emqx_ds:precondition()]
}).

-record(message_matcher, {
    %% Fields identifying the message:
    %% Client identifier
    from :: binary(),
    %% Topic that the message is published to
    topic :: emqx_types:topic(),
    %% Timestamp (Unit: millisecond)
    timestamp :: integer(),

    %% Fields the message is matched against:
    %% Message Payload
    payload,
    %% Message headers
    headers = #{} :: emqx_types:headers(),
    %% Extra filters
    %% Reserved for the forward compatibility purposes.
    filters = #{}
}).

-record(poll_reply, {
    ref :: reference(),
    userdata,
    payload :: emqx_ds:next_result() | poll_timeout
}).

-record(new_stream_event, {
    subref :: reference()
}).

-endif.
