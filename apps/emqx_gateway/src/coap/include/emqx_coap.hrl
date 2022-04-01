%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_coap).
-define(DEFAULT_COAP_PORT, 5683).
-define(DEFAULT_COAPS_PORT, 5684).
-define(MAX_MESSAGE_ID, 65535).
-define(MAX_BLOCK_SIZE, 1024).
-define(DEFAULT_MAX_AGE, 60).
-define(MAXIMUM_MAX_AGE, 4294967295).

-type coap_message_id() :: 1..?MAX_MESSAGE_ID.
-type message_type() :: con | non | ack | reset.
-type max_age() :: 1..?MAXIMUM_MAX_AGE.

-type message_option_name() ::
    if_match
    | uri_host
    | etag
    | if_none_match
    | uri_port
    | location_path
    | uri_path
    | content_format
    | max_age
    | uri_query
    | 'accept'
    | location_query
    | proxy_uri
    | proxy_scheme
    | size1
    | observer
    | block1
    | block2.

-type message_options() :: #{
    if_match => list(binary()),
    uri_host => binary(),
    etag => list(binary()),
    if_none_match => boolean(),
    uri_port => 0..65535,
    location_path => list(binary()),
    uri_path => list(binary()),
    content_format => 0..65535,
    max_age => non_neg_integer(),
    uri_query => list(binary()) | map(),
    'accept' => 0..65535,
    location_query => list(binary()),
    proxy_uri => binary(),
    proxy_scheme => binary(),
    size1 => non_neg_integer(),
    observer => non_neg_integer(),
    block1 => {non_neg_integer(), boolean(), non_neg_integer()},
    block2 => {non_neg_integer(), boolean(), non_neg_integer()}
}.

-record(coap_mqtt_auth, {clientid, username, password}).

-record(coap_message, {
    type :: message_type(),
    method,
    id,
    token = <<>>,
    options = #{},
    payload = <<>>
}).

-type coap_message() :: #coap_message{}.
