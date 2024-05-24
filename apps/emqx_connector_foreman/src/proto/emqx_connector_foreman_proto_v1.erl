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

-module(emqx_connector_foreman_proto_v1).

-behavior(emqx_bpapi).

-include_lib("emqx_utils/include/bpapi.hrl").

%% API
-export([
    stage_assignments/4,
    ack_assignments/3,
    nack_assignments/3
]).

%% `emqx_bpapi' API
-export([introduced_in/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_bpapi' API
%%------------------------------------------------------------------------------

introduced_in() ->
    "5.8.0".

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec stage_assignments(
    node(),
    gen_statem:server_ref(),
    emqx_connector_foreman:gen_id(),
    [emqx_connector_foreman:resource()]
) -> ok.
stage_assignments(Node, ServerRef, GenId, Assignments) ->
    erpc:cast(Node, emqx_connector_foreman, stage_assignments, [ServerRef, GenId, Assignments]).

-spec ack_assignments(
    node(),
    gen_statem:server_ref(),
    emqx_connector_foreman:gen_id()
) -> ok.
ack_assignments(Node, ServerRef, GenId) ->
    erpc:cast(Node, emqx_connector_foreman, ack_assignments, [ServerRef, GenId]).

-spec nack_assignments(
    node(),
    gen_statem:server_ref(),
    emqx_connector_foreman:gen_id()
) -> ok.
nack_assignments(Node, ServerRef, CurrentGenId) ->
    erpc:cast(Node, emqx_connector_foreman, nack_assignments, [ServerRef, CurrentGenId]).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
