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

-module(emqx_foreman_proto_v1).

-behavior(emqx_bpapi).

-include_lib("emqx_utils/include/bpapi.hrl").

%% API
-export([
    stage_assignments/4,
    commit_assignments/3,
    ack_assignments/4,
    nack_assignments/4,
    get_allocation/3
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
    emqx_foreman:gen_id(),
    [emqx_foreman:resource()]
) -> ok.
stage_assignments(Node, ServerRef, GenId, Assignments) ->
    erpc:cast(Node, emqx_foreman, stage_assignments, [ServerRef, GenId, Assignments]).

-spec commit_assignments(
    [node()],
    gen_statem:server_ref(),
    emqx_foreman:gen_id()
) -> ok.
commit_assignments(Nodes, ServerRef, GenId) ->
    erpc:multicast(Nodes, emqx_foreman, commit_assignments, [ServerRef, GenId]).

-spec ack_assignments(
    node(),
    gen_statem:server_ref(),
    emqx_foreman:gen_id(),
    node()
) -> ok.
ack_assignments(Node, ServerRef, GenId, Member) ->
    erpc:cast(Node, emqx_foreman, ack_assignments, [ServerRef, GenId, Member]).

-spec nack_assignments(
    node(),
    gen_statem:server_ref(),
    emqx_foreman:gen_id(),
    node()
) -> ok.
nack_assignments(Node, ServerRef, CurrentGenId, Member) ->
    erpc:cast(Node, emqx_foreman, nack_assignments, [ServerRef, CurrentGenId, Member]).

-spec get_allocation(
    node(),
    gen_statem:server_ref(),
    node()
) ->
    {ok, #{
        gen_id => emqx_foreman:gen_id(),
        status => emqx_foreman:allocation_status(),
        resources => undefined | [emqx_foreman:resource()]
    }}
    | {error, not_leader}
    | {error, noproc}.
get_allocation(Node, ServerRef, Member) ->
    erpc:call(Node, emqx_foreman, get_allocation, [ServerRef, Member]).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
