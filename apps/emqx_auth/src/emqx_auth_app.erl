%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_app).

-include("emqx_authn.hrl").

-behaviour(application).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

-include_lib("emqx_authn_chains.hrl").

-dialyzer({nowarn_function, [start/2]}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    %% required by test cases, ensure the injection of schema
    _ = emqx_conf_schema:roots(),
    {ok, Sup} = emqx_auth_sup:start_link(),
    ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    ok = deinitialize(),
    ok = emqx_authz:deinit().

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

deinitialize() ->
    ok = emqx_authn_utils:cleanup_resources().
