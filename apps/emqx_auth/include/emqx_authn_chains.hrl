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

-ifndef(EMQX_AUTHN_CHAINS_HRL).
-define(EMQX_AUTHN_CHAINS_HRL, true).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").

-define(GLOBAL, 'mqtt:global').

-define(TRACE_AUTHN_PROVIDER(Msg), ?TRACE_AUTHN_PROVIDER(Msg, #{})).
-define(TRACE_AUTHN_PROVIDER(Msg, Meta), ?TRACE_AUTHN_PROVIDER(debug, Msg, Meta)).
-define(TRACE_AUTHN_PROVIDER(Level, Msg, Meta),
    ?TRACE_AUTHN(Level, Msg, ?MAPPEND(Meta, #{provider => ?MODULE}))
).

-define(TRACE_AUTHN(Msg, Meta), ?TRACE_AUTHN(debug, Msg, Meta)).
-define(TRACE_AUTHN(Level, Msg, Meta), ?TRACE(Level, ?AUTHN_TRACE_TAG, Msg, Meta)).

%% config root name all auth providers have to agree on.
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME, "authentication").
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM, authentication).
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY, <<"authentication">>).

%% authentication move cmd
-define(CMD_MOVE_FRONT, front).
-define(CMD_MOVE_REAR, rear).
-define(CMD_MOVE_BEFORE(Before), {before, Before}).
-define(CMD_MOVE_AFTER(After), {'after', After}).
-define(CMD_MERGE, merge).

-endif.
