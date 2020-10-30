%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_psk).

-include("logger.hrl").

-logger_header("[PSK]").

%% SSL PSK Callbacks
-export([lookup/3]).

-type psk_identity() :: string().
-type psk_user_state() :: term().

-spec lookup(psk, psk_identity(), psk_user_state()) -> {ok, SharedSecret :: binary()} | error.
lookup(psk, ClientPSKID, _UserState) ->
    try emqx_hooks:run_fold('tls_handshake.psk_lookup', [ClientPSKID], not_found) of
        SharedSecret when is_binary(SharedSecret) -> {ok, SharedSecret};
        Error ->
            ?LOG(error, "Look PSK for PSKID ~p error: ~p", [ClientPSKID, Error]),
            error
    catch
        Except:Error:Stacktrace ->
          ?LOG(error, "Lookup PSK failed, ~0p: ~0p", [{Except,Error}, Stacktrace]),
          error
    end.
