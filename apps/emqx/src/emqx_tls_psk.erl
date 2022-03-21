%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tls_psk).

-include("logger.hrl").

%% SSL PSK Callbacks
-export([lookup/3]).

-type psk_identity() :: string().
-type psk_user_state() :: term().

-spec lookup(psk, psk_identity(), psk_user_state()) -> {ok, SharedSecret :: binary()} | error.
lookup(psk, PSKIdentity, _UserState) ->
    try emqx_hooks:run_fold('tls_handshake.psk_lookup', [PSKIdentity], normal) of
        {ok, SharedSecret} when is_binary(SharedSecret) ->
            {ok, SharedSecret};
        normal ->
            ?SLOG(info, #{
                msg => "psk_identity_not_found",
                psk_identity => PSKIdentity
            }),
            error;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "psk_identity_not_found",
                psk_identity => PSKIdentity,
                reason => Reason
            }),
            error
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "lookup_psk_failed",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            error
    end.
