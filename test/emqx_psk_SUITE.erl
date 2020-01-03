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

-module(emqx_psk_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

t_lookup(_) ->
    ok = load(),
    ok = emqx_logger:set_log_level(emergency),
    Opts = [{to_file, user}, {numtests, 10}],
    ?assert(proper:quickcheck(prop_lookup(), Opts)),
    ok = unload(),
    ok = emqx_logger:set_log_level(error).

prop_lookup() ->
    ?FORALL({ClientPSKID, UserState},
            {client_pskid(), user_state()},
            begin
                case emqx_psk:lookup(psk, ClientPSKID, UserState) of
                    {ok, _Result} -> true;
                    error -> true;
                    _Other -> false
                end
            end).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

load() ->
    ok = meck:new(emqx_hooks, [passthrough, no_history]),
    ok = meck:expect(emqx_hooks, run_fold,
                    fun('tls_handshake.psk_lookup', [ClientPSKID], not_found) ->
                            unicode:characters_to_binary(ClientPSKID)
                    end).

unload() ->
    ok = meck:unload(emqx_hooks).

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------

client_pskid() -> oneof([string(), integer(), [1, [-1]]]).

user_state() -> term().
