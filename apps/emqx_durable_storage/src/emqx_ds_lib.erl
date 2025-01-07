%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_lib).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API:
-export([with_worker/4, send_poll_timeout/2]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec with_worker(_UserData, module(), atom(), list()) -> {ok, reference()}.
with_worker(UserData, Mod, Function, Args) ->
    ReplyTo = alias([reply]),
    _ = spawn_opt(
        fun() ->
            Result =
                try
                    apply(Mod, Function, Args)
                catch
                    EC:Err:Stack ->
                        {error, unrecoverable, #{
                            msg => ?FUNCTION_NAME,
                            EC => Err,
                            stacktrace => Stack
                        }}
                end,
            ReplyTo ! #poll_reply{userdata = UserData, ref = ReplyTo, payload = Result}
        end,
        [link, {min_heap_size, 10000}]
    ),
    {ok, ReplyTo}.

-spec send_poll_timeout(reference(), timeout()) -> ok.
send_poll_timeout(ReplyTo, Timeout) ->
    _ = spawn(
        fun() ->
            receive
            after Timeout + 10 ->
                ?tp(emqx_ds_poll_timeout_send, #{reply_to => ReplyTo}),
                ReplyTo ! #poll_reply{ref = ReplyTo, payload = poll_timeout}
            end
        end
    ),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
