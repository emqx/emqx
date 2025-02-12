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
-export([with_worker/3, terminate/3]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

%% @doc The caller will receive message of type `{reference(), Result | {error, unrecoverable, map()}'
-spec with_worker(module(), atom(), list()) -> {ok, reference()}.
with_worker(Mod, Function, Args) ->
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
            ReplyTo ! {ReplyTo, Result}
        end,
        [link, {min_heap_size, 10000}]
    ),
    {ok, ReplyTo}.

-spec terminate(module(), _Reason, map()) -> ok.
terminate(Module, Reason, Misc) when Reason =:= shutdown; Reason =:= normal ->
    ?tp(emqx_ds_process_terminate, Misc#{module => Module, reason => Reason});
terminate(Module, Reason, Misc) ->
    ?tp(warning, emqx_ds_abnormal_process_terminate, Misc#{module => Module, reason => Reason}).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
