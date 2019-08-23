%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%--------------------------------------------------------------------
%% @doc OOM (Out Of Memory) monitor for the channel process.
%% @end
%%--------------------------------------------------------------------

-module(emqx_oom).

-include("types.hrl").

-export([ init/1
        , check/1
        , info/1
        ]).

-export_type([oom_policy/0]).

-type(opts() :: #{message_queue_len => non_neg_integer(),
                  max_heap_size => non_neg_integer()
                 }).

-opaque(oom_policy() :: {oom_policy, opts()}).

-type(reason() :: message_queue_too_long|proc_heap_too_large).

-define(DISABLED, 0).

%% @doc Init the OOM policy.
-spec(init(maybe(opts())) -> oom_policy()).
init(undefined) -> undefined;
init(#{message_queue_len := MaxQLen,
       max_heap_size := MaxHeapSizeInBytes}) ->
    MaxHeapSize = MaxHeapSizeInBytes div erlang:system_info(wordsize),
    %% If set to zero, the limit is disabled.
    _ = erlang:process_flag(max_heap_size, #{size => MaxHeapSize,
                                             kill => false,
                                             error_logger => true
                                            }),
    {oom_policy, #{message_queue_len => MaxQLen,
                   max_heap_size => MaxHeapSize
                  }}.

%% @doc Check self() process status against channel process management policy,
%% return `ok | {shutdown, Reason}' accordingly.
%% `ok': There is nothing out of the ordinary.
%% `shutdown': Some numbers (message queue length hit the limit),
%%             hence shutdown for greater good (system stability).
-spec(check(maybe(oom_policy())) -> ok | {shutdown, reason()}).
check(undefined) -> ok;
check({oom_policy, #{message_queue_len := MaxQLen,
                     max_heap_size := MaxHeapSize}}) ->
    Qlength = proc_info(message_queue_len),
    HeapSize = proc_info(total_heap_size),
    do_check([{fun() -> is_exceeded(Qlength, MaxQLen) end,
               {shutdown, message_queue_too_long}},
              {fun() -> is_exceeded(HeapSize, MaxHeapSize) end,
               {shutdown, proc_heap_too_large}}]).

do_check([]) ->
    ok;
do_check([{Pred, Result} | Rest]) ->
    case Pred() of
        true -> Result;
        false -> do_check(Rest)
    end.

-spec(info(maybe(oom_policy())) -> maybe(opts())).
info(undefined) -> undefined;
info({oom_policy, Opts}) ->
    Opts.

-compile({inline,
          [ is_exceeded/2
          , is_enabled/1
          , proc_info/1
          ]}).

is_exceeded(Val, Max) ->
    is_enabled(Max) andalso Val > Max.

is_enabled(Max) ->
    is_integer(Max) andalso Max > ?DISABLED.

proc_info(Key) ->
    {Key, Value} = erlang:process_info(self(), Key),
    Value.

