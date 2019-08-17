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

-module(emqx_oom_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_init(_) ->
    ?assertEqual(undefined, emqx_oom:init(undefined)),
    Opts = #{message_queue_len => 10,
             max_heap_size => 1024*1024*8
            },
    Oom = emqx_oom:init(Opts),
    ?assertEqual(#{message_queue_len => 10,
                   max_heap_size => 1024*1024
                  }, emqx_oom:info(Oom)).

t_check(_) ->
    ?assertEqual(ok, emqx_oom:check(undefined)),
    Opts = #{message_queue_len => 10,
             max_heap_size => 1024*1024*8
            },
    Oom = emqx_oom:init(Opts),
    [self() ! {msg, I} || I <- lists:seq(1, 5)],
    ?assertEqual(ok, emqx_oom:check(Oom)),
    [self() ! {msg, I} || I <- lists:seq(1, 6)],
    ?assertEqual({shutdown, message_queue_too_long}, emqx_oom:check(Oom)).

