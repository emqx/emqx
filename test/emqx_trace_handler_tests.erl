%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trace_handler_tests).

-include_lib("eunit/include/eunit.hrl").

handler_id_test_() ->
    [{"normal_printable",
     fun() ->
             ?assertEqual(trace_topic_t1, emqx_trace_handler:handler_id("t1", topic))
     end},
     {"normal_unicode",
      fun() ->
             ?assertEqual('trace_topic_主题', emqx_trace_handler:handler_id("主题", topic))
      end
     },
     {"not_printable",
      fun() ->
             ?assertEqual('trace_topic_93b885adfe0da089cdf634904fd59f71',
                          emqx_trace_handler:handler_id("\0", topic))
      end
     },
     {"too_long",
      fun() ->
            T = lists:duplicate(250, $a),
            ?assertEqual('trace_topic_1bdbdf1c9087c796394bcda5789f7206',
                         emqx_trace_handler:handler_id(T, topic))
      end
     }
    ].
