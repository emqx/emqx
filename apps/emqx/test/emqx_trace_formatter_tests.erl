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
-module(emqx_trace_formatter_tests).

-include_lib("eunit/include/eunit.hrl").

format_no_tns_in_meta_test() ->
    Meta = #{
        clientid => <<"c">>,
        trace_tag => tag
    },
    Event = #{
        level => debug,
        meta => Meta,
        msg => <<"test_msg">>
    },
    Config = #{payload_encode => hidden},
    Formatted = format(Event, Config),
    ?assertMatch(nomatch, re:run(Formatted, "tns:")),
    ok.

format_tns_in_meta_test() ->
    Meta = #{
        tns => <<"a">>,
        clientid => <<"c">>,
        trace_tag => tag
    },
    Event = #{
        level => debug,
        meta => Meta,
        msg => <<"test_msg">>
    },
    Config = #{payload_encode => hidden},
    Formatted = format(Event, Config),
    ?assertMatch({match, _}, re:run(Formatted, "\stns:\sa\s")),
    ok.

format(Event, Config) ->
    unicode:characters_to_binary(emqx_trace_formatter:format(Event, Config)).
