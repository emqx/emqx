%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_keepalive_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

keepalive_test() ->
    KA = emqttd_keepalive:start(fun() -> {ok, 1} end, 1, {keepalive, timeout}),
    ?assertEqual([resumed, timeout], lists:reverse(loop(KA, []))).

loop(KA, Acc) ->
    receive
        {keepalive, timeout} ->
            case emqttd_keepalive:check(KA) of
                {ok, KA1} -> loop(KA1, [resumed | Acc]);
                {error, timeout} -> [timeout | Acc]
            end
        after 4000 ->
                Acc
    end.

-endif.
