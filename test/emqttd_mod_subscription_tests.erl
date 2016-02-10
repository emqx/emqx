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

-module(emqttd_mod_subscription_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(M, emqttd_mod_subscription).

rep_test() ->
    ?assertEqual(<<"topic/clientId">>, ?M:rep(<<"$c">>, <<"clientId">>, <<"topic/$c">>)),
    ?assertEqual(<<"topic/username">>, ?M:rep(<<"$u">>, <<"username">>, <<"topic/$u">>)),
    ?assertEqual(<<"topic/username/clientId">>,
                 ?M:rep(<<"$c">>, <<"clientId">>,
                       ?M:rep(<<"$u">>, <<"username">>, <<"topic/$u/$c">>))).

-endif.
