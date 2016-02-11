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

-module(emqttd_time).

-export([seed/0, now_to_secs/0, now_to_secs/1, now_to_ms/0, now_to_ms/1]).

seed() ->
    case erlang:function_exported(erlang, timestamp, 0) of
        true  -> random:seed(erlang:timestamp()); %% R18
        false -> random:seed(os:timestamp()) %% Compress now() deprecated warning...
    end.

now_to_secs() -> now_to_secs(os:timestamp()).

now_to_secs({MegaSecs, Secs, _MicroSecs}) ->
    MegaSecs * 1000000 + Secs.

now_to_ms() -> now_to_ms(os:timestamp()).

now_to_ms({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + round(MicroSecs/1000).

