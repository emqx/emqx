%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_base62).

-export([encode/1, decode/1]).

%% @doc Encode an integer to base62 string
-spec(encode(non_neg_integer()) -> binary()).
encode(I) when is_integer(I) andalso I > 0 ->
    list_to_binary(encode(I, [])).

encode(I, Acc) when I < 62 ->
    [char(I) | Acc];
encode(I, Acc) ->
    encode(I div 62, [char(I rem 62) | Acc]).

char(I) when I < 10 ->
    $0 + I;

char(I) when I < 36 ->
    $A + I - 10;

char(I) when I < 62 ->
    $a + I - 36.

%% @doc Decode base62 string to an integer
-spec(decode(string() | binary()) -> integer()).
decode(B) when is_binary(B) ->
    decode(binary_to_list(B));
decode(S) when is_list(S) ->
    decode(S, 0).

decode([], I) ->
    I;
decode([C|S], I) ->
    decode(S, I * 62 + byte(C)).

byte(C) when $0 =< C andalso C =< $9 ->
    C - $0;
byte(C) when $A =< C andalso C =< $Z ->
    C - $A + 10;
byte(C) when $a =< C andalso C =< $z ->
    C - $a + 36.

