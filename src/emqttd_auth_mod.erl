%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc emqttd Authentication Behaviour
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_mod).

-include("emqttd.hrl").

-export([passwd_hash/2]).

-type hash_type() :: plain | md5 | sha | sha256.

%%%=============================================================================
%%% Auth behavihour
%%%=============================================================================

-ifdef(use_specs).

-callback init(AuthOpts :: list()) -> {ok, State :: any()}.

-callback check(Client, Password, State) -> ok | ignore | {error, string()} when
    Client    :: mqtt_client(),
    Password  :: binary(),
    State     :: any().

-callback description() -> string().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init, 1}, {check, 3}, {description, 0}];
behaviour_info(_Other) ->
    undefined.

-endif.

%% @doc Password Hash
-spec passwd_hash(hash_type(), binary()) -> binary().
passwd_hash(plain,  Password)  ->
    Password;
passwd_hash(md5,    Password)  ->
    hexstring(crypto:hash(md5, Password));
passwd_hash(sha,    Password)  ->
    hexstring(crypto:hash(sha, Password));
passwd_hash(sha256, Password)  ->
    hexstring(crypto:hash(sha256, Password)).

hexstring(<<X:128/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~32.16.0b", [X]));
hexstring(<<X:160/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~40.16.0b", [X]));
hexstring(<<X:256/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~64.16.0b", [X])).

