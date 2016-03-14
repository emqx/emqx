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

%% @doc Authentication Behaviour.
-module(emqttd_auth_mod).

-include("emqttd.hrl").

-export([passwd_hash/2]).

-type hash_type() :: plain | md5 | sha | sha256.

%%--------------------------------------------------------------------
%% Authentication behavihour
%%--------------------------------------------------------------------

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
-spec(passwd_hash(hash_type(), binary()) -> binary()).
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

