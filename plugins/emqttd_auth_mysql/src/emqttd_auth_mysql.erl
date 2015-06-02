%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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
%%% @doc
%%% emqttd authentication by mysql 'user' table.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_mysql).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqttd/include/emqttd.hrl").

-behaviour(emqttd_auth_mod).

-export([init/1, check/3, description/0]).

-record(state, {user_table, name_field, pass_field, pass_hash}).

init(Opts) -> 
    Mapper = proplists:get_value(field_mapper, Opts),
    {ok, #state{user_table = proplists:get_value(user_table, Opts),
                name_field = proplists:get_value(username, Mapper),
                pass_field = proplists:get_value(password, Mapper),
                pass_hash  = proplists:get_value(password_hash, Opts)}}.

check(#mqtt_client{username = undefined}, _Password, _State) ->
    {error, "Username undefined"};
check(#mqtt_client{username = <<>>}, _Password, _State) ->
    {error, "Username undefined"};
check(_Client, undefined, _State) ->
    {error, "Password undefined"};
check(_Client, <<>>, _State) ->
    {error, "Password undefined"};
check(#mqtt_client{username = Username}, Password,
      #state{user_table = UserTab, pass_hash = Type,
             name_field = NameField, pass_field = PassField}) ->
    Where = {'and', {NameField, Username}, {PassField, hash(Type, Password)}},
    case emysql:select(UserTab, Where) of
        {ok, []} -> {error, "Username or Password "};
        {ok, _Record} -> ok
    end.

description() -> "Authentication by MySQL".

hash(plain, Password) ->
    Password;

hash(md5, Password) ->
    hexstring(crypto:hash(md5, Password));

hash(sha, Password) ->
    hexstring(crypto:hash(sha, Password)).

hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X]));

hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X])).

