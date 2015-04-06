%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% emqttd authentication with username and password.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_username).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([init/1, add/2, check/2, delete/1]).

-define(AUTH_USER_TABLE, mqtt_auth_username).

-record(mqtt_auth_username, {username, password}).

init(_Opts) ->
	mnesia:create_table(?AUTH_USER_TABLE, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, mqtt_user)}]),
	mnesia:add_table_copy(?AUTH_USER_TABLE, node(), ram_copies),
	ok.

check(undefined, _) -> false;

check(_, undefined) -> false;

check(Username, Password) when is_binary(Username), is_binary(Password) ->
	PasswdHash = crypto:hash(md5, Password),	
	case mnesia:dirty_read(?AUTH_USER_TABLE, Username) of
        [#mqtt_user{}] -> true; %password=PasswdHash}
	_ -> false
	end.
	
add(Username, Password) when is_binary(Username) and is_binary(Password) ->
	mnesia:dirty_write(
        #mqtt_user{
            username = Username
            %password = crypto:hash(md5, Password)
        }
    ).

delete(Username) when is_binary(Username) ->
	mnesia:dirty_delete(?AUTH_USER_TABLE, Username).

