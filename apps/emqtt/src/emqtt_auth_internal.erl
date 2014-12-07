%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng.lee@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_auth_internal).

-include("emqtt_internal.hrl").

-export([init/1,
		add/2,
		check/2,
		delete/1]).

init(_Opts) ->
	mnesia:create_table(internal_user, [
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, internal_user)}]),
	mnesia:add_table_copy(internal_user, node(), ram_copies),
	ok.

check(undefined, _) -> false;

check(_, undefined) -> false;

check(Username, Password) when is_binary(Username) ->
	PasswdHash = crypto:hash(md5, Password),	
	case mnesia:dirty_read(internal_user, Username) of
	[#internal_user{passwdhash=PasswdHash}] -> true;
	_ -> false
	end.
	
add(Username, Password) when is_binary(Username) and is_binary(Password) ->
	mnesia:dirty_write(#internal_user{username=Username, passwdhash=crypto:hash(md5, Password)}).

delete(Username) when is_binary(Username) ->
	mnesia:dirty_delete(internal_user, Username).

