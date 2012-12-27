%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_auth_internal).

-include("emqtt.hrl").

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
	PasswdHash = crypto:md5(Password),	
	case mnesia:dirty_read(internal_user, Username) of
	[#internal_user{passwdhash=PasswdHash}] -> true;
	_ -> false
	end.
	
add(Username, Password) when is_binary(Username) and is_binary(Password) ->
	mnesia:dirty_write(#internal_user{username=Username, passwdhash=crypto:md5(Password)}).

delete(Username) when is_binary(Username) ->
	mnesia:dirty_delete(internal_user, Username).

