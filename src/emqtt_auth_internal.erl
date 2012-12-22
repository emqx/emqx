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

