-module(emqtt_ctl).

-include("emqtt.hrl").

-compile(export_all).
	
status() ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(emqtt, 1, application:which_applications()) of
	false ->
		?PRINT_MSG("emqtt is not running~n");
	{value,_Version} ->
		?PRINT_MSG("emqtt is running~n")
    end.

cluster_info() ->
    Nodes = [node()|nodes()],
    ?PRINT("cluster nodes: ~p~n", [Nodes]).

cluster(Node) ->
	case net_adm:ping(list_to_atom(Node)) of
	pong ->
		?PRINT("cluster with ~p successfully.~n", [Node]);
	pang ->
        ?PRINT("failed to cluster with ~p~n", [Node])
	end.

add_user(Username, Password) ->
	?PRINT("~p", [emqtt_auth:add(list_to_binary(Username), list_to_binary(Password))]).

delete_user(Username) ->
	?PRINT("~p", [emqtt_auth:delete(list_to_binary(Username))]).
