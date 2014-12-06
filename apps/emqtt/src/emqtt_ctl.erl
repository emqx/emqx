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

-module(emqtt_ctl).

-include("emqtt.hrl").

-include("emqtt_log.hrl").

-export([status/1,
		cluster_info/1,
		cluster/1,
		add_user/1,
		delete_user/1]).

status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(emqtt, 1, application:which_applications()) of
	false ->
		?PRINT_MSG("emqtt is not running~n");
	{value,_Version} ->
		?PRINT_MSG("emqtt is running~n")
    end.

cluster_info([]) ->
    Nodes = [node()|nodes()],
    ?PRINT("cluster nodes: ~p~n", [Nodes]).

cluster([SNode]) ->
	Node = list_to_atom(SNode),
	case net_adm:ping(Node) of
	pong ->
		application:stop(emqtt),
		mnesia:stop(),
		mnesia:start(),
		mnesia:change_config(extra_db_nodes, [Node]),
		application:start(emqtt),
		?PRINT("cluster with ~p successfully.~n", [Node]);
	pang ->
        ?PRINT("failed to connect to ~p~n", [Node])
	end.

add_user([Username, Password]) ->
	?PRINT("~p", [emqtt_auth:add(list_to_binary(Username), list_to_binary(Password))]).

delete_user([Username]) ->
	?PRINT("~p", [emqtt_auth:delete(list_to_binary(Username))]).
