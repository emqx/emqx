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
%%% emqttd control commands.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_ctl).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-define(PRINT_MSG(Msg), 
    io:format(Msg)).

-define(PRINT(Format, Args), 
    io:format(Format, Args)).

-export([status/1,
		cluster/1,
		useradd/1,
		userdel/1]).

%TODO: add comment
% bridge
% metrics
% broker
% sockets

status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(emqttd, 1, application:which_applications()) of
	false ->
		?PRINT_MSG("emqttd is not running~n");
	{value,_Version} ->
		?PRINT_MSG("emqttd is running~n")
    end.

cluster([]) ->
    Nodes = [node()|nodes()],
    ?PRINT("cluster nodes: ~p~n", [Nodes]);

cluster([SNode]) ->
	Node = node_name(SNode),
	case net_adm:ping(Node) of
	pong ->
		application:stop(emqttd),
        application:stop(esockd),
		mnesia:stop(),
		mnesia:start(),
		mnesia:change_config(extra_db_nodes, [Node]),
        application:start(esockd),
		application:start(emqttd),
		?PRINT("cluster with ~p successfully.~n", [Node]);
	pang ->
        ?PRINT("failed to connect to ~p~n", [Node])
	end.


useradd([Username, Password]) ->
	?PRINT("~p", [emqttd_auth:add(list_to_binary(Username), list_to_binary(Password))]).

userdel([Username]) ->
	?PRINT("~p", [emqttd_auth:delete(list_to_binary(Username))]).

node_name(SNode) ->
    SNode1 =
    case string:tokens(SNode, "@") of
    [_Node, _Server] ->
        SNode;
    _ ->
        case net_kernel:longnames() of
         true ->
             SNode ++ "@" ++ inet_db:gethostname() ++
                  "." ++ inet_db:res_option(domain);
         false ->
             SNode ++ "@" ++ inet_db:gethostname();
         _ ->
             SNode
         end
    end,
    list_to_atom(SNode1).
