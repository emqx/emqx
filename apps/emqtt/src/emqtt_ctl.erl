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

-module(emqtt_ctl).

-include("emqtt.hrl").

-include_lib("elog/include/elog.hrl").

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
