#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-define(TIMEOUT, 60000).
-define(INFO(Fmt,Args), io:format(Fmt,Args)).

main([NodeName, Cookie, ReleasePackage]) ->
    TargetNode = start_distribution(NodeName, Cookie),
    {ok, Vsn} = rpc:call(TargetNode, release_handler, unpack_release,
                         [ReleasePackage], ?TIMEOUT),
    ?INFO("Unpacked Release ~p~n", [Vsn]),
    {ok, OtherVsn, Desc} = rpc:call(TargetNode, release_handler,
                                    check_install_release, [Vsn], ?TIMEOUT),
    {ok, OtherVsn, Desc} = rpc:call(TargetNode, release_handler,
                                    install_release, [Vsn], ?TIMEOUT),
    ?INFO("Installed Release ~p~n", [Vsn]),
    ok = rpc:call(TargetNode, release_handler, make_permanent, [Vsn], ?TIMEOUT),
    ?INFO("Made Release ~p Permanent~n", [Vsn]);
main(_) ->
    init:stop(1).

start_distribution(NodeName, Cookie) ->
    MyNode = make_script_node(NodeName),
    {ok, _Pid} = net_kernel:start([MyNode, shortnames]),
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    TargetNode = make_target_node(NodeName),
    case {net_kernel:hidden_connect_node(TargetNode),
          net_adm:ping(TargetNode)} of
        {true, pong} ->
            ok;
        {_, pang} ->
            io:format("Node ~p not responding to pings.\n", [TargetNode]),
            init:stop(1)
    end,
    TargetNode.

make_target_node(Node) ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    list_to_atom(lists:concat([Node, "@", Host])).

make_script_node(Node) ->
    list_to_atom(lists:concat([Node, "_upgrader_", os:getpid()])).
