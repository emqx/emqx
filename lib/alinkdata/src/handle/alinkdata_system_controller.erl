-module(alinkdata_system_controller).
-behavior(ehttpd_rest).
-ehttpd_rest(default).

%% API
-export([swagger/1, handle/4]).

swagger(default) ->
    [
        ehttpd_server:bind(<<"/swagger_system.json">>, ?MODULE, [], priv)
    ].


-spec handle(OperationID :: atom(), Args :: map(), Context :: map(), Req :: ehttpd_req:req()) ->
    {Status :: ehttpd_req:http_status(), Data :: map()} |
    {Status :: ehttpd_req:http_status(), Headers :: map(), Data :: map()} |
    {Status :: ehttpd_req:http_status(), Headers :: map(), Data :: map(), Req :: ehttpd_req:req()}.

handle(OperationID, Args, Context, _Req) ->
    case do_request(OperationID, Args, Context) of
        {error, Reason} ->
            Err = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{error => Err}};
        {Status, Headers, Res} ->
            {Status, Headers, Res};
        {Status, Res} ->
            {Status, Res}
    end.

do_request(get_nodes, Args, _Context) ->
    Info = get_nodes_info(get_node(Args)),
    {200, #{data => Info, code => 200}};

do_request(get_brokers, Args, _Context) ->
    Info = get_brokers(get_node(Args)),
    {200, #{data => Info, code => 200}};

do_request(get_stats, Args, _Context) ->
    Info = get_stats(get_node(Args)),
    {200, #{data => Info, code => 200}};

do_request(get_listeners, Args, _Context) ->
    Info = get_listeners(get_node(Args)),
    {200, #{data => Info, code => 200}}.

get_nodes_info(all) ->
    Nodes = emqx_mgmt:list_nodes(),
    [Info ||{_, Info} <- Nodes];
get_nodes_info(Node) ->
    emqx_mgmt:node_info(Node).

get_brokers(all) ->
    Nodes = emqx_mgmt:list_brokers(),
    {_, Desc, Vsn} = lists:keyfind(alinkiot, 1, application:which_applications()),
    [Info#{ version => list_to_binary(Vsn), sysdescr => list_to_binary(Desc) } ||{_, Info} <- Nodes];
get_brokers(Node) ->
    Info = emqx_mgmt:broker_info(Node),
    {_, Desc, Vsn} = lists:keyfind(alinkiot, 1, application:which_applications()),
    Info#{ version => list_to_binary(Vsn), sysdescr => list_to_binary(Desc) }.

get_stats(all) ->
    Nodes = emqx_mgmt:get_stats(),
    [format_stats(Node, Info) || {Node, Info} <- Nodes];
get_stats(Node) ->
    format_stats(Node, emqx_mgmt:get_stats(Node)).

get_listeners(all) ->
    Nodes = emqx_mgmt:list_listeners(),
    [#{Node => format_listen(Infos)} || {Node, Infos} <- Nodes];
get_listeners(Node) ->
    format_listen(emqx_mgmt:list_listeners(Node)).

format_stats(Node, Info) ->
    lists:foldl(
        fun({Key, Value}, Acc) ->
            Acc#{Key => Value}
        end, #{node => Node}, Info).

format_listen(Infos) ->
    lists:foldl(
        fun(Info, Acc0) ->
            R = maps:fold(
                fun(Key, Value, Acc) ->
                    case lists:member(Key, [protocol, current_conns, listen_on, max_conns]) of
                        true when Key =:= listen_on ->
                            Acc#{ listen_on => list_to_binary(format_listen_on(Value)) };
                        true ->
                            Acc#{Key => Value};
                        _ ->
                            Acc
                    end
                end, #{}, Info),
            [R |Acc0]
        end, [], Infos).


format_listen_on({Ip, Port}) ->
    esockd:format({Ip, Port});
format_listen_on(Port) ->
    esockd:format({{0,0,0,0}, Port}).

get_node(#{ <<"node">> := undefined }) -> all;
get_node(#{ <<"node">> := Node }) -> binary_to_atom(Node).

