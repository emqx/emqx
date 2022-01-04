%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_exhook_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([exhooks/2, action_with_name/2, move/2]).

-import(hoconsc, [mk/2, ref/1, enum/1, array/1]).
-import(emqx_dashboard_swagger, [schema_with_example/2, error_codes/2]).

-define(TAGS, [<<"exhooks">>]).
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(BAD_RPC, 'BAD_RPC').

namespace() -> "exhook".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/exhooks", "/exhooks/:name", "/exhooks/:name/move"].

schema(("/exhooks")) ->
    #{
      'operationId' => exhooks,
      get => #{tags => ?TAGS,
               description => <<"List all servers">>,
               responses => #{200 => mk(array(ref(detailed_server_info)), #{})}
              },
      post => #{tags => ?TAGS,
                description => <<"Add a servers">>,
                'requestBody' => server_conf_schema(),
                responses => #{201 => mk(ref(detailed_server_info), #{}),
                               500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
                              }
               }
     };

schema("/exhooks/:name") ->
    #{'operationId' => action_with_name,
      get => #{tags => ?TAGS,
               description => <<"Get the detail information of server">>,
               parameters => params_server_name_in_path(),
               responses => #{200 => mk(ref(detailed_server_info), #{}),
                              400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
                             }
              },
      put => #{tags => ?TAGS,
               description => <<"Update the server">>,
               parameters => params_server_name_in_path(),
               'requestBody' => server_conf_schema(),
               responses => #{200 => <<>>,
                              400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                              500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
                             }
              },
      delete => #{tags => ?TAGS,
                  description => <<"Delete the server">>,
                  parameters => params_server_name_in_path(),
                  responses => #{204 => <<>>,
                                 500 => error_codes([?BAD_RPC], <<"Bad RPC">>)                                }
                 }
     };

schema("/exhooks/:name/move") ->
    #{'operationId' => move,
      post => #{tags => ?TAGS,
                description => <<"Move the server">>,
                parameters => params_server_name_in_path(),
                'requestBody' => mk(ref(move_req), #{}),
                responses => #{200 => <<>>,
                               400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                               500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
                              }
               }
     }.

fields(move_req) ->
    [
     {position, mk(enum([top, bottom, before, 'after']), #{})},
     {related, mk(string(), #{desc => <<"Relative position of movement">>,
                              default => <<>>,
                              example => <<>>
                             })}
    ];

fields(detailed_server_info) ->
    [ {status, mk(enum([running, waiting, stopped]), #{})}
    , {hooks, mk(array(string()), #{default => []})}
    , {node_status, mk(ref(node_status), #{})}
    ] ++ emqx_exhook_schema:server_config();

fields(node_status) ->
    [ {node, mk(string(), #{})}
    , {status, mk(enum([running, waiting, stopped, not_found, error]), #{})}
    ];

fields(server_config) ->
    emqx_exhook_schema:server_config().

params_server_name_in_path() ->
    [{name, mk(string(), #{in => path,
                           required => true,
                           example => <<"default">>})}
    ].

server_conf_schema() ->
    schema_with_example(ref(server_config),
                        #{ name => "default"
                         , enable => true
                         , url => <<"http://127.0.0.1:8081">>
                         , request_timeout => "5s"
                         , failed_action => deny
                         , auto_reconnect => "60s"
                         , pool_size => 8
                         , ssl => #{ enable => false
                                   , cacertfile => <<"{{ platform_etc_dir }}/certs/cacert.pem">>
                                   , certfile => <<"{{ platform_etc_dir }}/certs/cert.pem">>
                                   , keyfile => <<"{{ platform_etc_dir }}/certs/key.pem">>
                                   }
                         }).


exhooks(get, _) ->
    ServerL = emqx_exhook_mgr:list(),
    ServerL2 = nodes_all_server_status(ServerL),
    {200, ServerL2};

exhooks(post, #{body := Body}) ->
    case emqx_exhook_mgr:update_config([exhook, servers], {add, Body}) of
        {ok, Result} ->
            {201, Result};
        {error, Error} ->
            {500, #{code => <<"BAD_RPC">>,
                    message => Error
                   }}
    end.

action_with_name(get, #{bindings := #{name := Name}}) ->
    Result = emqx_exhook_mgr:lookup(Name),
    NodeStatus = nodes_server_status(Name),
    case Result of
        not_found ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"Server not found">>
                   }};
        ServerInfo ->
            {200, ServerInfo#{node_status => NodeStatus}}
    end;

action_with_name(put, #{bindings := #{name := Name}, body := Body}) ->
    case emqx_exhook_mgr:update_config([exhook, servers],
                                       {update, Name, Body}) of
        {ok, not_found} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"Server not found">>
                   }};
        {ok, {error, Reason}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => unicode:characters_to_binary(io_lib:format("Error Reason:~p~n", [Reason]))
                   }};
        {ok, _} ->
            {200};
        {error, Error} ->
            {500, #{code => <<"BAD_RPC">>,
                    message => Error
                   }}
    end;

action_with_name(delete, #{bindings := #{name := Name}}) ->
    case emqx_exhook_mgr:update_config([exhook, servers],
                                       {delete, Name}) of
        {ok, _} ->
            {200};
        {error, Error} ->
            {500, #{code => <<"BAD_RPC">>,
                    message => Error
                   }}
    end.

move(post, #{bindings := #{name := Name}, body := Body}) ->
    #{<<"position">> := PositionT, <<"related">> := Related} = Body,
    Position = erlang:binary_to_atom(PositionT),
    case emqx_exhook_mgr:update_config([exhook, servers],
                                       {move, Name, Position, Related}) of
        {ok, ok} ->
            {200};
        {ok, not_found} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"Server not found">>
                   }};
        {error, Error} ->
            {500, #{code => <<"BAD_RPC">>,
                    message => Error
                   }}
    end.

nodes_server_status(Name) ->
    StatusL = call_cluster(emqx_exhook_mgr, server_status, [Name]),

    Handler = fun({Node, {error, _}}) ->
                      #{node => Node,
                        status => error
                       };
                 ({Node, Status}) ->
                      #{node => Node,
                        status => Status
                       }
              end,

    lists:map(Handler, StatusL).

nodes_all_server_status(ServerL) ->
    AllStatusL = call_cluster(emqx_exhook_mgr, all_servers_status, []),

    AggreMap = lists:foldl(fun(#{name := Name}, Acc) ->
                                   Acc#{Name => []}
                           end,
                           #{},
                           ServerL),

    AddToMap = fun(Servers, Node, Status, Map) ->
                       lists:foldl(fun(Name, Acc) ->
                                           StatusL = maps:get(Name, Acc),
                                           StatusL2 = [#{node => Node,
                                                         status => Status
                                                        } | StatusL],
                                           Acc#{Name := StatusL2}
                                   end,
                                   Map,
                                   Servers)
               end,

    AggreMap2 = lists:foldl(fun({Node, #{running := Running,
                                         waiting := Waiting,
                                         stopped := Stopped}},
                                Acc) ->
                                    AddToMap(Stopped, Node, stopped,
                                             AddToMap(Waiting, Node, waiting,
                                                      AddToMap(Running, Node, running, Acc)))
                            end,
                            AggreMap,
                            AllStatusL),

    Handler = fun(#{name := Name} = Server) ->
                      Server#{node_status => maps:get(Name, AggreMap2)}
              end,

    lists:map(Handler, ServerL).

call_cluster(Module, Fun, Args) ->
    Nodes = mria_mnesia:running_nodes(),
    [{Node, rpc_call(Node, Module, Fun, Args)} || Node <- Nodes].

rpc_call(Node, Module, Fun, Args) when Node =:= node() ->
    erlang:apply(Module, Fun, Args);

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.
