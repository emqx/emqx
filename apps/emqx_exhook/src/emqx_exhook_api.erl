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

-include("emqx_exhook.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    exhooks/2,
    action_with_name/2,
    move/2,
    server_hooks/2
]).

-import(hoconsc, [mk/1, mk/2, ref/1, enum/1, array/1, map/2]).
-import(emqx_dashboard_swagger, [schema_with_example/2, error_codes/2]).

-define(TAGS, [<<"exhooks">>]).
-define(NOT_FOURD, 'NOT_FOUND').
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(BAD_RPC, 'BAD_RPC').
-define(ERR_BADARGS(REASON), begin
    R0 = err_msg(REASON),
    <<"Bad Arguments: ", R0/binary>>
end).

-dialyzer([
    {nowarn_function, [
        fill_cluster_server_info/5,
        nodes_server_info/5,
        fill_server_hooks_info/4
    ]}
]).

%%--------------------------------------------------------------------
%% schema
%%--------------------------------------------------------------------
namespace() -> "exhook".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/exhooks", "/exhooks/:name", "/exhooks/:name/move", "/exhooks/:name/hooks"].

schema(("/exhooks")) ->
    #{
        'operationId' => exhooks,
        get => #{
            tags => ?TAGS,
            desc => ?DESC(list_all_servers),
            responses => #{200 => mk(array(ref(detail_server_info)))}
        },
        post => #{
            tags => ?TAGS,
            desc => ?DESC(add_server),
            'requestBody' => server_conf_schema(),
            responses => #{
                200 => mk(ref(detail_server_info)),
                400 => error_codes([?BAD_REQUEST], <<"Already exists">>),
                500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
            }
        }
    };
schema("/exhooks/:name") ->
    #{
        'operationId' => action_with_name,
        get => #{
            tags => ?TAGS,
            desc => ?DESC(get_detail),
            parameters => params_server_name_in_path(),
            responses => #{
                200 => mk(ref(detail_server_info)),
                404 => error_codes([?NOT_FOURD], <<"Server not found">>)
            }
        },
        put => #{
            tags => ?TAGS,
            desc => ?DESC(update_server),
            parameters => params_server_name_in_path(),
            'requestBody' => server_conf_schema(),
            responses => #{
                200 => mk(ref(detail_server_info)),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOURD], <<"Server not found">>),
                500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
            }
        },
        delete => #{
            tags => ?TAGS,
            desc => ?DESC(delete_server),
            parameters => params_server_name_in_path(),
            responses => #{
                204 => <<>>,
                404 => error_codes([?NOT_FOURD], <<"Server not found">>),
                500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
            }
        }
    };
schema("/exhooks/:name/hooks") ->
    #{
        'operationId' => server_hooks,
        get => #{
            tags => ?TAGS,
            desc => ?DESC(get_hooks),
            parameters => params_server_name_in_path(),
            responses => #{
                200 => mk(array(ref(list_hook_info))),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
            }
        }
    };
schema("/exhooks/:name/move") ->
    #{
        'operationId' => move,
        post => #{
            tags => ?TAGS,
            desc => ?DESC(move_api),
            parameters => params_server_name_in_path(),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(move_req),
                position_example()
            ),
            responses => #{
                204 => <<"No Content">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                500 => error_codes([?BAD_RPC], <<"Bad RPC">>)
            }
        }
    }.

fields(move_req) ->
    [
        {position,
            mk(string(), #{
                required => true,
                desc => ?DESC(move_position),
                example => <<"front">>
            })}
    ];
fields(detail_server_info) ->
    [
        {metrics, mk(ref(metrics), #{desc => ?DESC(server_metrics)})},
        {node_metrics, mk(array(ref(node_metrics)), #{desc => ?DESC(node_metrics)})},
        {node_status, mk(array(ref(node_status)), #{desc => ?DESC(node_status)})},
        {hooks, mk(array(ref(hook_info)))}
    ] ++ emqx_exhook_schema:server_config();
fields(list_hook_info) ->
    [
        {name, mk(binary(), #{desc => ?DESC(hook_name)})},
        {params, mk(map(name, binary()), #{desc => ?DESC(hook_params)})},
        {metrics, mk(ref(metrics), #{desc => ?DESC(hook_metrics)})},
        {node_metrics, mk(array(ref(node_metrics)), #{desc => ?DESC(node_hook_metrics)})}
    ];
fields(node_metrics) ->
    [
        {node, mk(string(), #{desc => ?DESC(node)})},
        {metrics, mk(ref(metrics), #{desc => ?DESC(metrics)})}
    ];
fields(node_status) ->
    [
        {node, mk(string(), #{desc => ?DESC(node)})},
        {status,
            mk(enum([connected, connecting, disconnected, disabled, error]), #{
                desc => ?DESC(status)
            })}
    ];
fields(hook_info) ->
    [
        {name, mk(binary(), #{desc => ?DESC(hook_name)})},
        {params, mk(map(name, binary()), #{desc => ?DESC(hook_params)})}
    ];
fields(metrics) ->
    [
        {succeed, mk(integer(), #{desc => ?DESC(metric_succeed)})},
        {failed, mk(integer(), #{desc => ?DESC(metric_failed)})},
        {rate, mk(integer(), #{desc => ?DESC(metric_rate)})},
        {max_rate, mk(integer(), #{desc => ?DESC(metric_max_rate)})}
    ];
fields(server_config) ->
    emqx_exhook_schema:server_config().

params_server_name_in_path() ->
    [
        {name,
            mk(string(), #{
                desc => ?DESC(server_name),
                in => path,
                required => true,
                example => <<"default">>
            })}
    ].

server_conf_schema() ->
    SSL = #{
        enable => false,
        cacertfile => emqx:cert_file(<<"cacert.pem">>),
        certfile => emqx:cert_file(<<"cert.pem">>),
        keyfile => emqx:cert_file(<<"key.pem">>)
    },
    schema_with_example(
        ref(server_config),
        #{
            name => "default",
            enable => true,
            url => <<"http://127.0.0.1:8081">>,
            request_timeout => "5s",
            failed_action => deny,
            auto_reconnect => "60s",
            pool_size => 8,
            ssl => SSL
        }
    ).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
exhooks(get, _) ->
    Confs = get_raw_config(),
    Infos = nodes_all_server_info(Confs),
    {200, Infos};
exhooks(post, #{body := #{<<"name">> := Name} = Body}) ->
    case emqx_exhook_mgr:update_config([exhook, servers], {add, Body}) of
        {ok, _} ->
            get_nodes_server_info(Name);
        {error, already_exists} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Already exists">>
            }};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => ?ERR_BADARGS(Reason)
            }}
    end.

action_with_name(get, #{bindings := #{name := Name}}) ->
    get_nodes_server_info(Name);
action_with_name(put, #{bindings := #{name := Name}, body := Body}) ->
    case
        emqx_exhook_mgr:update_config(
            [exhook, servers],
            {update, Name, Body}
        )
    of
        {ok, _} ->
            get_nodes_server_info(Name);
        {error, not_found} ->
            {404, #{
                code => <<"NOT_FOUND">>,
                message => <<"Server not found">>
            }};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => ?ERR_BADARGS(Reason)
            }}
    end;
action_with_name(delete, #{bindings := #{name := Name}}) ->
    case
        emqx_exhook_mgr:update_config(
            [exhook, servers],
            {delete, Name}
        )
    of
        {ok, _} ->
            {204};
        {error, not_found} ->
            {404, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Server not found">>
            }};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => ?ERR_BADARGS(Reason)
            }}
    end.

move(post, #{bindings := #{name := Name}, body := #{<<"position">> := RawPosition}}) ->
    case parse_position(RawPosition) of
        {ok, Position} ->
            case
                emqx_exhook_mgr:update_config(
                    [exhook, servers],
                    {move, Name, Position}
                )
            of
                {ok, ok} ->
                    {204};
                {error, not_found} ->
                    {404, #{
                        code => <<"BAD_REQUEST">>,
                        message => <<"Server not found">>
                    }};
                {error, Error} ->
                    {500, #{
                        code => <<"BAD_RPC">>,
                        message => Error
                    }}
            end;
        {error, invalid_position} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Invalid Position">>
            }}
    end.

server_hooks(get, #{bindings := #{name := Name}}) ->
    Confs = get_raw_config(),
    case lists:search(fun(#{<<"name">> := CfgName}) -> CfgName =:= Name end, Confs) of
        false ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Server not found">>
            }};
        _ ->
            Info = get_nodes_server_hooks_info(Name),
            {200, Info}
    end.

get_nodes_server_info(Name) ->
    Confs = get_raw_config(),
    case lists:search(fun(#{<<"name">> := CfgName}) -> CfgName =:= Name end, Confs) of
        false ->
            {404, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Server not found">>
            }};
        {value, Conf} ->
            NodeStatus = nodes_server_info(Name),
            {200, maps:merge(Conf, NodeStatus)}
    end.

%%--------------------------------------------------------------------
%% GET /exhooks
%%--------------------------------------------------------------------
nodes_all_server_info(ConfL) ->
    AllInfos = call_cluster(fun(Nodes) -> emqx_exhook_proto_v1:all_servers_info(Nodes) end),
    Default = emqx_exhook_metrics:new_metrics_info(),
    node_all_server_info(ConfL, AllInfos, Default, []).

node_all_server_info([#{<<"name">> := ServerName} = Conf | T], AllInfos, Default, Acc) ->
    Info = fill_cluster_server_info(AllInfos, [], [], ServerName, Default),
    AllInfo = maps:merge(Conf, Info),
    node_all_server_info(T, AllInfos, Default, [AllInfo | Acc]);
node_all_server_info([], _, _, Acc) ->
    lists:reverse(Acc).

fill_cluster_server_info([{Node, {error, _}} | T], StatusL, MetricsL, ServerName, Default) ->
    fill_cluster_server_info(
        T,
        [#{node => Node, status => error} | StatusL],
        [#{node => Node, metrics => Default} | MetricsL],
        ServerName,
        Default
    );
fill_cluster_server_info([{Node, Result} | T], StatusL, MetricsL, ServerName, Default) ->
    #{status := Status, metrics := Metrics} = Result,
    fill_cluster_server_info(
        T,
        [#{node => Node, status => maps:get(ServerName, Status, error)} | StatusL],
        [#{node => Node, metrics => maps:get(ServerName, Metrics, Default)} | MetricsL],
        ServerName,
        Default
    );
fill_cluster_server_info([], StatusL, MetricsL, ServerName, _) ->
    Metrics = emqx_exhook_metrics:metrics_aggregate_by_key(metrics, MetricsL),
    #{
        metrics => Metrics,
        node_metrics => MetricsL,
        node_status => StatusL,
        hooks => emqx_exhook_mgr:hooks(ServerName)
    }.

%%--------------------------------------------------------------------
%% GET /exhooks/{name}
%%--------------------------------------------------------------------
nodes_server_info(Name) ->
    InfoL = call_cluster(fun(Nodes) -> emqx_exhook_proto_v1:server_info(Nodes, Name) end),
    Default = emqx_exhook_metrics:new_metrics_info(),
    nodes_server_info(InfoL, Name, Default, [], []).

nodes_server_info([{Node, {error, _}} | T], Name, Default, StatusL, MetricsL) ->
    nodes_server_info(
        T,
        Name,
        Default,
        [#{node => Node, status => error} | StatusL],
        [#{node => Node, metrics => Default} | MetricsL]
    );
nodes_server_info([{Node, Result} | T], Name, Default, StatusL, MetricsL) ->
    #{status := Status, metrics := Metrics} = Result,
    nodes_server_info(
        T,
        Name,
        Default,
        [#{node => Node, status => Status} | StatusL],
        [#{node => Node, metrics => Metrics} | MetricsL]
    );
nodes_server_info([], Name, _, StatusL, MetricsL) ->
    #{
        metrics => emqx_exhook_metrics:metrics_aggregate_by_key(metrics, MetricsL),
        node_status => StatusL,
        node_metrics => MetricsL,
        hooks => emqx_exhook_mgr:hooks(Name)
    }.

%%--------------------------------------------------------------------
%% GET /exhooks/{name}/hooks
%%--------------------------------------------------------------------
get_nodes_server_hooks_info(Name) ->
    case emqx_exhook_mgr:hooks(Name) of
        [] ->
            [];
        Hooks ->
            AllInfos = call_cluster(fun(Nodes) ->
                emqx_exhook_proto_v1:server_hooks_metrics(Nodes, Name)
            end),
            Default = emqx_exhook_metrics:new_metrics_info(),
            get_nodes_server_hooks_info(Hooks, AllInfos, Default, [])
    end.

get_nodes_server_hooks_info([#{name := Name} = Spec | T], AllInfos, Default, Acc) ->
    Info = fill_server_hooks_info(AllInfos, Name, Default, []),
    AllInfo = maps:merge(Spec, Info),
    get_nodes_server_hooks_info(T, AllInfos, Default, [AllInfo | Acc]);
get_nodes_server_hooks_info([], _, _, Acc) ->
    Acc.

fill_server_hooks_info([{_, {error, _}} | T], Name, Default, MetricsL) ->
    fill_server_hooks_info(T, Name, Default, MetricsL);
fill_server_hooks_info([{Node, MetricsMap} | T], Name, Default, MetricsL) ->
    Metrics = maps:get(Name, MetricsMap, Default),
    NodeMetrics = #{node => Node, metrics => Metrics},
    fill_server_hooks_info(T, Name, Default, [NodeMetrics | MetricsL]);
fill_server_hooks_info([], _Name, _Default, MetricsL) ->
    Metrics = emqx_exhook_metrics:metrics_aggregate_by_key(metrics, MetricsL),
    #{metrics => Metrics, node_metrics => MetricsL}.

%%--------------------------------------------------------------------
%% cluster call
%%--------------------------------------------------------------------

-spec call_cluster(fun(([node()]) -> emqx_rpc:erpc_multicall(A))) ->
    [{node(), A | {error, _Err}}].
call_cluster(Fun) ->
    Nodes = mria_mnesia:running_nodes(),
    Ret = Fun(Nodes),
    lists:zip(Nodes, lists:map(fun emqx_rpc:unwrap_erpc/1, Ret)).

%%--------------------------------------------------------------------
%% Internal Funcs
%%--------------------------------------------------------------------
err_msg(Msg) -> emqx_misc:readable_error_msg(Msg).

get_raw_config() ->
    RawConfig = emqx:get_raw_config([exhook, servers], []),
    Schema = #{roots => emqx_exhook_schema:fields(exhook), fields => #{}},
    Conf = #{<<"servers">> => lists:map(fun drop_invalid_certs/1, RawConfig)},
    Options = #{only_fill_defaults => true},
    #{<<"servers">> := Servers} = hocon_tconf:check_plain(Schema, Conf, Options),
    Servers.

drop_invalid_certs(#{<<"ssl">> := SSL} = Conf) when SSL =/= undefined ->
    Conf#{<<"ssl">> => emqx_tls_lib:drop_invalid_certs(SSL)};
drop_invalid_certs(Conf) ->
    Conf.

position_example() ->
    #{
        front =>
            #{
                summary => <<"absolute position 'front'">>,
                value => #{<<"position">> => <<"front">>}
            },
        rear =>
            #{
                summary => <<"absolute position 'rear'">>,
                value => #{<<"position">> => <<"rear">>}
            },
        related_before =>
            #{
                summary => <<"relative position 'before'">>,
                value => #{<<"position">> => <<"before:default">>}
            },
        related_after =>
            #{
                summary => <<"relative position 'after'">>,
                value => #{<<"position">> => <<"after:default">>}
            }
    }.

parse_position(<<"front">>) ->
    {ok, ?CMD_MOVE_FRONT};
parse_position(<<"rear">>) ->
    {ok, ?CMD_MOVE_REAR};
parse_position(<<"before:">>) ->
    {error, invalid_position};
parse_position(<<"after:">>) ->
    {error, invalid_position};
parse_position(<<"before:", Related/binary>>) ->
    {ok, ?CMD_MOVE_BEFORE(Related)};
parse_position(<<"after:", Related/binary>>) ->
    {ok, ?CMD_MOVE_AFTER(Related)};
parse_position(_) ->
    {error, invalid_position}.
