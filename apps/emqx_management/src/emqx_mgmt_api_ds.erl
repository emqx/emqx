%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_ds).

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/1, enum/1, array/1]).

%% API:
-export([
    list_sites/2,
    get_site/2,
    forget_site/2,
    list_dbs/2,
    get_db/2,
    db_replicas/2,
    db_replica/2,

    update_db_sites/3,
    join/3,
    leave/3,

    shards_of_this_site/0,
    shards_of_site/1,

    forget/2
]).

%% Internal exports:
-export([is_enabled/0]).

%% behavior callbacks:
-export([
    namespace/0,
    api_spec/0,
    schema/1,
    paths/0,
    fields/1
]).

-export([
    check_enabled/2,
    check_db_exists/2
]).

-type sites_shard() :: #{
    storage := emqx_ds:db(),
    id := binary(),
    status => up | down | lost,
    transition => joining | leaving
}.

-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(TAGS, [<<"Durable storage">>]).

%%================================================================================
%% behavior callbacks
%%================================================================================

namespace() ->
    undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true,
        filter => emqx_dashboard_swagger:compose_filters(
            fun ?MODULE:check_enabled/2,
            fun ?MODULE:check_db_exists/2
        )
    }).

paths() ->
    [
        "/ds/sites",
        "/ds/sites/:site",
        "/ds/sites/:site/forget",
        "/ds/storages",
        "/ds/storages/:ds",
        "/ds/storages/:ds/replicas",
        "/ds/storages/:ds/replicas/:site"
    ].

schema("/ds/sites") ->
    #{
        'operationId' => list_sites,
        get =>
            #{
                description => ?DESC("list_sites"),
                tags => ?TAGS,
                responses =>
                    #{
                        200 => mk(array(binary()), #{desc => ?DESC("list_sites")}),
                        404 => disabled_schema()
                    }
            }
    };
schema("/ds/sites/:site") ->
    #{
        'operationId' => get_site,
        get =>
            #{
                description => ?DESC("get_site"),
                parameters => [param_site_id()],
                tags => ?TAGS,
                responses =>
                    #{
                        200 => mk(ref(site), #{desc => ?DESC("get_site")}),
                        404 => not_found(?DESC("site_not_found"))
                    }
            }
    };
schema("/ds/sites/:site/forget") ->
    #{
        'operationId' => forget_site,
        put =>
            #{
                description => ?DESC("forget_site"),
                parameters => [param_site_id()],
                tags => ?TAGS,
                responses =>
                    #{
                        204 => ?DESC("site_forgotten"),
                        400 => bad_request(),
                        404 => not_found(?DESC("site_not_found"))
                    }
            }
    };
schema("/ds/storages") ->
    #{
        'operationId' => list_dbs,
        get =>
            #{
                description => ?DESC("list_dbs"),
                tags => ?TAGS,
                responses =>
                    #{
                        200 => mk(array(atom()), #{desc => ?DESC("list_dbs")}),
                        404 => disabled_schema()
                    }
            }
    };
schema("/ds/storages/:ds") ->
    #{
        'operationId' => get_db,
        get =>
            #{
                description => ?DESC("get_db"),
                tags => ?TAGS,
                parameters => [param_storage_id()],
                responses =>
                    #{
                        200 => mk(ref(db), #{desc => ?DESC("get_db")}),
                        400 => not_found(?DESC("durable_storage_not_found")),
                        404 => disabled_schema()
                    }
            }
    };
schema("/ds/storages/:ds/replicas") ->
    Parameters = [param_storage_id()],
    #{
        'operationId' => db_replicas,
        get =>
            #{
                description => ?DESC("list_db_replicas"),
                tags => ?TAGS,
                parameters => Parameters,
                responses =>
                    #{
                        200 => mk(array(binary()), #{
                            desc => ?DESC("list_db_replicas")
                        }),
                        400 => bad_request(),
                        404 => disabled_schema()
                    }
            },
        put =>
            #{
                description => ?DESC("update_db_replicas"),
                tags => ?TAGS,
                parameters => Parameters,
                responses =>
                    #{
                        202 => mk(array(binary()), #{}),
                        400 => bad_request(),
                        404 => disabled_schema()
                    },
                'requestBody' => mk(array(binary()), #{desc => ?DESC("new_list_of_sites")})
            }
    };
schema("/ds/storages/:ds/replicas/:site") ->
    Parameters = [param_storage_id(), param_site_id()],
    #{
        'operationId' => db_replica,
        put =>
            #{
                description => ?DESC("add_db_replica"),
                tags => ?TAGS,
                parameters => Parameters,
                responses =>
                    #{
                        202 => ?DESC("ok"),
                        400 => bad_request(),
                        404 => not_found(?DESC("storage_not_found"))
                    }
            },
        delete =>
            #{
                description => ?DESC("remove_db_replica"),
                tags => ?TAGS,
                parameters => Parameters,
                responses =>
                    #{
                        202 => ?DESC("ok"),
                        400 => bad_request(),
                        404 => not_found(?DESC("storage_not_found"))
                    }
            }
    }.

fields(site) ->
    [
        {node,
            mk(
                atom(),
                #{
                    desc => ?DESC("node_name"),
                    example => <<"'emqx@example.com'">>
                }
            )},
        {up,
            mk(
                boolean(),
                #{desc => ?DESC("site_up_and_running")}
            )},
        {shards,
            mk(
                array(ref(sites_shard)),
                #{desc => ?DESC("site_shards")}
            )}
    ];
fields(sites_shard) ->
    [
        {storage,
            mk(
                atom(),
                #{
                    desc => ?DESC("durable_storage_name"),
                    example => ?PERSISTENT_MESSAGE_DB
                }
            )},
        {id,
            mk(
                binary(),
                #{
                    desc => ?DESC("shard_id"),
                    example => <<"1">>
                }
            )},
        {status,
            mk(
                atom(),
                #{
                    desc => ?DESC("shard_status"),
                    example => up
                }
            )},
        {transition,
            mk(
                enum([joining, leaving]),
                #{
                    desc => ?DESC("shard_transition"),
                    example => joining
                }
            )}
    ];
fields(db) ->
    [
        {name,
            mk(
                atom(),
                #{
                    desc => ?DESC("durable_storage_name"),
                    example => ?PERSISTENT_MESSAGE_DB
                }
            )},
        {shards,
            mk(
                array(ref(db_shard)),
                #{desc => ?DESC("list_of_shards")}
            )}
    ];
fields(db_shard) ->
    [
        {id,
            mk(
                binary(),
                #{
                    desc => ?DESC("shard_id"),
                    example => <<"1">>
                }
            )},
        {replicas,
            mk(
                hoconsc:array(ref(db_site)),
                #{desc => ?DESC("shard_replicas")}
            )}
    ];
fields(db_site) ->
    [
        {site,
            mk(
                binary(),
                #{
                    desc => ?DESC("site_id"),
                    example => example_site()
                }
            )},
        {status,
            mk(
                enum([up, down, lost]),
                #{desc => ?DESC("status_of_replica")}
            )},
        {transition,
            mk(
                enum([joining, leaving]),
                #{
                    desc => ?DESC("shard_transition"),
                    example => joining
                }
            )}
    ].

%%================================================================================
%% Internal exports
%%================================================================================

check_enabled(Request, _ReqMeta) ->
    case is_enabled() of
        true -> {ok, Request};
        false -> ?NOT_FOUND(<<"Durable storage is disabled">>)
    end.

check_db_exists(Request = #{bindings := #{ds := DB}}, _ReqMeta) ->
    case db_config(DB) of
        #{} -> {ok, Request};
        undefined -> ?NOT_FOUND(emqx_utils:format("DB '~p' does not exist", [DB]))
    end;
check_db_exists(Request, _ReqMeta) ->
    {ok, Request}.

list_sites(get, _Params) ->
    {200, emqx_ds_builtin_raft_meta:sites()}.

get_site(get, #{bindings := #{site := Site}}) ->
    case lists:member(Site, emqx_ds_builtin_raft_meta:sites()) of
        false ->
            ?NOT_FOUND(<<"Site not found: ", Site/binary>>);
        true ->
            Node = emqx_ds_builtin_raft_meta:node(Site),
            Shards = shards_of_site(Site),
            ?OK(#{
                node => Node,
                up => emqx_ds_builtin_raft_meta:node_status(Node) == up,
                shards => Shards
            })
    end.

forget_site(put, #{bindings := #{site := Site}}) ->
    case lists:member(Site, emqx_ds_builtin_raft_meta:sites()) of
        false ->
            ?NOT_FOUND(<<"Site not found: ", Site/binary>>);
        true ->
            case forget(Site, rest) of
                ok ->
                    {204, <<>>};
                {error, Description} ->
                    ?BAD_REQUEST(400, Description)
            end
    end.

list_dbs(get, _Params) ->
    ?OK(dbs()).

get_db(get, #{bindings := #{ds := DB}}) ->
    ?OK(#{
        name => DB,
        shards => list_shards(DB)
    }).

db_replicas(get, #{bindings := #{ds := DB}}) ->
    Replicas = emqx_ds_builtin_raft_meta:db_sites(DB),
    ?OK(Replicas);
db_replicas(put, #{bindings := #{ds := DB}, body := Sites}) ->
    case update_db_sites(DB, Sites, rest) of
        {ok, _} ->
            {202, <<"OK">>};
        {error, Description} ->
            ?BAD_REQUEST(400, Description)
    end.

db_replica(put, #{bindings := #{ds := DB, site := Site}}) ->
    case join(DB, Site, rest) of
        {ok, _} ->
            {202, <<"OK">>};
        {error, Description} ->
            ?BAD_REQUEST(400, Description)
    end;
db_replica(delete, #{bindings := #{ds := DB, site := Site}}) ->
    case leave(DB, Site, rest) of
        {ok, Sites} when is_list(Sites) ->
            {202, <<"OK">>};
        {ok, unchanged} ->
            ?NOT_FOUND(<<"Site is not part of replica set">>);
        {error, Description} ->
            ?BAD_REQUEST(400, Description)
    end.

-spec update_db_sites(emqx_ds:db(), [emqx_ds_builtin_raft_meta:site()], rest | cli) ->
    {ok, [emqx_ds_builtin_raft_meta:site()]} | {error, _}.
update_db_sites(DB, Sites, Via) when is_list(Sites) ->
    ?SLOG(notice, #{
        msg => "durable_storage_rebalance_request",
        ds => DB,
        sites => Sites,
        via => Via
    }),
    meta_result_to_binary(emqx_ds_builtin_raft_meta:assign_db_sites(DB, Sites));
update_db_sites(_, _, _) ->
    {error, <<"Bad type">>}.

-spec join(emqx_ds:db(), emqx_ds_builtin_raft_meta:site(), rest | cli) ->
    {ok, unchanged | [emqx_ds_builtin_raft_meta:site()]} | {error, _}.
join(DB, Site, Via) ->
    ?SLOG(notice, #{
        msg => "durable_storage_join_request",
        ds => DB,
        site => Site,
        via => Via
    }),
    meta_result_to_binary(emqx_ds_builtin_raft_meta:join_db_site(DB, Site)).

-spec leave(emqx_ds:db(), emqx_ds_builtin_raft_meta:site(), rest | cli) ->
    {ok, unchanged | [emqx_ds_builtin_raft_meta:site()]} | {error, _}.
leave(DB, Site, Via) ->
    ?SLOG(notice, #{
        msg => "durable_storage_leave_request",
        ds => DB,
        site => Site,
        via => Via
    }),
    meta_result_to_binary(emqx_ds_builtin_raft_meta:leave_db_site(DB, Site)).

-spec forget(emqx_ds_builtin_raft_meta:site(), rest | cli) ->
    ok | {error, _}.
forget(Site, Via) ->
    ?SLOG(warning, #{
        msg => "durable_storage_forget_request",
        site => Site,
        via => Via
    }),
    meta_result_to_binary(emqx_ds_builtin_raft_meta:forget_site(Site)).

-spec shards_of_this_site() -> [sites_shard()].
shards_of_this_site() ->
    try emqx_ds_builtin_raft_meta:this_site() of
        Site -> shards_of_site(Site)
    catch
        error:badarg -> []
    end.

-spec shards_of_site(emqx_ds_builtin_raft_meta:site()) -> [sites_shard()].
shards_of_site(Site) ->
    lists:flatmap(
        fun({DB, Shard}) ->
            ShardInfo = emqx_ds_builtin_raft_meta:shard_info(DB, Shard),
            ReplicaSet = maps:get(replica_set, ShardInfo),
            TargetSet = maps:get(target_set, ShardInfo, #{}),
            TransitionSet = get_transition_set(ShardInfo),
            case ReplicaSet of
                #{Site := Info} ->
                    S = #{
                        storage => DB,
                        id => Shard,
                        status => maps:get(status, Info)
                    },
                    [annotate_transition(Site, TransitionSet, S)];
                _ ->
                    case TransitionSet of
                        #{Site := add} ->
                            S = #{
                                storage => DB,
                                id => Shard,
                                transition => joining
                            },
                            [annotate_target_status(Site, TargetSet, S)];
                        _ ->
                            []
                    end
            end
        end,
        [
            {DB, Shard}
         || DB <- dbs(),
            Shard <- emqx_ds_builtin_raft_meta:shards(DB)
        ]
    ).

get_transition_set(ShardInfo) ->
    lists:foldr(
        fun maps:merge/2,
        #{},
        maps:get(transitions, ShardInfo, [])
    ).

annotate_transition(Site, TransitionSet, Acc) ->
    case TransitionSet of
        #{Site := T} ->
            Acc#{transition => meta_to_transition(T)};
        #{} ->
            Acc
    end.

annotate_target_status(Site, TargetSet, Acc) ->
    case TargetSet of
        #{Site := Info} ->
            Acc#{status => maps:get(status, Info)};
        #{} ->
            Acc
    end.

%%================================================================================
%% Internal functions
%%================================================================================

disabled_schema() ->
    emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("durable_storage_disabled")).

not_found(Desc) ->
    emqx_dashboard_swagger:error_codes(['NOT_FOUND'], Desc).

bad_request() ->
    emqx_dashboard_swagger:error_codes(['BAD_REQUEST'], ?DESC("bad_request")).

param_site_id() ->
    Info = #{
        required => true,
        in => path,
        desc => ?DESC("site_id"),
        example => example_site()
    },
    {site, mk(binary(), Info)}.

param_storage_id() ->
    Info = #{
        required => true,
        in => path,
        desc => ?DESC("durable_storage_name"),
        example => ?PERSISTENT_MESSAGE_DB
    },
    {ds, mk(atom(), Info)}.

example_site() ->
    try
        emqx_ds_builtin_raft_meta:this_site()
    catch
        _:_ ->
            <<"AFA18CB1C22F0157">>
    end.

dbs() ->
    [DB || DB <- emqx_ds_builtin_raft_meta:dbs(), db_config(DB) =/= undefined].

db_config(DB) ->
    case emqx_ds_builtin_raft_meta:db_config(DB) of
        Config = #{backend := _Builtin} ->
            Config;
        _ ->
            undefined
    end.

list_shards(DB) ->
    [
        begin
            ShardInfo = emqx_ds_builtin_raft_meta:shard_info(DB, Shard),
            ReplicaSet = maps:get(replica_set, ShardInfo),
            Transitions = maps:get(transitions, ShardInfo, []),
            Replicas = maps:fold(
                fun(Site, #{status := Status}, Acc) ->
                    Elem = #{
                        site => Site,
                        status => Status
                    },
                    [Elem | Acc]
                end,
                [],
                maps:iterator(ReplicaSet, reversed)
            ),
            RShard = #{
                id => Shard,
                replicas => Replicas
            },
            case Transitions of
                [] ->
                    RShard;
                [_ | _] ->
                    RTransitions = [
                        #{
                            site => Site,
                            transition => meta_to_transition(T)
                        }
                     || Transition <- Transitions,
                        {Site, T} <- maps:to_list(Transition)
                    ],
                    RShard#{
                        transitions => RTransitions
                    }
            end
        end
     || Shard <- emqx_ds_builtin_raft_meta:shards(DB)
    ].

meta_to_transition(add) -> joining;
meta_to_transition(del) -> leaving.

meta_result_to_binary(ok) ->
    ok;
meta_result_to_binary({Result, {member_of_replica_sets, DBNames}}) ->
    DBs = lists:map(fun atom_to_binary/1, DBNames),
    Msg = ["Site is still a member of replica sets of: " | lists:join(", ", DBs)],
    {Result, iolist_to_binary(Msg)};
meta_result_to_binary({Result, {member_of_target_sets, DBNames}}) ->
    DBs = lists:map(fun atom_to_binary/1, DBNames),
    Msg = ["Site is still a target of replica set transitions in: " | lists:join(", ", DBs)],
    {Result, iolist_to_binary(Msg)};
meta_result_to_binary({ok, Res}) ->
    {ok, Res};
meta_result_to_binary({error, Err}) ->
    {error, meta_error_to_binary(Err)}.

meta_error_to_binary({nonexistent_sites, UnknownSites}) ->
    iolist_to_binary(["Unknown sites: " | lists:join(", ", UnknownSites)]);
meta_error_to_binary({nonexistent_db, DB}) ->
    emqx_utils:format("Unknown storage: ~p", [DB]);
meta_error_to_binary(nonexistent_site) ->
    <<"Unknown site">>;
meta_error_to_binary({lost_sites, LostSites}) ->
    iolist_to_binary(["Replication still targets lost sites: " | lists:join(", ", LostSites)]);
meta_error_to_binary({too_few_sites, _Sites}) ->
    <<"Replica sets would become too small">>;
meta_error_to_binary(site_up) ->
    <<"Site is up and running">>;
meta_error_to_binary(site_temporarily_down) ->
    <<"Site is considered temporarily down">>;
meta_error_to_binary(Err) ->
    emqx_utils:format("Error: ~p", [Err]).

is_enabled() ->
    emqx_ds_builtin_raft_sup:which_dbs() =/= {error, inactive}.
