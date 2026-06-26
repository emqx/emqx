%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics2_api).
-moduledoc """
REST v2 surface for the named-collection topic-metrics feature.
Routes live under `/api/v5/mqtt/topic_metrics2`.

Writes call the `emqx_topic_metrics2` facade, which fans the
operation out across the cluster via `emqx_cluster_rpc`. There is no
HOCON config root behind the scenes — the durable cluster state is a
mria `disc_copies` table owned by `emqx_topic_metrics_registry`.
""".

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("emqx_topic_metrics.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

-import(hoconsc, [mk/2, ref/1, array/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([scopes/0]).

-export([
    collections/2,
    collection/2,
    reset/2
]).

-define(TAGS, [<<"MQTT">>]).
%% Path prefix is deliberately repeated in each schema/1 clause head and
%% in paths/0 so a `grep "/mqtt/topic_metrics2"' finds every reference.
-define(NAME_NOT_FOUND, 'NAME_NOT_FOUND').
-define(BAD_NAME, 'BAD_NAME').
-define(BAD_TOPIC_FILTER, 'BAD_TOPIC_FILTER').
%% ?EXCEED_LIMIT and ?ALREADY_EXISTS come from emqx_utils/include/emqx_http_api.hrl.

namespace() -> "topic_metrics2".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

scopes() -> ?SCOPE_MONITORING.

paths() ->
    [
        "/mqtt/topic_metrics2",
        "/mqtt/topic_metrics2/:name",
        "/mqtt/topic_metrics2/:name/reset"
    ].

schema("/mqtt/topic_metrics2") ->
    #{
        'operationId' => collections,
        get => #{
            tags => ?TAGS,
            description => ?DESC(list_collections),
            responses => #{
                200 => mk(array(ref(collection_view)), #{desc => ?DESC(collection_view_resp)})
            }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC(create_collection),
            'requestBody' => mk(ref(collection_create), #{}),
            responses => #{
                201 => mk(ref(collection_view), #{desc => ?DESC(collection_created_resp)}),
                400 => error_codes(
                    [?BAD_NAME, ?BAD_TOPIC_FILTER],
                    ?DESC(bad_request_resp)
                ),
                409 => error_codes(
                    [?ALREADY_EXISTS, ?EXCEED_LIMIT],
                    ?DESC(conflict_resp)
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(delete_collections),
            responses => #{
                204 => <<"All visible collections removed.">>
            }
        }
    };
schema("/mqtt/topic_metrics2/:name") ->
    #{
        'operationId' => collection,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_collection),
            parameters => [name_param()],
            responses => #{
                200 => mk(ref(collection_view), #{desc => ?DESC(collection_view_resp)}),
                404 => error_codes([?NAME_NOT_FOUND], ?DESC(not_found_resp))
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(delete_collection),
            parameters => [name_param()],
            responses => #{
                204 => <<"Deleted.">>,
                404 => error_codes([?NAME_NOT_FOUND], ?DESC(not_found_resp))
            }
        }
    };
schema("/mqtt/topic_metrics2/:name/reset") ->
    #{
        'operationId' => reset,
        put => #{
            tags => ?TAGS,
            description => ?DESC(reset_collection),
            parameters => [name_param()],
            responses => #{
                204 => <<"Reset.">>,
                404 => error_codes([?NAME_NOT_FOUND], ?DESC(not_found_resp))
            }
        }
    }.

fields(collection_create) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(field_name),
                example => <<"alpha">>
            })},
        {topic_filter,
            mk(binary(), #{
                required => true,
                desc => ?DESC(field_topic_filter),
                example => <<"alpha/#">>
            })}
    ];
fields(collection_view) ->
    [
        {name, mk(binary(), #{required => true, example => <<"alpha">>})},
        {topic_filter, mk(binary(), #{required => true, example => <<"alpha/#">>})},
        {namespace,
            mk(hoconsc:union([null, binary()]), #{
                desc => ?DESC(field_namespace),
                example => null
            })},
        {create_time, mk(binary(), #{example => <<"2026-06-02T12:34:56+00:00">>})},
        {metrics, mk(ref(metrics), #{required => true})}
    ];
fields(metrics) ->
    [{Key, mk(integer(), #{example => 0})} || Key <- count_keys()].

name_param() ->
    {name,
        mk(binary(), #{
            in => path,
            required => true,
            desc => ?DESC(field_name),
            example => <<"alpha">>
        })}.

count_keys() ->
    [
        'messages.in.count',
        'messages.out.count',
        'messages.dropped.count',
        'bytes.in',
        'bytes.out'
    ].

%%--------------------------------------------------------------------
%% Handlers
%%
%% Each collection's mnesia key is `{OwnerNs, BinName}'. The URL path
%% only carries the BinName; the OwnerNs is the actor's namespace
%% (derived from the dashboard token). That means a namespaced admin
%% addresses their own collections by short name, and a global admin's
%% URL operations target the global-namespace collections. To act on
%% another namespace's collection, log in as that namespace's admin.
%% Listing remains "global sees everything".
%%--------------------------------------------------------------------

collections(get, Req) ->
    OwnerNs = actor_ns(Req),
    ?OK([view(R) || R <- aggregated_list(OwnerNs)]);
collections(post, #{body := Body} = Req) ->
    OwnerNs = actor_ns(Req),
    case parse_create(Body) of
        {ok, BinName, TopicFilter} ->
            Name = {OwnerNs, BinName},
            %% Pre-check: register/3 is idempotent at the registry
            %% layer, so we surface duplicate-name errors here to
            %% match operator expectations of POST = create-only.
            case emqx_topic_metrics2:lookup(Name) of
                {ok, _} ->
                    err_create(already_registered);
                {error, not_found} ->
                    case emqx_topic_metrics2:register(BinName, TopicFilter, OwnerNs) of
                        ok ->
                            {ok, Rec} = emqx_topic_metrics2:lookup(Name),
                            ?CREATED(view(with_cluster_counters(Rec)));
                        {error, Reason} ->
                            err_create(Reason)
                    end
            end;
        {error, Code, Msg} ->
            ?BAD_REQUEST(Code, Msg)
    end;
collections(delete, Req) ->
    OwnerNs = actor_ns(Req),
    %% A global admin's "delete all visible" really means "delete
    %% everything"; a namespaced admin's call is scoped to their ns.
    ok = emqx_topic_metrics2:deregister_all(list_scope(OwnerNs)),
    ?NO_CONTENT.

collection(get, #{bindings := #{name := BinName}} = Req) ->
    OwnerNs = actor_ns(Req),
    case emqx_topic_metrics2:lookup(BinName, OwnerNs) of
        {ok, Rec} -> ?OK(view(with_cluster_counters(Rec)));
        {error, not_found} -> not_found(BinName)
    end;
collection(delete, #{bindings := #{name := BinName}} = Req) ->
    OwnerNs = actor_ns(Req),
    case emqx_topic_metrics2:deregister(BinName, OwnerNs) of
        ok -> ?NO_CONTENT;
        {error, not_found} -> not_found(BinName)
    end.

reset(put, #{bindings := #{name := BinName}} = Req) ->
    OwnerNs = actor_ns(Req),
    case emqx_topic_metrics2:reset(BinName, OwnerNs) of
        ok -> ?NO_CONTENT;
        {error, not_found} -> not_found(BinName)
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

actor_ns(Req) ->
    emqx_dashboard:get_namespace(Req).

%% Cluster-aggregated list. Short-circuits the RPC when this node is
%% the only cluster member. A global admin sees every collection; a
%% namespaced admin only sees collections they own.
aggregated_list(OwnerNs) ->
    Scope = list_scope(OwnerNs),
    case emqx:running_nodes() of
        [_OnlyNode] ->
            emqx_topic_metrics2:list(Scope);
        Nodes ->
            Results = emqx_topic_metrics2_proto_v1:list(Nodes, Scope),
            aggregate_node_lists(Results)
    end.

list_scope(?global_ns) -> all_ns;
list_scope(NS) when is_binary(NS) -> NS.

%% `Results' is the flat list returned by `erpc:multicall' — each
%% element is `{ok, NodeList} | {error, _} | {exit, _} | {throw, _}'.
%% Bad responses are ignored so a single dead/lagging node never fails
%% the whole list.
aggregate_node_lists(Results) ->
    Map = lists:foldl(
        fun
            ({ok, NodeList}, Acc) when is_list(NodeList) ->
                lists:foldl(fun merge_into/2, Acc, NodeList);
            (_BadResp, Acc) ->
                Acc
        end,
        #{},
        Results
    ),
    maps:values(Map).

merge_into(#{name := Name, metrics := M} = Rec, Acc) ->
    case maps:find(Name, Acc) of
        error -> Acc#{Name => Rec};
        {ok, #{metrics := M0} = Existing} -> Acc#{Name => Existing#{metrics => sum_metrics(M0, M)}}
    end;
merge_into(_BadEntry, Acc) ->
    Acc.

sum_metrics(A, B) ->
    maps:map(fun(K, V) -> V + maps:get(K, B, 0) end, A).

with_cluster_counters(#{name := Name} = Rec) ->
    case emqx:running_nodes() of
        [_OnlyNode] ->
            case emqx_topic_metrics2:lookup(Name) of
                {ok, #{metrics := M}} -> Rec#{metrics => M};
                _ -> Rec
            end;
        Nodes ->
            Results = emqx_topic_metrics2_proto_v1:lookup(Nodes, Name),
            %% Each Result entry is `{ok, FunReturn} | {error, _} |
            %% {exit, _} | {throw, _}'. The success path nests once
            %% more because the called fun itself returns `{ok, _} |
            %% {error, not_found}'.
            Summed = lists:foldl(
                fun
                    ({ok, {ok, #{metrics := M}}}, Acc) -> sum_metrics(Acc, M);
                    (_, Acc) -> Acc
                end,
                zero_metrics(),
                Results
            ),
            Rec#{metrics => Summed}
    end.

zero_metrics() ->
    maps:from_list([{K, 0} || K <- count_keys()]).

view(
    #{
        bin_name := BinName,
        owner_ns := OwnerNs,
        topic_filter := TF,
        create_time := CreateTime
    } = Rec
) ->
    #{
        name => BinName,
        topic_filter => TF,
        namespace => ns_to_view(OwnerNs),
        create_time => CreateTime,
        metrics => maps:get(metrics, Rec, zero_metrics())
    }.

ns_to_view(?global_ns) -> null;
ns_to_view(NS) when is_binary(NS) -> NS.

parse_create(#{<<"name">> := Name, <<"topic_filter">> := TF}) ->
    case emqx_topic_metrics_schema:validate_name(Name) of
        ok ->
            case emqx_topic_metrics_schema:validate_topic_filter(TF) of
                ok -> {ok, Name, TF};
                {error, _} -> {error, ?BAD_TOPIC_FILTER, <<"Invalid topic filter">>}
            end;
        {error, _} ->
            {error, ?BAD_NAME, <<"Invalid name">>}
    end;
parse_create(_) ->
    {error, ?BAD_NAME, <<"name and topic_filter are required">>}.

err_create(quota_exceeded) ->
    ?CONFLICT(
        ?EXCEED_LIMIT,
        fmt("Max topic_metrics2 collections is ~p", [?MAX_COLLECTIONS])
    );
err_create(already_registered) ->
    ?CONFLICT(?ALREADY_EXISTS, <<"Name already exists">>);
err_create(#{cause := bad_name}) ->
    ?BAD_REQUEST(?BAD_NAME, <<"Invalid name">>);
err_create(#{cause := bad_topic_filter}) ->
    ?BAD_REQUEST(?BAD_TOPIC_FILTER, <<"Invalid topic filter">>);
err_create(Reason) ->
    ?BAD_REQUEST(Reason).

%% Custom code (`NAME_NOT_FOUND'), so we hand-build the response
%% map via `?ERROR_MSG' rather than the generic `?NOT_FOUND' macro
%% (which always uses code `NOT_FOUND').
not_found(Name) when is_binary(Name) ->
    {404, ?ERROR_MSG(?NAME_NOT_FOUND, fmt("Collection '~ts' not found", [Name]))}.

fmt(Fmt, Args) ->
    iolist_to_binary(io_lib:format(Fmt, Args)).
