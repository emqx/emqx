%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_prometheus_auth).

-export([
    deregister_cleanup/1,
    collect_mf/2,
    collect_metrics/2
]).

-export([collect/1]).

%% for bpapi
-export([
    fetch_metric_data_from_local_node/0
]).

%% %% @private
-export([
    zip_json_auth_metrics/3
]).

-include("emqx_prometheus.hrl").
-include_lib("emqx_auth/include/emqx_authn_chains.hrl").
-include_lib("prometheus/include/prometheus.hrl").

-import(
    prometheus_model_helpers,
    [
        create_mf/5,
        gauge_metric/1,
        gauge_metrics/1,
        counter_metrics/1
    ]
).

-type authn_metric_name() ::
    emqx_authn_enable
    | emqx_authn_status
    | emqx_authn_nomatch
    | emqx_authn_total
    | emqx_authn_success
    | emqx_authn_failed.

-type authz_metric_name() ::
    emqx_authz_enable
    | emqx_authz_status
    | emqx_authz_nomatch
    | emqx_authz_total
    | emqx_authz_success
    | emqx_authz_failed.

%% Please don't remove this attribute, prometheus uses it to
%% automatically register collectors.
-behaviour(prometheus_collector).

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------

-define(METRIC_NAME_PREFIX, "emqx_auth_").

-define(MG(K, MAP), maps:get(K, MAP)).
-define(MG0(K, MAP), maps:get(K, MAP, 0)).
-define(PG0(K, PROPLISTS), proplists:get_value(K, PROPLISTS, 0)).

-define(AUTHNS_WITH_TYPE, [
    {emqx_authn_enable, gauge},
    {emqx_authn_status, gauge},
    {emqx_authn_nomatch, counter},
    {emqx_authn_total, counter},
    {emqx_authn_success, counter},
    {emqx_authn_failed, counter}
]).

-define(AUTHZS_WITH_TYPE, [
    {emqx_authz_enable, gauge},
    {emqx_authz_status, gauge},
    {emqx_authz_nomatch, counter},
    {emqx_authz_total, counter},
    {emqx_authz_success, counter},
    {emqx_authz_failed, counter}
]).

-define(AUTHN_USERS_COUNT_WITH_TYPE, [
    {emqx_authn_users_count, gauge}
]).

-define(AUTHZ_RULES_COUNT_WITH_TYPE, [
    {emqx_authz_rules_count, gauge}
]).

-define(BANNED_WITH_TYPE, [
    {emqx_banned_count, gauge}
]).

-define(LOGICAL_SUM_METRIC_NAMES, [
    emqx_authn_enable,
    emqx_authn_status,
    emqx_authz_enable,
    emqx_authz_status
]).

%%--------------------------------------------------------------------
%% Collector API
%%--------------------------------------------------------------------

%% @private
deregister_cleanup(_) -> ok.

%% @private
-spec collect_mf(_Registry, Callback) -> ok when
    _Registry :: prometheus_registry:registry(),
    Callback :: prometheus_collector:collect_mf_callback().
%% erlfmt-ignore
collect_mf(?PROMETHEUS_AUTH_REGISTRY, Callback) ->
    RawData = raw_data(erlang:get(format_mode)),
    ok = add_collect_family(Callback, ?AUTHNS_WITH_TYPE, ?MG(authn, RawData)),
    ok = add_collect_family(Callback, ?AUTHN_USERS_COUNT_WITH_TYPE, ?MG(authn_users_count, RawData)),
    ok = add_collect_family(Callback, ?AUTHZS_WITH_TYPE, ?MG(authz, RawData)),
    ok = add_collect_family(Callback, ?AUTHZ_RULES_COUNT_WITH_TYPE, ?MG(authz_rules_count, RawData)),
    ok = add_collect_family(Callback, ?BANNED_WITH_TYPE, ?MG(banned_count, RawData)),
    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    FormatMode = erlang:get(format_mode),
    RawData = raw_data(FormatMode),
    %% TODO: merge node name in json format
    #{
        emqx_authn => collect_json_data(?MG(authn, RawData)),
        emqx_authz => collect_json_data(?MG(authz, RawData)),
        emqx_banned => collect_banned_data()
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_AUTH_REGISTRY).

add_collect_family(Callback, MetricWithType, Data) ->
    _ = [add_collect_family(Name, Data, Callback, Type) || {Name, Type} <- MetricWithType],
    ok.

add_collect_family(Name, Data, Callback, Type) ->
    Callback(create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

collect_metrics(Name, Metrics) ->
    collect_auth(Name, Metrics).

%% @private
fetch_metric_data_from_local_node() ->
    {node(self()), #{
        authn => authn_data(),
        authz => authz_data()
    }}.

fetch_cluster_consistented_metric_data() ->
    #{
        authn_users_count => authn_users_count_data(),
        authz_rules_count => authz_rules_count_data(),
        banned_count => banned_count_data()
    }.

%% raw data for different format modes
raw_data(nodes_aggregated) ->
    AggregatedNodesMetrics = aggre_cluster(all_nodes_metrics()),
    maps:merge(AggregatedNodesMetrics, fetch_cluster_consistented_metric_data());
raw_data(nodes_unaggregated) ->
    %% then fold from all nodes
    AllNodesMetrics = with_node_name_label(all_nodes_metrics()),
    maps:merge(AllNodesMetrics, fetch_cluster_consistented_metric_data());
raw_data(node) ->
    {_Node, LocalNodeMetrics} = fetch_metric_data_from_local_node(),
    maps:merge(LocalNodeMetrics, fetch_cluster_consistented_metric_data()).

all_nodes_metrics() ->
    Nodes = mria:running_nodes(),
    _ResL = emqx_prometheus_proto_v2:raw_prom_data(
        Nodes, ?MODULE, fetch_metric_data_from_local_node, []
    ).

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%====================
%% Authn overview
collect_auth(K = emqx_authn_enable, Data) ->
    gauge_metrics(?MG(K, Data));
collect_auth(K = emqx_authn_status, Data) ->
    gauge_metrics(?MG(K, Data));
collect_auth(K = emqx_authn_nomatch, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authn_total, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authn_success, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authn_failed, Data) ->
    counter_metrics(?MG(K, Data));
%%====================
%% Authn users count
%% Only provided for `password_based:built_in_database` and `scram:built_in_database`
collect_auth(K = emqx_authn_users_count, Data) ->
    gauge_metrics(?MG(K, Data));
%%====================
%% Authz overview
collect_auth(K = emqx_authz_enable, Data) ->
    gauge_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_status, Data) ->
    gauge_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_nomatch, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_total, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_success, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_failed, Data) ->
    counter_metrics(?MG(K, Data));
%%====================
%% Authz rules count
%% Only provided for `file` and `built_in_database`
collect_auth(K = emqx_authz_rules_count, Data) ->
    gauge_metrics(?MG(K, Data));
%%====================
%% Banned
collect_auth(emqx_banned_count, Data) ->
    gauge_metric(Data).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%========================================
%% AuthN (Authentication)
%%========================================

%%====================
%% Authn overview

-spec authn_data() -> #{Key => [Point]} when
    Key :: authn_metric_name(),
    Point :: {[Label], Metric},
    Label :: IdLabel,
    IdLabel :: {id, AuthnName :: binary()},
    Metric :: number().
authn_data() ->
    Authns = emqx_config:get([authentication]),
    lists:foldl(
        fun(Key, AccIn) ->
            AccIn#{Key => authn_backend_to_points(Key, Authns)}
        end,
        #{},
        authn_metric_names()
    ).

-spec authn_backend_to_points(Key, list(Authn)) -> list(Point) when
    Key :: authn_metric_name(),
    Authn :: map(),
    Point :: {[Label], Metric},
    Label :: IdLabel,
    IdLabel :: {id, AuthnName :: binary()},
    Metric :: number().
authn_backend_to_points(Key, Authns) ->
    do_authn_backend_to_points(Key, Authns, []).

do_authn_backend_to_points(_K, [], AccIn) ->
    lists:reverse(AccIn);
do_authn_backend_to_points(K, [Authn | Rest], AccIn) ->
    Id = authenticator_id(Authn),
    Point = {[{id, Id}], do_metric(K, Authn, lookup_authn_metrics_local(Id))},
    do_authn_backend_to_points(K, Rest, [Point | AccIn]).

lookup_authn_metrics_local(Id) ->
    case emqx_authn_api:lookup_from_local_node(?GLOBAL, Id) of
        {ok, {_Node, Status, #{counters := Counters}, _ResourceMetrics}} ->
            #{
                emqx_authn_status => emqx_prometheus_utils:status_to_number(Status),
                emqx_authn_nomatch => ?MG0(nomatch, Counters),
                emqx_authn_total => ?MG0(total, Counters),
                emqx_authn_success => ?MG0(success, Counters),
                emqx_authn_failed => ?MG0(failed, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authn_metric_names() -- [emqx_authn_enable], 0)
    end.

authn_metric_names() ->
    emqx_prometheus_utils:metric_names(?AUTHNS_WITH_TYPE).

%%====================
%% Authn users count

-define(AUTHN_MNESIA, emqx_authn_mnesia).
-define(AUTHN_SCRAM_MNESIA, emqx_authn_scram_mnesia).

authn_users_count_data() ->
    Samples = lists:foldl(
        fun
            (#{backend := built_in_database, mechanism := password_based} = Authn, AccIn) ->
                [auth_data_sample_point(authn, Authn, ?AUTHN_MNESIA) | AccIn];
            (#{backend := built_in_database, mechanism := scram} = Authn, AccIn) ->
                [auth_data_sample_point(authn, Authn, ?AUTHN_SCRAM_MNESIA) | AccIn];
            (_, AccIn) ->
                AccIn
        end,
        [],
        emqx_config:get([authentication])
    ),
    #{emqx_authn_users_count => Samples}.

%%========================================
%% AuthZ (Authorization)
%%========================================

%%====================
%% Authz overview

-spec authz_data() -> #{Key => [Point]} when
    Key :: authz_metric_name(),
    Point :: {[Label], Metric},
    Label :: TypeLabel,
    TypeLabel :: {type, AuthZType :: binary()},
    Metric :: number().
authz_data() ->
    Authzs = emqx_config:get([authorization, sources]),
    lists:foldl(
        fun(Key, AccIn) ->
            AccIn#{Key => authz_backend_to_points(Key, Authzs)}
        end,
        #{},
        authz_metric_names()
    ).

-spec authz_backend_to_points(Key, list(Authz)) -> list(Point) when
    Key :: authz_metric_name(),
    Authz :: map(),
    Point :: {[Label], Metric},
    Label :: TypeLabel,
    TypeLabel :: {type, AuthZType :: binary()},
    Metric :: number().
authz_backend_to_points(Key, Authzs) ->
    do_authz_backend_to_points(Key, Authzs, []).

do_authz_backend_to_points(_K, [], AccIn) ->
    lists:reverse(AccIn);
do_authz_backend_to_points(K, [Authz | Rest], AccIn) ->
    Type = maps:get(type, Authz),
    Point = {[{type, Type}], do_metric(K, Authz, lookup_authz_metrics_local(Type))},
    do_authz_backend_to_points(K, Rest, [Point | AccIn]).

lookup_authz_metrics_local(Type) ->
    case emqx_authz_api_sources:lookup_from_local_node(Type) of
        {ok, {_Node, Status, #{counters := Counters}, _ResourceMetrics}} ->
            #{
                emqx_authz_status => emqx_prometheus_utils:status_to_number(Status),
                emqx_authz_nomatch => ?MG0(nomatch, Counters),
                emqx_authz_total => ?MG0(total, Counters),
                emqx_authz_success => ?MG0(success, Counters),
                emqx_authz_failed => ?MG0(failed, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authz_metric_names() -- [emqx_authz_enable], 0)
    end.

authz_metric_names() ->
    emqx_prometheus_utils:metric_names(?AUTHZS_WITH_TYPE).

%%====================
%% Authz rules count

-define(ACL_TABLE, emqx_acl).

authz_rules_count_data() ->
    Samples = lists:foldl(
        fun
            (#{type := built_in_database} = Authz, AccIn) ->
                [auth_data_sample_point(authz, Authz, ?ACL_TABLE) | AccIn];
            (#{type := file}, AccIn) ->
                #{annotations := #{rules := Rules}} = emqx_authz:lookup(file),
                Size = erlang:length(Rules),
                [{[{type, file}], Size} | AccIn];
            (_, AccIn) ->
                AccIn
        end,
        [],
        emqx_config:get([authorization, sources])
    ),
    #{emqx_authz_rules_count => Samples}.

%%========================================
%% Banned
%%========================================

%%====================
%% Banned count

-define(BANNED_TABLE, emqx_banned).
banned_count_data() ->
    mnesia_size(?BANNED_TABLE).

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`

collect_json_data(Data) ->
    emqx_prometheus_utils:collect_json_data(Data, fun zip_json_auth_metrics/3).

collect_banned_data() ->
    #{emqx_banned_count => banned_count_data()}.

%% for initialized empty AccIn
%% The following fields will be put into Result
%% For Authn:
%%     `id`, `emqx_authn_users_count`
%% For Authz:
%%     `type`, `emqx_authz_rules_count`n
zip_json_auth_metrics(Key, Points, [] = _AccIn) ->
    lists:foldl(
        fun({Lables, Metric}, AccIn2) ->
            LablesKVMap = maps:from_list(Lables),
            Point = (maps:merge(LablesKVMap, users_or_rule_count(LablesKVMap)))#{Key => Metric},
            [Point | AccIn2]
        end,
        [],
        Points
    );
zip_json_auth_metrics(Key, Points, AllResultedAcc) ->
    ThisKeyResult = lists:foldl(emqx_prometheus_utils:point_to_map_fun(Key), [], Points),
    lists:zipwith(fun maps:merge/2, AllResultedAcc, ThisKeyResult).

users_or_rule_count(#{id := Id}) ->
    #{emqx_authn_users_count := Points} = authn_users_count_data(),
    case lists:keyfind([{id, Id}], 1, Points) of
        {_, Metric} ->
            #{emqx_authn_users_count => Metric};
        false ->
            #{}
    end;
users_or_rule_count(#{type := Type}) ->
    #{emqx_authz_rules_count := Points} = authz_rules_count_data(),
    case lists:keyfind([{type, Type}], 1, Points) of
        {_, Metric} ->
            #{emqx_authz_rules_count => Metric};
        false ->
            #{}
    end;
users_or_rule_count(_) ->
    #{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `text/plain`
aggre_cluster(ResL) ->
    emqx_prometheus_utils:aggre_cluster(?LOGICAL_SUM_METRIC_NAMES, ResL, aggre_or_zip_init_acc()).

with_node_name_label(ResL) ->
    emqx_prometheus_utils:with_node_name_label(ResL, aggre_or_zip_init_acc()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper funcs

authenticator_id(Authn) ->
    emqx_authn_chains:authenticator_id(Authn).

auth_data_sample_point(authn, Authn, Tab) ->
    Size = mnesia_size(Tab),
    Id = authenticator_id(Authn),
    {[{id, Id}], Size};
auth_data_sample_point(authz, #{type := Type} = _Authz, Tab) ->
    Size = mnesia_size(Tab),
    {[{type, Type}], Size}.

mnesia_size(Tab) ->
    mnesia:table_info(Tab, size).

do_metric(emqx_authn_enable, #{enable := B}, _) ->
    emqx_prometheus_utils:boolean_to_number(B);
do_metric(K, _, Metrics) ->
    ?MG0(K, Metrics).

aggre_or_zip_init_acc() ->
    #{
        authn => maps:from_keys(authn_metric_names(), []),
        authz => maps:from_keys(authz_metric_names(), [])
    }.
