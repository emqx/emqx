%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-behaviour(emqx_prometheus_cluster).
-export([
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
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
    | emqx_authz_allow
    | emqx_authz_deny.

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
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    ok = add_collect_family(Callback, authn_metric_meta(), ?MG(authn_data, RawData)),
    ok = add_collect_family(Callback, authn_users_count_metric_meta(), ?MG(authn_users_count_data, RawData)),
    ok = add_collect_family(Callback, authz_metric_meta(), ?MG(authz_data, RawData)),
    ok = add_collect_family(Callback, authz_rules_count_metric_meta(), ?MG(authz_rules_count_data, RawData)),
    ok = add_collect_family(Callback, banned_count_metric_meta(), ?MG(banned_count_data, RawData)),
    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    #{
        emqx_authn => collect_json_data(?MG(authn_data, RawData)),
        emqx_authz => collect_json_data(?MG(authz_data, RawData)),
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

%% behaviour
fetch_from_local_node(Mode) ->
    {node(self()), #{
        authn_data => authn_data(Mode),
        authz_data => authz_data(Mode)
    }}.

fetch_cluster_consistented_data() ->
    #{
        authn_users_count_data => authn_users_count_data(),
        authz_rules_count_data => authz_rules_count_data(),
        banned_count_data => banned_count_data()
    }.

aggre_or_zip_init_acc() ->
    #{
        authn_data => maps:from_keys(authn_metric(names), []),
        authz_data => maps:from_keys(authz_metric(names), [])
    }.

logic_sum_metrics() ->
    [
        emqx_authn_enable,
        emqx_authn_status,
        emqx_authz_enable,
        emqx_authz_status
    ].

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
collect_auth(K = emqx_authz_allow, Data) ->
    counter_metrics(?MG(K, Data));
collect_auth(K = emqx_authz_deny, Data) ->
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

authn_metric_meta() ->
    [
        {emqx_authn_enable, gauge},
        {emqx_authn_status, gauge},
        {emqx_authn_nomatch, counter},
        {emqx_authn_total, counter},
        {emqx_authn_success, counter},
        {emqx_authn_failed, counter}
    ].

authn_metric(names) ->
    emqx_prometheus_cluster:metric_names(authn_metric_meta()).

-spec authn_data(atom()) -> #{Key => [Point]} when
    Key :: authn_metric_name(),
    Point :: {[Label], Metric},
    Label :: IdLabel,
    IdLabel :: {id, AuthnName :: binary()},
    Metric :: number().
authn_data(Mode) ->
    Authns = emqx_config:get([authentication]),
    lists:foldl(
        fun(Key, AccIn) ->
            AccIn#{Key => authn_backend_to_points(Mode, Key, Authns)}
        end,
        #{},
        authn_metric(names)
    ).

-spec authn_backend_to_points(atom(), Key, list(Authn)) -> list(Point) when
    Key :: authn_metric_name(),
    Authn :: map(),
    Point :: {[Label], Metric},
    Label :: IdLabel,
    IdLabel :: {id, AuthnName :: binary()},
    Metric :: number().
authn_backend_to_points(Mode, Key, Authns) ->
    do_authn_backend_to_points(Mode, Key, Authns, []).

do_authn_backend_to_points(_Mode, _K, [], AccIn) ->
    lists:reverse(AccIn);
do_authn_backend_to_points(Mode, K, [Authn | Rest], AccIn) ->
    Id = authenticator_id(Authn),
    Point = {
        with_node_label(Mode, [{id, Id}]),
        do_metric(K, Authn, lookup_authn_metrics_local(Id))
    },
    do_authn_backend_to_points(Mode, K, Rest, [Point | AccIn]).

lookup_authn_metrics_local(Id) ->
    case emqx_authn_api:lookup_from_local_node(?GLOBAL, Id) of
        {ok, {_Node, Status, #{counters := Counters}, _ResourceMetrics}} ->
            #{
                emqx_authn_status => emqx_prometheus_cluster:status_to_number(Status),
                emqx_authn_nomatch => ?MG0(nomatch, Counters),
                emqx_authn_total => ?MG0(total, Counters),
                emqx_authn_success => ?MG0(success, Counters),
                emqx_authn_failed => ?MG0(failed, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authn_metric(names) -- [emqx_authn_enable], 0)
    end.

%%====================
%% Authn users count

authn_users_count_metric_meta() ->
    [
        {emqx_authn_users_count, gauge}
    ].

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

authz_metric_meta() ->
    [
        {emqx_authz_enable, gauge},
        {emqx_authz_status, gauge},
        {emqx_authz_nomatch, counter},
        {emqx_authz_total, counter},
        {emqx_authz_allow, counter},
        {emqx_authz_deny, counter}
    ].

authz_metric(names) ->
    emqx_prometheus_cluster:metric_names(authz_metric_meta()).

-spec authz_data(atom()) -> #{Key => [Point]} when
    Key :: authz_metric_name(),
    Point :: {[Label], Metric},
    Label :: TypeLabel,
    TypeLabel :: {type, AuthZType :: binary()},
    Metric :: number().
authz_data(Mode) ->
    Authzs = emqx_config:get([authorization, sources]),
    lists:foldl(
        fun(Key, AccIn) ->
            AccIn#{Key => authz_backend_to_points(Mode, Key, Authzs)}
        end,
        #{},
        authz_metric(names)
    ).

-spec authz_backend_to_points(atom(), Key, list(Authz)) -> list(Point) when
    Key :: authz_metric_name(),
    Authz :: map(),
    Point :: {[Label], Metric},
    Label :: TypeLabel,
    TypeLabel :: {type, AuthZType :: binary()},
    Metric :: number().
authz_backend_to_points(Mode, Key, Authzs) ->
    do_authz_backend_to_points(Mode, Key, Authzs, []).

do_authz_backend_to_points(_Mode, _K, [], AccIn) ->
    lists:reverse(AccIn);
do_authz_backend_to_points(Mode, K, [Authz | Rest], AccIn) ->
    Type = maps:get(type, Authz),
    Point = {
        with_node_label(Mode, [{type, Type}]),
        do_metric(K, Authz, lookup_authz_metrics_local(Type))
    },
    do_authz_backend_to_points(Mode, K, Rest, [Point | AccIn]).

lookup_authz_metrics_local(Type) ->
    case emqx_authz_api_sources:lookup_from_local_node(Type) of
        {ok, {_Node, Status, #{counters := Counters}, _ResourceMetrics}} ->
            #{
                emqx_authz_status => emqx_prometheus_cluster:status_to_number(Status),
                emqx_authz_nomatch => ?MG0(nomatch, Counters),
                emqx_authz_total => ?MG0(total, Counters),
                emqx_authz_allow => ?MG0(allow, Counters),
                emqx_authz_deny => ?MG0(deny, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authz_metric(names) -- [emqx_authz_enable], 0)
    end.

%%====================
%% Authz rules count

authz_rules_count_metric_meta() ->
    [
        {emqx_authz_rules_count, gauge}
    ].

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

banned_count_metric_meta() ->
    [
        {emqx_banned_count, gauge}
    ].
-define(BANNED_TABLE,
    emqx_banned
).
banned_count_data() ->
    mnesia_size(?BANNED_TABLE).

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`

collect_json_data(Data) ->
    emqx_prometheus_cluster:collect_json_data(Data, fun zip_json_auth_metrics/3).

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
    ThisKeyResult = lists:foldl(emqx_prometheus_cluster:point_to_map_fun(Key), [], Points),
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
    emqx_prometheus_cluster:boolean_to_number(B);
do_metric(emqx_authz_enable, #{enable := B}, _) ->
    emqx_prometheus_cluster:boolean_to_number(B);
do_metric(K, _, Metrics) ->
    ?MG0(K, Metrics).

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node(self())} | Labels].
