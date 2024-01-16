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
    ok = add_collect_family(Callback, ?AUTHNS_WITH_TYPE, authn_data()),
    ok = add_collect_family(Callback, ?AUTHN_USERS_COUNT_WITH_TYPE, authn_users_count_data()),
    ok = add_collect_family(Callback, ?AUTHZS_WITH_TYPE, authz_data()),
    ok = add_collect_family(Callback, ?AUTHZ_RULES_COUNT_WITH_TYPE, authz_rules_count_data()),
    ok = add_collect_family(Callback, ?BANNED_WITH_TYPE, banned_count_data()),
    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    #{
        emqx_authn => collect_auth_data(authn),
        emqx_authz => collect_auth_data(authz),
        emqx_banned => collect_banned_data()
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_AUTH_REGISTRY).

collect_auth_data(AuthDataType) ->
    maps:fold(
        fun(K, V, Acc) ->
            zip_auth_metrics(AuthDataType, K, V, Acc)
        end,
        [],
        auth_data(AuthDataType)
    ).

collect_banned_data() ->
    #{emqx_banned_count => banned_count_data()}.

add_collect_family(Callback, MetricWithType, Data) ->
    _ = [add_collect_family(Name, Data, Callback, Type) || {Name, Type} <- MetricWithType],
    ok.

add_collect_family(Name, Data, Callback, Type) ->
    Callback(create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

collect_metrics(Name, Metrics) ->
    collect_auth(Name, Metrics).

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
                emqx_authn_status => status_to_number(Status),
                emqx_authn_nomatch => ?MG0(nomatch, Counters),
                emqx_authn_total => ?MG0(total, Counters),
                emqx_authn_success => ?MG0(success, Counters),
                emqx_authn_failed => ?MG0(failed, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authn_metric_names() -- [emqx_authn_enable], 0)
    end.

authn_metric_names() ->
    metric_names(?AUTHNS_WITH_TYPE).

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
                emqx_authz_status => status_to_number(Status),
                emqx_authz_nomatch => ?MG0(nomatch, Counters),
                emqx_authz_total => ?MG0(total, Counters),
                emqx_authz_success => ?MG0(success, Counters),
                emqx_authz_failed => ?MG0(failed, Counters)
            };
        {error, _Reason} ->
            maps:from_keys(authz_metric_names() -- [emqx_authz_enable], 0)
    end.

authz_metric_names() ->
    metric_names(?AUTHZS_WITH_TYPE).

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
%% Helper functions
%%--------------------------------------------------------------------

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
    boolean_to_number(B);
do_metric(K, _, Metrics) ->
    ?MG0(K, Metrics).

boolean_to_number(true) -> 1;
boolean_to_number(false) -> 0.

status_to_number(connected) -> 1;
status_to_number(stopped) -> 0.

zip_auth_metrics(AuthDataType, K, V, Acc) ->
    LabelK = label_key(AuthDataType),
    UserOrRuleD = user_rule_data(AuthDataType),
    do_zip_auth_metrics(LabelK, UserOrRuleD, K, V, Acc).

do_zip_auth_metrics(LabelK, UserOrRuleD, Key, Points, [] = _AccIn) ->
    lists:foldl(
        fun({[{K, LabelV}], Metric}, AccIn2) when K =:= LabelK ->
            %% for initialized empty AccIn
            %% The following fields will be put into Result
            %% For Authn:
            %%     `id`, `emqx_authn_users_count`
            %% For Authz:
            %%     `type`, `emqx_authz_rules_count`
            Point = (users_or_rule_count(LabelK, LabelV, UserOrRuleD))#{
                LabelK => LabelV, Key => Metric
            },
            [Point | AccIn2]
        end,
        [],
        Points
    );
do_zip_auth_metrics(LabelK, _UserOrRuleD, Key, Points, AllResultedAcc) ->
    ThisKeyResult = lists:foldl(
        fun({[{K, Id}], Metric}, AccIn2) when K =:= LabelK ->
            [#{LabelK => Id, Key => Metric} | AccIn2]
        end,
        [],
        Points
    ),
    lists:zipwith(
        fun(AllResulted, ThisKeyMetricOut) ->
            maps:merge(AllResulted, ThisKeyMetricOut)
        end,
        AllResultedAcc,
        ThisKeyResult
    ).

auth_data(authn) -> authn_data();
auth_data(authz) -> authz_data().

label_key(authn) -> id;
label_key(authz) -> type.

user_rule_data(authn) -> authn_users_count_data();
user_rule_data(authz) -> authz_rules_count_data().

users_or_rule_count(id, Id, #{emqx_authn_users_count := Points} = _AuthnUsersD) ->
    case lists:keyfind([{id, Id}], 1, Points) of
        {_, Metric} ->
            #{emqx_authn_users_count => Metric};
        false ->
            #{}
    end;
users_or_rule_count(type, Type, #{emqx_authz_rules_count := Points} = _AuthzRulesD) ->
    case lists:keyfind([{type, Type}], 1, Points) of
        {_, Metric} ->
            #{emqx_authz_rules_count => Metric};
        false ->
            #{}
    end;
users_or_rule_count(_, _, _) ->
    #{}.

metric_names(MetricWithType) when is_list(MetricWithType) ->
    [Name || {Name, _Type} <- MetricWithType].
