%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_prometheus_config).

-behaviour(emqx_config_handler).

-include("emqx_prometheus.hrl").

-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).
-export([update/1]).
-export([conf/0, is_push_gateway_server_enabled/1]).
-export([to_recommend_type/1]).

-ifdef(TEST).
-export([all_collectors/0]).
-endif.

update(Config) ->
    case
        emqx_conf:update(
            [prometheus],
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

add_handler() ->
    ok = emqx_config_handler:add_handler(?PROMETHEUS, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?PROMETHEUS),
    ok.

%% when we import the config with the old version
%% we need to respect it, and convert to new schema.
pre_config_update(?PROMETHEUS, MergeConf, OriginConf) ->
    OriginType = emqx_prometheus_schema:is_recommend_type(OriginConf),
    MergeType = emqx_prometheus_schema:is_recommend_type(MergeConf),
    {ok,
        case {OriginType, MergeType} of
            {true, false} -> to_recommend_type(MergeConf);
            _ -> MergeConf
        end}.

to_recommend_type(Conf) ->
    #{
        <<"push_gateway">> => to_push_gateway(Conf),
        <<"collectors">> => to_collectors(Conf),
        <<"enable_basic_auth">> => false
    }.

to_push_gateway(Conf) ->
    Init = maps:with([<<"interval">>, <<"headers">>, <<"job_name">>, <<"enable">>], Conf),
    case maps:get(<<"push_gateway_server">>, Conf, "") of
        "" ->
            Init#{<<"enable">> => false};
        Url ->
            Init#{<<"url">> => Url}
    end.

to_collectors(Conf) ->
    lists:foldl(
        fun({From, To}, Acc) ->
            case maps:find(From, Conf) of
                {ok, Value} -> Acc#{To => Value};
                error -> Acc
            end
        end,
        #{},
        [
            {<<"vm_dist_collector">>, <<"vm_dist">>},
            {<<"mnesia_collector">>, <<"mnesia">>},
            {<<"vm_statistics_collector">>, <<"vm_statistics">>},
            {<<"vm_system_info_collector">>, <<"vm_system_info">>},
            {<<"vm_memory_collector">>, <<"vm_memory">>},
            {<<"vm_msacc_collector">>, <<"vm_msacc">>}
        ]
    ).

post_config_update(?PROMETHEUS, _Req, New, Old, AppEnvs) ->
    update_prometheus(AppEnvs),
    _ = update_push_gateway(New),
    update_auth(New, Old);
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

update_prometheus(AppEnvs) ->
    PrevCollectors = all_collectors(),
    CurCollectors = proplists:get_value(collectors, proplists:get_value(prometheus, AppEnvs)),
    lists:foreach(
        fun prometheus_registry:deregister_collector/1,
        PrevCollectors -- CurCollectors
    ),
    lists:foreach(
        fun prometheus_registry:register_collector/1,
        CurCollectors -- PrevCollectors
    ),
    application:set_env(AppEnvs).

all_collectors() ->
    lists:foldl(
        fun(Registry, AccIn) ->
            prometheus_registry:collectors(Registry) ++ AccIn
        end,
        _InitAcc = [],
        ?PROMETHEUS_ALL_REGISTRIES
    ).

update_push_gateway(Prometheus) ->
    case is_push_gateway_server_enabled(Prometheus) of
        true ->
            case erlang:whereis(?APP) of
                undefined -> emqx_prometheus_sup:start_child(?APP, Prometheus);
                Pid -> emqx_prometheus_sup:update_child(Pid, Prometheus)
            end;
        false ->
            emqx_prometheus_sup:stop_child(?APP)
    end.

update_auth(#{enable_basic_auth := New}, #{enable_basic_auth := Old}) when New =/= Old ->
    emqx_dashboard_listener:delay_job(regenerate),
    ok;
update_auth(_, _) ->
    ok.

conf() ->
    emqx_config:get(?PROMETHEUS).

is_push_gateway_server_enabled(#{enable := true, push_gateway_server := Url}) ->
    Url =/= "";
is_push_gateway_server_enabled(#{push_gateway := #{url := Url, enable := Enable}}) ->
    Enable andalso Url =/= "";
is_push_gateway_server_enabled(_) ->
    false.
