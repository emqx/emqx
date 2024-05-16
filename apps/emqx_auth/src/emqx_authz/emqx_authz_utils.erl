%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_utils).

-include_lib("emqx_authz.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    cleanup_resources/0,
    make_resource_id/1,
    create_resource/2,
    create_resource/3,
    update_resource/2,
    remove_resource/1,
    update_config/2,
    vars_for_rule_query/2,
    parse_rule_from_row/2
]).

-export([
    parse_http_resp_body/2,
    content_type/1
]).

-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

create_resource(Module, Config) ->
    ResourceId = make_resource_id(Module),
    create_resource(ResourceId, Module, Config).

create_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:create_local(
        ResourceId,
        ?AUTHZ_RESOURCE_GROUP,
        Module,
        Config,
        ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(Result, ResourceId, Config).

update_resource(Module, #{annotations := #{id := ResourceId}} = Config) ->
    Result =
        case
            emqx_resource:recreate_local(
                ResourceId,
                Module,
                Config,
                ?DEFAULT_RESOURCE_OPTS
            )
        of
            {ok, _} -> {ok, ResourceId};
            {error, Reason} -> {error, Reason}
        end,
    start_resource_if_enabled(Result, ResourceId, Config).

remove_resource(ResourceId) ->
    emqx_resource:remove_local(ResourceId).

start_resource_if_enabled({ok, _} = Result, ResourceId, #{enable := true}) ->
    _ = emqx_resource:start(ResourceId),
    Result;
start_resource_if_enabled(Result, _ResourceId, _Config) ->
    Result.

cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)
    ).

make_resource_id(Name) ->
    NameBin = emqx_utils_conv:bin(Name),
    emqx_resource:generate_id(NameBin).

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

-spec parse_http_resp_body(binary(), binary()) -> allow | deny | ignore | error.
parse_http_resp_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    try
        result(maps:from_list(cow_qs:parse_qs(Body)))
    catch
        _:_ -> error
    end;
parse_http_resp_body(<<"application/json", _/binary>>, Body) ->
    try
        result(emqx_utils_json:decode(Body, [return_maps]))
    catch
        _:_ -> error
    end.

result(#{<<"result">> := <<"allow">>}) -> allow;
result(#{<<"result">> := <<"deny">>}) -> deny;
result(#{<<"result">> := <<"ignore">>}) -> ignore;
result(_) -> error.

-spec content_type(cow_http:headers()) -> binary().
content_type(Headers) when is_list(Headers) ->
    %% header name is lower case, see:
    %% https://github.com/ninenines/cowlib/blob/ce6798c6b2e95b6a34c6a76d2489eaf159827d80/src/cow_http.erl#L192
    proplists:get_value(
        <<"content-type">>,
        Headers,
        <<"application/json">>
    ).

-define(RAW_RULE_KEYS, [<<"permission">>, <<"action">>, <<"topic">>, <<"qos">>, <<"retain">>]).

parse_rule_from_row(ColumnNames, Row) ->
    RuleRaw = maps:with(?RAW_RULE_KEYS, maps:from_list(lists:zip(ColumnNames, to_list(Row)))),
    case emqx_authz_rule_raw:parse_rule(RuleRaw) of
        {ok, {Permission, Action, Topics}} ->
            emqx_authz_rule:compile({Permission, all, Action, Topics});
        {error, Reason} ->
            error(Reason)
    end.

vars_for_rule_query(Client, ?authz_action(PubSub, Qos) = Action) ->
    Client#{
        action => PubSub,
        qos => Qos,
        retain => maps:get(retain, Action, false)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

to_list(Tuple) when is_tuple(Tuple) ->
    tuple_to_list(Tuple);
to_list(List) when is_list(List) ->
    List.
