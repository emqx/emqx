%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_actions_test_utils).
%% ====================================================================
%% API functions
%% ====================================================================
-export([ create_resource/1
        , delete_resource/1
        , create_rule_with_resource/3
        , create_rule/1
        , delete_rule/1
        , create_resource_failed/1
        , create_rules_failed/1
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
%%
-define(API_HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").
%%
%% ====================================================================
%% Http request function
%% ====================================================================

create_resource(SourceOptions) ->
    {ok, R} = request_api(post, api_path(["resources"]), [], auth_header(), SourceOptions),
    % ct:pal("======> Create resource:>~p~nwith options: ~p~n", [R, SourceOptions]),
    0 = get(<<"code">>, R),
    ResourceID = maps:get(<<"id">>, get(<<"data">>, R)),
    ResourceID.

create_rule_with_resource(RuleSQL, #{<<"params">> := Params0} = ActionOptions, ResourceID) ->
    Params = Params0#{<<"$resource">> => ResourceID},
    create_rule(#{<<"rawsql">> => RuleSQL,
                  <<"actions">> => [ActionOptions#{<<"params">> => Params}]}).

create_rule(RulesOptions) ->
    {ok, RuleData} = request_api(post, api_path(["rules"]), [], auth_header(), RulesOptions),
    % ct:pal("======> Create rules:>~p~nwith options: ~p~n", [RuleData, RulesOptions]),
    0 = get(<<"code">>, RuleData),
    RuleID = maps:get(<<"id">>, get(<<"data">>, RuleData)),
    RuleID.

delete_resource(ResourceID) ->
    {ok, _} = request_api(delete, api_path(["resources", binary_to_list(ResourceID)]), auth_header()).

delete_rule(RuleID) ->
    {ok, _} = request_api(delete, api_path(["rules", binary_to_list(RuleID)]), auth_header()).

create_resource_failed(SourceOptions) ->
    {ok, R} = request_api(post, api_path(["resources"]), [], auth_header(), SourceOptions),
    400 = get(<<"code">>, R),
    ct:pal("======> Create resource failed test OK\n").

create_rules_failed(RulesOptions) ->
    {ok, RuleData} = request_api(post, api_path(["rules"]), [], auth_header(), RulesOptions),
    400 = get(<<"code">>, RuleData),
    ct:pal("======> Create rules failed test OK\n").

%%==============================================================================

request_api(Method, Url, Auth) ->
    % ct:pal("API request, Method=> ~p Url~p~n", [Method, Url]),
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    % ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?API_HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).

get(Key, ResponseBody) ->
    maps:get(Key, jiffy:decode(list_to_binary(ResponseBody), [return_maps])).

str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(List) when is_list(List) -> List;
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Int) when is_integer(Int) -> integer_to_list(Int).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> iolist_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
bin(Int) when is_integer(Int) -> integer_to_binary(Int).