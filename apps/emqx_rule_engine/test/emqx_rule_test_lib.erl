%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_test_lib).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% Start Apps
%%------------------------------------------------------------------------------

stop_apps() ->
    stopped = mnesia:stop(),
    [application:stop(App) || App <- [emqx_rule_engine, emqx]].

start_apps() ->
    [start_apps(App, SchemaFile, ConfigFile) ||
        {App, SchemaFile, ConfigFile}
            <- [{emqx, deps_path(emqx, "priv/emqx.schema"),
                       deps_path(emqx, "etc/emqx.conf")},
                {emqx_rule_engine, local_path("priv/emqx_rule_engine.schema"),
                                   local_path("etc/emqx_rule_engine.conf")}]].

%%--------------------------------------
%% start apps helper funcs

start_apps(App, SchemaFile, ConfigFile) ->
    read_schema_configs(App, SchemaFile, ConfigFile),
    set_special_configs(App),
    {ok, _} = application:ensure_all_started(App).

read_schema_configs(App, SchemaFile, ConfigFile) ->
    ct:pal("Read configs - SchemaFile: ~p, ConfigFile: ~p", [SchemaFile, ConfigFile]),
    Schema = cuttlefish_schema:files([SchemaFile]),
    Conf = conf_parse:file(ConfigFile),
    NewConfig = cuttlefish_generator:map(Schema, Conf),
    Vals = proplists:get_value(App, NewConfig, []),
    [application:set_env(App, Par, Value) || {Par, Value} <- Vals].

deps_path(App, RelativePath) ->
    %% Note: not lib_dir because etc dir is not sym-link-ed to _build dir
    %% but priv dir is
    Path0 = code:priv_dir(App),
    Path = case file:read_link(Path0) of
               {ok, Resolved} -> Resolved;
               {error, _} -> Path0
           end,
    filename:join([Path, "..", RelativePath]).

local_path(RelativePath) ->
    deps_path(emqx_rule_engine, RelativePath).

set_special_configs(emqx_rule_engine) ->
    application:set_env(emqx_rule_engine, ignore_sys_message, true),
    application:set_env(emqx_rule_engine, events,
                       [{'client.connected',on,1},
                        {'client.disconnected',on,1},
                        {'session.subscribed',on,1},
                        {'session.unsubscribed',on,1},
                        {'message.acked',on,1},
                        {'message.dropped',on,1},
                        {'message.delivered',on,1}
                       ]),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% rule test helper funcs
%%------------------------------------------------------------------------------

create_simple_repub_rule(TargetTopic, SQL) ->
    create_simple_repub_rule(TargetTopic, SQL, <<"${payload}">>).

create_simple_repub_rule(TargetTopic, SQL, Template) ->
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => SQL,
                      actions => [#{name => 'republish',
                                    args => #{<<"target_topic">> => TargetTopic,
                                              <<"target_qos">> => -1,
                                              <<"payload_tmpl">> => Template}
                                    }],
                      description => <<"simple repub rule">>}),
    Rule.

make_simple_debug_resource_type() ->
    #resource_type{
       name = built_in,
       provider = ?APP,
       params_spec = #{},
       on_create = {?MODULE, on_resource_create},
       on_destroy = {?MODULE, on_resource_destroy},
       on_status = {?MODULE, on_get_resource_status},
       title = #{en => <<"Built-In Resource Type (debug)">>},
       description = #{en => <<"The built in resource type for debug purpose">>}}.

make_simple_resource_type(ResTypeName) ->
    #resource_type{
       name = ResTypeName,
       provider = ?APP,
       params_spec = #{},
       on_create = {?MODULE, on_simple_resource_type_create},
       on_destroy = {?MODULE, on_simple_resource_type_destroy},
       on_status = {?MODULE, on_simple_resource_type_status},
       title = #{en => <<"Simple Resource Type">>},
       description = #{en => <<"Simple Resource Type">>}}.

make_bad_resource_type() ->
    #resource_type{
       name = bad_resource,
       provider = ?APP,
       params_spec = #{},
       on_create = {?MODULE, on_bad_resource_type_create},
       on_destroy = {?MODULE, on_bad_resource_type_destroy},
       on_status = {?MODULE, on_bad_resource_type_status},
       title = #{en => <<"Bad Resource Type">>},
       description = #{en => <<"Bad Resource Type">>}}.

init_events_counters() ->
    ets:new(events_record_tab, [named_table, bag, public]).

%%------------------------------------------------------------------------------
%% rule test helper funcs
%%------------------------------------------------------------------------------

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    Headers = case Auth of
                  no_auth -> [];
                  Header  -> [Header]
              end,
    do_request_api(Method, {NewUrl, Headers});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    Headers = case Auth of
                  no_auth -> [];
                  Header  -> [Header]
              end,
    do_request_api(Method, {NewUrl, Headers, "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    %% ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).

%%------------------------------------------------------------------------------
%% Internal helper funcs
%%------------------------------------------------------------------------------

on_resource_create(_id, _) -> #{}.
on_resource_destroy(_id, _) -> ok.
on_get_resource_status(_id, _) -> #{is_alive => true}.

on_simple_resource_type_create(_Id, #{}) -> #{}.
on_simple_resource_type_destroy(_Id, #{}) -> ok.
on_simple_resource_type_status(_Id, #{}, #{}) -> #{is_alive => true}.

on_bad_resource_type_create(_Id, #{}) -> error(never_get_started).
on_bad_resource_type_destroy(_Id, #{}) -> ok.
on_bad_resource_type_status(_Id, #{}, #{}) -> #{is_alive => false}.
