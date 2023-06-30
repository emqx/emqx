%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_api).

-behaviour(gen_server).

-include("rule_engine.hrl").

-logger_header("[RuleEngineAPI]").

-import(minirest,  [return/1]).

-rest_api(#{name   => create_rule,
            method => 'POST',
            path   => "/rules/",
            func   => create_rule,
            descr  => "Create a rule"
           }).

-rest_api(#{name   => update_rule,
            method => 'PUT',
            path   => "/rules/:bin:id",
            func   => update_rule,
            descr  => "Update a rule"
           }).

-rest_api(#{name   => list_rules,
            method => 'GET',
            path   => "/rules/",
            func   => list_rules,
            descr  => "A list of all rules"
           }).

-rest_api(#{name   => show_rule,
            method => 'GET',
            path   => "/rules/:bin:id",
            func   => show_rule,
            descr  => "Show a rule"
           }).

-rest_api(#{name   => delete_rule,
            method => 'DELETE',
            path   => "/rules/:bin:id",
            func   => delete_rule,
            descr  => "Delete a rule"
           }).

-rest_api(#{name   => reset_metrics,
            method => 'PUT',
            path   => "/rules/:bin:id/reset_metrics",
            func   => reset_metrics,
            descr  => "reset a rule metrics"
           }).

-rest_api(#{name   => list_actions,
            method => 'GET',
            path   => "/actions/",
            func   => list_actions,
            descr  => "A list of all actions"
           }).

-rest_api(#{name   => show_action,
            method => 'GET',
            path   => "/actions/:atom:name",
            func   => show_action,
            descr  => "Show an action"
           }).

-rest_api(#{name   => list_resources,
            method => 'GET',
            path   => "/resources/",
            func   => list_resources,
            descr  => "A list of all resources"
           }).

-rest_api(#{name   => create_resource,
            method => 'POST',
            path   => "/resources/",
            func   => create_resource,
            descr  => "Create a resource"
           }).

-rest_api(#{name   => update_resource,
            method => 'PUT',
            path   => "/resources/:bin:id",
            func   => update_resource,
            descr  => "Update a resource"
           }).

-rest_api(#{name   => show_resource,
            method => 'GET',
            path   => "/resources/:bin:id",
            func   => show_resource,
            descr  => "Show a resource"
           }).

-rest_api(#{name   => get_resource_status,
            method => 'GET',
            path   => "/resource_status/:bin:id",
            func   => get_resource_status,
            descr  => "Get status of a resource"
           }).

-rest_api(#{name   => start_resource,
            method => 'POST',
            path   => "/resources/:bin:id",
            func   => start_resource,
            descr  => "Start a resource"
           }).

-rest_api(#{name   => delete_resource,
            method => 'DELETE',
            path   => "/resources/:bin:id",
            func   => delete_resource,
            descr  => "Delete a resource"
           }).

-rest_api(#{name   => list_resource_types,
            method => 'GET',
            path   => "/resource_types/",
            func   => list_resource_types,
            descr  => "List all resource types"
           }).

-rest_api(#{name   => show_resource_type,
            method => 'GET',
            path   => "/resource_types/:atom:name",
            func   => show_resource_type,
            descr  => "Show a resource type"
           }).

-rest_api(#{name   => list_resources_by_type,
            method => 'GET',
            path   => "/resource_types/:atom:type/resources",
            func   => list_resources_by_type,
            descr  => "List all resources of a resource type"
           }).

-rest_api(#{name   => list_events,
            method => 'GET',
            path   => "/rule_events/",
            func   => list_events,
            descr  => "List all events with detailed info"
           }).

-export([start_link/0]).

% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ create_rule/2
        , update_rule/2
        , list_rules/2
        , show_rule/2
        , delete_rule/2
        , reset_metrics/2
        , reset_metrics_local/1
        ]).

-export([ list_actions/2
        , show_action/2
        ]).

-export([ create_resource/2
        , list_resources/2
        , show_resource/2
        , get_resource_status/2
        , start_resource/2
        , delete_resource/2
        , update_resource/2
        ]).

-export([ list_resource_types/2
        , list_resources_by_type/2
        , show_resource_type/2
        ]).

-export([list_events/2]).
-export([query/3]).

-define(RULE_QS_SCHEMA, {?RULE_TAB,
    [
        {<<"enabled">>, atom},
        {<<"for">>, binary},
        {<<"_like_id">>, binary},
        {<<"_like_for">>, binary},
        {<<"_match_for">>, binary},
        {<<"_like_description">>, binary}
    ]}).

-define(ERR_NO_RULE(ID), list_to_binary(io_lib:format("Rule ~s Not Found", [(ID)]))).
-define(ERR_NO_ACTION(NAME), list_to_binary(io_lib:format("Action ~s Not Found", [(NAME)]))).
-define(ERR_NO_RESOURCE(RESID), list_to_binary(io_lib:format("Resource ~s Not Found", [(RESID)]))).
-define(ERR_NO_RESOURCE_TYPE(TYPE), list_to_binary(io_lib:format("Resource Type ~s Not Found", [(TYPE)]))).
-define(ERR_DEP_RULES_EXISTS(RULEIDS), list_to_binary(io_lib:format("Found rules ~0p depends on this resource, disable them first", [(RULEIDS)]))).
-define(ERR_BADARGS(REASON),
        begin
            R0 = list_to_binary(io_lib:format("~0p", [REASON])),
            <<"Bad Arguments: ", R0/binary>>
        end).

-define(T_CALL, 30000).

start_link() ->
    %% The caller process (the cowboy process serves the HTTP request) may times out and dies
    %% before some time-consuming operations complete, e.g. creating rules/resources or testing
    %% the connectivity on unreachable resources.
    %% To avoid this problem, we delegate the operations to a gen_server.
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_rule(_Bindings, Params) ->
    delegate_call({create_rule, _Bindings, Params}).

update_rule(_Bindings, Params) ->
    delegate_call({update_rule, _Bindings, Params}).

delete_rule(_Bindings, Params) ->
    delegate_call({delete_rule, _Bindings, Params}).

create_resource(_Bindings, Params) ->
    delegate_call({create_resource, _Bindings, Params}).

update_resource(_Bindings, Params) ->
    delegate_call({update_resource, _Bindings, Params}).

start_resource(_Bindings, Params) ->
    delegate_call({start_resource, _Bindings, Params}).

delete_resource(_Bindings, Params) ->
    delegate_call({delete_resource, _Bindings, Params}).

%% delegate API calls to a single process.
delegate_call(Req) ->
    gen_server:call(?MODULE, Req, ?T_CALL).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call({create_rule, _Bindings, Params}, _From, State) ->
    {reply, delegate_create_rule(_Bindings, Params), State};

handle_call({update_rule, _Bindings, Params}, _From, State) ->
    {reply, delegate_update_rule(_Bindings, Params), State};

handle_call({delete_rule, _Bindings, Params}, _From, State) ->
    {reply, delegate_delete_rule(_Bindings, Params), State};

handle_call({create_resource, _Bindings, Params}, _From, State) ->
    {reply, delegate_create_resource(_Bindings, Params), State};

handle_call({start_resource, _Bindings, Params}, _From, State) ->
    {reply, delegate_start_resource(_Bindings, Params), State};

handle_call({update_resource, _Bindings, Params}, _From, State) ->
    {reply, delegate_update_resource(_Bindings, Params), State};

handle_call({delete_resource, _Bindings, Params}, _From, State) ->
    {reply, delegate_delete_resource(_Bindings, Params), State};

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Rules API
%%------------------------------------------------------------------------------
delegate_create_rule(_Bindings, Params) ->
    if_test(fun() -> test_rule_sql(Params) end,
            fun() -> do_create_rule(Params) end,
            Params).

test_rule_sql(Params) ->
    case emqx_rule_sqltester:test(emqx_json:decode(emqx_json:encode(Params), [return_maps])) of
        {ok, Result} -> return({ok, Result});
        {error, nomatch} -> return({error, 404, <<"SQL Not Match">>});
        {error, Reason} ->
            ?LOG(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

do_create_rule(Params) ->
    case parse_rule_params(Params) of
        {ok, ParsedParams} ->
            case maps:find(id, ParsedParams) of
                {ok, RuleId} ->
                    case emqx_rule_registry:get_rule(RuleId) of
                        {ok, _} -> return({error, 400, <<"Already Exists">>});
                        not_found -> do_create_rule2(ParsedParams)
                    end;
                error -> do_create_rule2(ParsedParams)
            end;
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

do_create_rule2(ParsedParams) ->
    case emqx_rule_engine:create_rule(ParsedParams) of
        {ok, Rule} -> return({ok, record_to_map(Rule)});
        {error, {action_not_found, ActionName}} ->
            return({error, 400, ?ERR_NO_ACTION(ActionName)});
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

delegate_update_rule(#{id := Id0}, Params) ->
    Id = urldecode(Id0),
    case parse_rule_params(Params, #{id => Id}) of
        {ok, ParsedParams} ->
            case emqx_rule_engine:update_rule(ParsedParams) of
                {ok, Rule} -> return({ok, record_to_map(Rule)});
                {error, {not_found, RuleId}} ->
                    return({error, 400, ?ERR_NO_RULE(RuleId)});
                {error, Reason} ->
                    ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
                    return({error, 400, ?ERR_BADARGS(Reason)})
            end;
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

list_rules(_Bindings, Params) ->
    case proplists:get_value(<<"enable_paging">>, Params, true) of
        true ->
            SortFun = fun(#{created_at := C1}, #{created_at := C2}) -> C1 > C2 end,
            return({ok, emqx_mgmt_api:node_query(node(), Params, ?RULE_QS_SCHEMA, {?MODULE, query}, SortFun)});
        false ->
            return_all(emqx_rule_registry:get_rules_ordered_by_ts())
    end.

show_rule(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    reply_with(fun emqx_rule_registry:get_rule/1, Id).

delegate_delete_rule(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    ok = emqx_rule_engine:delete_rule(Id),
    return(ok).

reset_metrics_local(Id0) ->
    Id = urldecode(Id0),
    emqx_rule_metrics:reset_metrics(Id).

reset_metrics(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    _ = ?CLUSTER_CALL(reset_metrics_local, [Id]),
    return(ok).

%%------------------------------------------------------------------------------
%% Actions API
%%------------------------------------------------------------------------------

list_actions(#{}, _Params) ->
    return_all(
        sort_by_title(action,
            emqx_rule_registry:get_actions())).

show_action(#{name := Name}, _Params) ->
    reply_with(fun emqx_rule_registry:find_action/1, Name).

%%------------------------------------------------------------------------------
%% Resources API
%%------------------------------------------------------------------------------
delegate_create_resource(#{}, Params) ->
    case parse_resource_params(Params) of
        {ok, ParsedParams} ->
            if_test(fun() -> do_create_resource(test_resource, maps:without([id], ParsedParams)) end,
                    fun() -> do_create_resource(create_resource, ParsedParams) end,
                    Params);
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

do_create_resource(Create, ParsedParams) ->
    case maps:find(id, ParsedParams) of
        {ok, ResId} ->
            case emqx_rule_registry:find_resource(ResId) of
                {ok, _} -> return({error, 400, <<"Already Exists">>});
                not_found -> do_create_resource2(Create, ParsedParams)
            end;
        error -> do_create_resource2(Create, ParsedParams)
    end.

do_create_resource2(Create, ParsedParams) ->
    case emqx_rule_engine:Create(ParsedParams) of
        ok ->
            return(ok);
        {ok, Resource} ->
            return({ok, record_to_map(Resource)});
        {error, {resource_type_not_found, Type}} ->
            return({error, 400, ?ERR_NO_RESOURCE_TYPE(Type)});
        {error, {init_resource, _}} ->
            return({error, 500, <<"Init resource failure!">>});
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

list_resources(#{}, _Params) ->
    Data0 = lists:foldr(fun maybe_record_to_map/2, [], emqx_rule_registry:get_resources()),
    Data = lists:map(fun(Res = #{id := ResId}) ->
               Status = emqx_rule_engine:is_resource_alive(ResId),
               maps:put(status, Status, Res)
           end, Data0),
    return({ok, Data}).

list_resources_by_type(#{type := Type}, _Params) ->
    return_all(emqx_rule_registry:get_resources_by_type(Type)).

show_resource(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    case emqx_rule_registry:find_resource(Id) of
        {ok, R} ->
            StatusFun =
                fun(Node) ->
                    #{
                        node => Node,
                        is_alive => emqx_rule_engine:is_resource_alive(Node, Id, #{fetch => false})
                    }
                end,
            Status = [StatusFun(Node) || Node <- ekka_mnesia:running_nodes()],
            return({ok, maps:put(status, Status, record_to_map(R))});
        not_found ->
            return({error, 404, <<"Not Found">>})
    end.

get_resource_status(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    case emqx_rule_engine:get_resource_status(Id) of
        {ok, Status} ->
            return({ok, Status});
        {error, resource_not_initialized} ->
            return({error, 400, ?ERR_NO_RESOURCE(Id)})
    end.

delegate_start_resource(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    case emqx_rule_engine:start_resource(Id) of
        ok ->
            return(ok);
        {error, {resource_not_found, ResId}} ->
            return({error, 400, ?ERR_NO_RESOURCE(ResId)});
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "~p failed: ~0p", [?FUNCTION_NAME, Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

delegate_update_resource(#{id := Id0}, NewParams) ->
    Id = urldecode(Id0),
    P1 = case proplists:get_value(<<"description">>, NewParams) of
        undefined -> #{};
        Value -> #{<<"description">> => Value}
    end,
    P2 = case proplists:get_value(<<"config">>, NewParams) of
        undefined -> #{};
        [{}] -> #{};
        Config -> #{<<"config">> => ?RAISE(json_term_to_map(Config), {invalid_config, Config})}
    end,
    case emqx_rule_engine:update_resource(Id, maps:merge(P1, P2)) of
        ok ->
            return(ok);
        {error, not_found} ->
            return({error, 400, <<"Resource not found:", Id/binary>>});
        {error, {init_resource, _}} ->
            return({error, 500, <<"Init resource failure:", Id/binary>>});
        {error, {dependent_rules_exists, RuleIds}} ->
            return({error, 400, ?ERR_DEP_RULES_EXISTS(RuleIds)});
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "Resource update failed: ~0p", [Reason]),
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

delegate_delete_resource(#{id := Id0}, _Params) ->
    Id = urldecode(Id0),
    case emqx_rule_engine:delete_resource(Id) of
        ok -> return(ok);
        {error, not_found} -> return(ok);
        {error, {dependent_rules_exists, RuleIds}} ->
            return({error, 400, ?ERR_DEP_RULES_EXISTS(RuleIds)});
        {error, Reason} ->
            return({error, 400, ?ERR_BADARGS(Reason)})
    end.

%%------------------------------------------------------------------------------
%% Resource Types API
%%------------------------------------------------------------------------------

list_resource_types(#{}, _Params) ->
    return_all(
        sort_by_title(resource_type,
            emqx_rule_registry:get_resource_types())).

show_resource_type(#{name := Name}, _Params) ->
    reply_with(fun emqx_rule_registry:find_resource_type/1, Name).


%%------------------------------------------------------------------------------
%% Events API
%%------------------------------------------------------------------------------

list_events(#{}, _Params) ->
    return({ok, emqx_rule_events:event_info()}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

if_test(True, False, Params) ->
    case proplists:get_value(<<"test">>, Params) of
        Test when Test =:= true; Test =:= <<"true">> ->
            True();
        _ ->
            False()
    end.

return_all(Records) ->
    Data = lists:foldr(fun maybe_record_to_map/2, [], Records),
    return({ok, Data}).

maybe_record_to_map(Rec, Acc) ->
    case record_to_map(Rec) of
        ignore -> Acc;
        Map -> [Map | Acc]
    end.

reply_with(Find, Key) ->
    case Find(Key) of
        {ok, R} ->
            return({ok, record_to_map(R)});
        not_found ->
            return({error, 404, <<"Not Found">>})
    end.

record_to_map(#rule{id = Id,
                    for = Hook,
                    rawsql = RawSQL,
                    actions = Actions,
                    on_action_failed = OnFailed,
                    enabled = Enabled,
                    created_at = CreatedAt,
                    description = Descr}) ->
    #{id => Id,
      for => Hook,
      rawsql => RawSQL,
      actions => printable_actions(Actions),
      on_action_failed => OnFailed,
      metrics => get_rule_metrics(Id),
      enabled => Enabled,
      created_at => CreatedAt,
      description => Descr
     };

record_to_map(#action{hidden = true}) ->
    ignore;
record_to_map(#action{name = Name,
                      category = Category,
                      app = App,
                      for = Hook,
                      types = Types,
                      params_spec = Params,
                      title = Title,
                      description = Descr}) ->
    #{name => Name,
      category => Category,
      app => App,
      for => Hook,
      types => Types,
      params => Params,
      title => Title,
      description => Descr
     };

record_to_map(#resource{id = Id,
                        type = Type,
                        config = Config,
                        description = Descr}) ->
    #{id => Id,
      type => Type,
      config => Config,
      description => Descr
     };

record_to_map(#resource_type{name = Name,
                             provider = Provider,
                             params_spec = Params,
                             title = Title,
                             description = Descr}) ->
    #{name => Name,
      provider => Provider,
      params => Params,
      title => Title,
      description => Descr
     }.

printable_actions(Actions) ->
    [#{id => Id, name => Name, params => Args,
       metrics => get_action_metrics(Id),
       fallbacks => printable_actions(Fallbacks)}
     || #action_instance{id = Id, name = Name, args = Args, fallbacks = Fallbacks} <- Actions].

parse_rule_params(Params) ->
    parse_rule_params(Params, #{description => <<"">>}).
parse_rule_params([], Rule) ->
    {ok, Rule};
parse_rule_params([{<<"id">>, <<>>} | _], _) ->
    {error, {empty_string_not_allowed, id}};
parse_rule_params([{<<"id">>, Id} | Params], Rule) ->
    parse_rule_params(Params, Rule#{id => Id});
parse_rule_params([{<<"rawsql">>, RawSQL} | Params], Rule) ->
    parse_rule_params(Params, Rule#{rawsql => RawSQL});
parse_rule_params([{<<"enabled">>, Enabled} | Params], Rule) ->
    parse_rule_params(Params, Rule#{enabled => enabled(Enabled)});
parse_rule_params([{<<"on_action_failed">>, OnFailed} | Params], Rule) ->
    parse_rule_params(Params, Rule#{on_action_failed => on_failed(OnFailed)});
parse_rule_params([{<<"actions">>, Actions} | Params], Rule) ->
    parse_rule_params(Params, Rule#{actions => parse_actions(Actions)});
parse_rule_params([{<<"description">>, Descr} | Params], Rule) ->
    parse_rule_params(Params, Rule#{description => Descr});
parse_rule_params([_ | Params], Rule) ->
    parse_rule_params(Params, Rule).

on_failed(<<"continue">>) -> continue;
on_failed(<<"stop">>) -> stop;
on_failed(OnFailed) -> error({invalid_on_failed, OnFailed}).

enabled(Enabled) when is_boolean(Enabled) -> Enabled;
enabled(Enabled) -> error({invalid_enabled, Enabled}).

parse_actions(Actions) ->
    [parse_action(json_term_to_map(A)) || A <- Actions].

parse_action(Action) ->
    #{name => binary_to_existing_atom(maps:get(<<"name">>, Action), utf8),
      args => maps:get(<<"params">>, Action, #{}),
      fallbacks => parse_actions(maps:get(<<"fallbacks">>, Action, []))}.

parse_resource_params(Params) ->
    parse_resource_params(Params, #{config => #{}, description => <<"">>}).
parse_resource_params([], Res) ->
    {ok, Res};
parse_resource_params([{<<"id">>, <<>>} | _], _Res) ->
    {error, {empty_string_not_allowed, id}};
parse_resource_params([{<<"id">>, Id} | Params], Res) ->
    parse_resource_params(Params, Res#{id => Id});
parse_resource_params([{<<"type">>, ResourceType} | Params], Res) ->
    try parse_resource_params(Params, Res#{type => binary_to_existing_atom(ResourceType, utf8)})
    catch error:badarg ->
        {error, {resource_type_not_found, ResourceType}}
    end;
parse_resource_params([{<<"config">>, Config} | Params], Res) ->
    parse_resource_params(Params, Res#{config => json_term_to_map(Config)});
parse_resource_params([{<<"description">>, Descr} | Params], Res) ->
    parse_resource_params(Params, Res#{description => Descr});
parse_resource_params([_ | Params], Res) ->
    parse_resource_params(Params, Res).

json_term_to_map(List) ->
    emqx_json:decode(emqx_json:encode(List), [return_maps]).

sort_by_title(action, Actions) ->
    sort_by(#action.title, Actions);
sort_by_title(resource_type, ResourceTypes) ->
    sort_by(#resource_type.title, ResourceTypes).

sort_by(Pos, TplList) ->
    lists:sort(
        fun(RecA, RecB) ->
            maps:get(en, element(Pos, RecA), 0)
            =< maps:get(en, element(Pos, RecB), 0)
        end, TplList).

get_rule_metrics(Id) ->
    lists:concat(
      [ case rpc:call(Node, emqx_rule_metrics, get_rule_metrics, [Id]) of
            {badrpc, _} -> [];
            Res -> [maps:put(node, Node, Res)]
        end
        || Node <- ekka_mnesia:running_nodes()]).

get_action_metrics(Id) ->
    lists:concat(
      [ case rpc:call(Node, emqx_rule_metrics, get_action_metrics, [Id]) of
            {badrpc, _} -> [];
            Res -> [maps:put(node, Node, Res)]
        end
        || Node <- ekka_mnesia:running_nodes()]).

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(?RULE_TAB, Ms, Start, Limit, fun record_to_map/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(?RULE_TAB, MatchFun, Start, Limit, fun record_to_map/1).

qs2ms(Qs) ->
    Init = #rule{for = '_', enabled = '_', _ = '_'},
    MatchHead = lists:foldl(fun(Q, Acc) ->  match_ms(Q, Acc) end, Init, Qs),
    [{MatchHead, [], ['$_']}].

match_ms({for, '=:=', Value}, MatchHead) -> MatchHead#rule{for = Value};
match_ms({enabled, '=:=', Value}, MatchHead) -> MatchHead#rule{enabled = Value};
match_ms(_, MatchHead) -> MatchHead.

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    fun(Rows) ->
        Ls = ets:match_spec_run(Rows, MsC),
        lists:filter(fun(E) -> run_fuzzy_match(E, Fuzzy) end, Ls)
    end.

run_fuzzy_match(_, []) -> true;
run_fuzzy_match(E = #rule{id = Id}, [{id, like, Pattern}|Fuzzy]) ->
    binary:match(Id, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = #rule{description = Desc}, [{description, like, Pattern}|Fuzzy]) ->
    binary:match(Desc, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = #rule{for = Topics}, [{for, match, Pattern}|Fuzzy]) ->
    lists:any(fun(For) -> emqx_topic:match(For, Pattern) end, Topics)
        andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = #rule{for = Topics}, [{for, like, Pattern}|Fuzzy]) ->
    lists:any(fun(For) -> binary:match(For, Pattern) /= nomatch end, Topics)
        andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(_E, [{_Key, like, _SubStr}| _Fuzzy]) -> false.

urldecode(S) ->
    emqx_http_lib:uri_decode(S).
