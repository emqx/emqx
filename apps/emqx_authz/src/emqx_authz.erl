%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz).
-behaviour(emqx_config_handler).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([ register_metrics/0
        , init/0
        , lookup/0
        , lookup/1
        , move/2
        , update/2
        , authorize/5
        ]).

-export([post_config_update/3, pre_config_update/2]).

-define(CONF_KEY_PATH, [authorization_rules, rules]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTHZ_METRICS).

init() ->
    ok = register_metrics(),
    emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    NRules = [init_provider(Rule) || Rule <- emqx:get_config(?CONF_KEY_PATH, [])],
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NRules]}, -1).

lookup() ->
    {_M, _F, [A]}= find_action_in_hooks(),
    A.
lookup(Id) ->
    try find_rule_by_id(Id, lookup()) of
        {_, Rule} -> Rule
    catch
        error:Reason -> {error, Reason}
    end.

move(Id, Position) ->
    emqx:update_config(?CONF_KEY_PATH, {move, Id, Position}).

update(Cmd, Rules) ->
    emqx:update_config(?CONF_KEY_PATH, {Cmd, Rules}).

pre_config_update({move, Id, <<"top">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_rule_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, [lists:nth(Index, Conf)] ++ lists:droplast(List1) ++ List2};

pre_config_update({move, Id, <<"bottom">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_rule_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, lists:droplast(List1) ++ List2 ++ [lists:nth(Index, Conf)]};

pre_config_update({move, Id, #{<<"before">> := BeforeId}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_rule_by_id(Id),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_rule_by_id(BeforeId),
    Conf2 = lists:nth(Index2, Conf),

    {List1, List2} = lists:split(Index2, Conf),
    {ok, lists:delete(Conf1, lists:droplast(List1))
        ++ [Conf1] ++ [Conf2]
        ++ lists:delete(Conf1, List2)};

pre_config_update({move, Id, #{<<"after">> := AfterId}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_rule_by_id(Id),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_rule_by_id(AfterId),

    {List1, List2} = lists:split(Index2, Conf),
    {ok, lists:delete(Conf1, List1)
        ++ [Conf1]
        ++ lists:delete(Conf1, List2)};

pre_config_update({head, Rules}, Conf) when is_list(Rules), is_list(Conf) ->
    {ok, Rules ++ Conf};
pre_config_update({tail, Rules}, Conf) when is_list(Rules), is_list(Conf) ->
    {ok, Conf ++ Rules};
pre_config_update({{replace_once, Id}, Rule}, Conf) when is_map(Rule), is_list(Conf) ->
    {Index, _} = find_rule_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, lists:droplast(List1) ++ [Rule] ++ List2};
pre_config_update({_, Rules}, _Conf) when is_list(Rules)->
    %% overwrite the entire config!
    {ok, Rules}.

post_config_update(_, undefined, _Conf) ->
    ok;
post_config_update({move, Id, <<"top">>}, _NewRules, _OldRules) ->
    InitedRules = lookup(),
    {Index, Rule} = find_rule_by_id(Id, InitedRules),
    {Rules1, Rules2 } = lists:split(Index, InitedRules),
    Rules3 = [Rule] ++ lists:droplast(Rules1) ++ Rules2,
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Rules3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Id, <<"bottom">>}, _NewRules, _OldRules) ->
    InitedRules = lookup(),
    {Index, Rule} = find_rule_by_id(Id, InitedRules),
    {Rules1, Rules2 } = lists:split(Index, InitedRules),
    Rules3 = lists:droplast(Rules1) ++ Rules2 ++ [Rule],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Rules3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Id, #{<<"before">> := BeforeId}}, _NewRules, _OldRules) ->
    InitedRules = lookup(),
    {_, Rule0} = find_rule_by_id(Id, InitedRules),
    {Index, Rule1} = find_rule_by_id(BeforeId, InitedRules),
    {Rules1, Rules2} = lists:split(Index, InitedRules),
    Rules3 = lists:delete(Rule0, lists:droplast(Rules1))
             ++ [Rule0] ++ [Rule1]
             ++ lists:delete(Rule0, Rules2),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Rules3]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({move, Id, #{<<"after">> := AfterId}}, _NewRules, _OldRules) ->
    InitedRules = lookup(),
    {_, Rule} = find_rule_by_id(Id, InitedRules),
    {Index, _} = find_rule_by_id(AfterId, InitedRules),
    {Rules1, Rules2} = lists:split(Index, InitedRules),
    Rules3 = lists:delete(Rule, Rules1)
             ++ [Rule]
             ++ lists:delete(Rule, Rules2),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Rules3]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({head, Rules}, _NewRules, _OldConf) ->
    InitedRules = [init_provider(R) || R <- check_rules(Rules)],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedRules ++ lookup()]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({tail, Rules}, _NewRules, _OldConf) ->
    InitedRules = [init_provider(R) || R <- check_rules(Rules)],
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [lookup() ++ InitedRules]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({{replace_once, Id}, Rule}, _NewRules, _OldConf) when is_map(Rule) ->
    OldInitedRules = lookup(),
    {Index, OldRule} = find_rule_by_id(Id, OldInitedRules),
    case maps:get(type, OldRule, undefined) of
       undefined -> ok;
       _ ->
            #{annotations := #{id := Id}} = OldRule,
            ok = emqx_resource:remove(Id)
    end,
    {OldRules1, OldRules2 } = lists:split(Index, OldInitedRules),
    InitedRules = [init_provider(R#{annotations => #{id => Id}}) || R <- check_rules([Rule])],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [lists:droplast(OldRules1) ++ InitedRules ++ OldRules2]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update(_, NewRules, _OldConf) ->
    %% overwrite the entire config!
    OldInitedRules = lookup(),
    InitedRules = [init_provider(Rule) || Rule <- NewRules],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedRules]}, -1),
    lists:foreach(fun (#{type := _Type, enable := true, annotations := #{id := Id}}) ->
                         ok = emqx_resource:remove(Id);
                      (_) -> ok
                  end, OldInitedRules),
    ok = emqx_authz_cache:drain_cache().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_rules(RawRules) ->
    {ok, Conf} = hocon:binary(jsx:encode(#{<<"authorization_rules">> => #{<<"rules">> => RawRules}}), #{format => richmap}),
    CheckConf = hocon_schema:check(emqx_authz_schema, Conf, #{atom_key => true}),
    #{authorization_rules := #{rules := Rules}} = hocon_schema:richmap_to_map(CheckConf),
    Rules.

find_rule_by_id(Id) -> find_rule_by_id(Id, lookup()).
find_rule_by_id(Id, Rules) -> find_rule_by_id(Id, Rules, 1).
find_rule_by_id(_RuleId, [], _N) -> error(not_found_rule);
find_rule_by_id(RuleId, [ Rule = #{annotations := #{id := Id}} | Tail], N) ->
    case RuleId =:= Id of
        true -> {N, Rule};
        false -> find_rule_by_id(RuleId, Tail, N + 1)
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

gen_id(Type) ->
    iolist_to_binary([io_lib:format("~s_~s",[?APP, Type]), "_", integer_to_list(erlang:system_time())]).

create_resource(#{type := DB,
                  config := Config,
                  annotations := #{id := ResourceID}}) ->
    case emqx_resource:update(
            ResourceID,
            list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
            Config,
            [])
    of
        {ok, _} -> ResourceID;
        {error, already_created} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end;
create_resource(#{type := DB,
                  config := Config}) ->
    ResourceID = gen_id(DB),
    case emqx_resource:create(
            ResourceID,
            list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
            Config)
    of
        {ok, _} -> ResourceID;
        {error, already_created} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end.

-spec(init_provider(rule()) -> rule()).
init_provider(#{enable := true,
                type := file,
                path := Path
               } = Rule) ->
    Rules = case file:consult(Path) of
                {ok, Terms} ->
                    [emqx_authz_rule:compile(Term) || Term <- Terms];
                {error, eacces} ->
                    ?LOG(alert, "Insufficient permissions to read the ~s file", [Path]),
                    error(eaccess);
                {error, enoent} ->
                    ?LOG(alert, "The ~s file does not exist", [Path]),
                    error(enoent);
                {error, Reason} ->
                    ?LOG(alert, "Failed to read ~s: ~p", [Path, Reason]),
                    error(Reason)
            end,
    Rule#{annotations =>
            #{id => gen_id(file),
              rules => Rules
         }};
init_provider(#{enable := true,
                type := http,
                config := #{url := Url} = Config
               } = Rule) ->
    NConfig = maps:merge(Config, #{base_url => maps:remove(query, Url)}),
    case create_resource(Rule#{config := NConfig}) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Rule#{annotations =>
                      #{id => Id}
                   }
    end;
init_provider(#{enable := true,
                type := DB
               } = Rule) when DB =:= redis;
                              DB =:= mongo ->
    case create_resource(Rule) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Rule#{annotations =>
                      #{id => Id}
                   }
    end;
init_provider(#{enable := true,
                type := DB,
                sql := SQL
               } = Rule) when DB =:= mysql;
                              DB =:= pgsql ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[?APP, DB])),
    case create_resource(Rule) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Rule#{annotations =>
                      #{id => Id,
                        sql => Mod:parse_query(SQL)
                       }
                   }
    end;
init_provider(#{enable := false} = Rule) ->Rule.

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize(emqx_types:clientinfo(), emqx_types:all(), emqx_topic:topic(), allow | deny, rules())
      -> {stop, allow} | {ok, deny}).
authorize(#{username := Username,
            peerhost := IpAddress
           } = Client, PubSub, Topic, _DefaultResult, Rules) ->
    case do_authorize(Client, PubSub, Topic, Rules) of
        {matched, allow} ->
            ?LOG(info, "Client succeeded authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: allow", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(allow)),
            {stop, allow};
        {matched, deny} ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: deny", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(deny)),
            {stop, deny};
        nomatch ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Reasion: ~p", [Username, IpAddress, Topic, "no-match rule"]),
            {stop, deny}
    end.

do_authorize(Client, PubSub, Topic,
               [#{type := file,
                  enable := true,
                  annotations := #{rule := Rules}
                 } | Tail] ) ->
    case emqx_authz_rule:match(Client, PubSub, Topic, Rules) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end;
do_authorize(Client, PubSub, Topic,
               [Connector = #{type := Type,
                              enable := true
                             } | Tail] ) ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[emqx_authz, Type])),
    case Mod:authorize(Client, PubSub, Topic, Connector) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end;
do_authorize(_Client, _PubSub, _Topic, []) -> nomatch.
