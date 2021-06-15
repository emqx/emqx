-module(emqx_authz_resource).

-include("emqx_authz.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

-emqx_resource_api_path("/authz").

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_health_check/2
        ]).

%% callbacks for emqx_resource config schema
-export([fields/1]).
-export([check_authz/4]).

fields(ConfPath) ->
    emqx_authz_schema:fields(ConfPath).

on_start(_InstId, Config) ->
    io:format("Rules: ================++~p~n",[Config]),
    % Rules = maps:get(relus, Config, []),
    NRules = [compile(Rule) || Rule <- Config],
    ok = emqx_hooks:add('client.check_acl', {?MODULE, check_authz, [NRules]},  -1),
    {ok, #{}}.

on_stop(_InstId, _State) ->
    Action = find_action_in_hooks(),
    emqx_hooks:del('client.check_acl', Action).

on_health_check(_InstId, State) -> {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.check_acl'),
    [Action] = [Action || {callback,{?MODULE, check_authz, _} = Action, _, _} <- Callbacks ],
    Action.

-spec(compile(rule()) -> rule()).
compile(#{<<"topics">> := Topics,
          <<"action">> := Action,
          <<"access">> := Access,
          <<"principal">> := Principal
         } = Rule ) when ?ALLOW_DENY(Access), ?PUBSUB(Action), is_list(Topics) ->
    NTopics = [compile(topic, Topic) || Topic <- Topics],
    Rule#{<<"principal">> => Principal,
          <<"topics">> => NTopics
         }.

compile(principal, all) -> all;
compile(principal, #{<<"username">> := Username}) ->
    {ok, MP} = re:compile(bin(Username)),
    #{<<"username">> => MP};
compile(principal, #{<<"clientid">> := Clientid}) ->
    {ok, MP} = re:compile(bin(Clientid)),
    #{<<"clientid">> => MP};
compile(principal, #{<<"ipaddress">> := IpAddress}) ->
    #{<<"ipaddress">> => esockd_cidr:parse(binary_to_list(IpAddress), true)};
compile(principal, #{<<"and">> := Principals}) when is_list(Principals) ->
    #{<<"and">> => [compile(principal, Principal) || Principal <- Principals]};
compile(principal, #{<<"or">> := Principals}) when is_list(Principals) ->
    #{<<"or">> => [compile(principal, Principal) || Principal <- Principals]};

compile(topic, #{<<"eq">> := Topic}) ->
    #{<<"eq">> => emqx_topic:words(bin(Topic))};
compile(topic, Topic) ->
    Words = emqx_topic:words(bin(Topic)),
    case pattern(Words) of
        true  -> #{<<"pattern">> => Words};
        false -> Words
    end.

pattern(Words) ->
    lists:member(<<"%u">>, Words) orelse lists:member(<<"%c">>, Words).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

%% @doc Check ACL
-spec(check_authz(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_topic:topic(),rules())
      -> {matched, allow} | {matched, deny} | {nomatch, deny}).
check_authz(Client, PubSub, Topic,
                    [Rule = #{<<"access">> := Access} | Tail]) ->
    case match(Client, PubSub, Topic, Rule) of
        true -> {matched, binary_to_existing_atom(Access, utf8)};
        false -> check_authz(Client, PubSub, Topic, Tail)
    end;
check_authz(_Client, _PubSub, _Topic, []) ->
    {nomatch, deny}.

match(Client, PubSub, Topic,
      #{<<"principal">> := Principal,
        <<"topics">> := TopicFilters,
        <<"action">> := Action
       }) ->
    match_action(PubSub, Action) andalso
    match_principal(Client, Principal) andalso
    match_topics(Client, Topic, TopicFilters).

match_action(publish, <<"pub">>) -> true;
match_action(subscribe, <<"sub">>) -> true;
match_action(_, <<"pubsub">>) -> true;
match_action(_, _) -> false.

match_principal(_, <<"all">>) -> true;
match_principal(#{username := Username}, #{<<"username">> := MP}) ->
    case re:run(Username, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_principal(#{clientid := Clientid}, #{<<"clientid">> := MP}) ->
    case re:run(Clientid, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_principal(#{peerhost := undefined}, #{<<"ipaddress">> := _CIDR}) ->
    false;
match_principal(#{peerhost := IpAddress}, #{<<"ipaddress">> := CIDR}) ->
    esockd_cidr:match(IpAddress, CIDR);
match_principal(ClientInfo, #{<<"and">> := Principals}) when is_list(Principals) ->
    lists:foldl(fun(Principal, Access) ->
                  match_principal(ClientInfo, Principal) andalso Access
                end, true, Principals);
match_principal(ClientInfo, #{<<"or">> := Principals}) when is_list(Principals) ->
    lists:foldl(fun(Principal, Access) ->
                  match_principal(ClientInfo, Principal) orelse Access
                end, false, Principals);
match_principal(_, _) -> false.

match_topics(_ClientInfo, _Topic, []) ->
    false;
match_topics(ClientInfo, Topic, [#{<<"pattern">> := PatternFilter}|Filters]) ->
    TopicFilter = feed_var(ClientInfo, PatternFilter),
    match_topic(emqx_topic:words(Topic), TopicFilter)
        orelse match_topics(ClientInfo, Topic, Filters);
match_topics(ClientInfo, Topic, [TopicFilter|Filters]) ->
   match_topic(emqx_topic:words(Topic), TopicFilter)
       orelse match_topics(ClientInfo, Topic, Filters).

match_topic(Topic, #{<<"eq">> := TopicFilter}) ->
    Topic == TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqx_topic:match(Topic, TopicFilter).

feed_var(ClientInfo, Pattern) ->
    feed_var(ClientInfo, Pattern, []).
feed_var(_ClientInfo, [], Acc) ->
    lists:reverse(Acc);
feed_var(ClientInfo = #{clientid := undefined}, [<<"%c">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [<<"%c">>|Acc]);
feed_var(ClientInfo = #{clientid := ClientId}, [<<"%c">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [ClientId |Acc]);
feed_var(ClientInfo = #{username := undefined}, [<<"%u">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [<<"%u">>|Acc]);
feed_var(ClientInfo = #{username := Username}, [<<"%u">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [Username|Acc]);
feed_var(ClientInfo, [W|Words], Acc) ->
    feed_var(ClientInfo, Words, [W|Acc]).
