%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry).

%% API
-export([
    ensure_card_schema_registered/0,

    discovery_topic/4,

    list_cards/0,
    list_cards/1,
    delete_card/1,
    write_card/1,

    lookup_agent_status/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

ensure_card_schema_registered() ->
    case emqx_schema_registry:get_serde(?A2A_SCHEMA_REGISTRY_SERDE_NAME) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            ok = emqx_schema_registry:build_serdes([
                {?A2A_SCHEMA_REGISTRY_SERDE_NAME, #{
                    type => json,
                    source => agent_card_schema_source()
                }}
            ])
    end.

discovery_topic(?global_ns, OrgId, UnitId, AgentId) ->
    emqx_topic:join([
        ?A2A_TOPIC_NS,
        ?A2A_TOPIC_V1,
        ?A2A_TOPIC_DISCOVERY,
        OrgId,
        UnitId,
        AgentId
    ]);
discovery_topic(Namespace, OrgId, UnitId, AgentId) when is_binary(Namespace) ->
    emqx_topic:join([
        Namespace,
        ?A2A_TOPIC_NS,
        ?A2A_TOPIC_V1,
        ?A2A_TOPIC_DISCOVERY,
        OrgId,
        UnitId,
        AgentId
    ]).

-spec list_cards() -> {ok, [map()]} | {error, retainer_disabled}.
list_cards() ->
    list_cards(_Opts = #{}).

-spec list_cards(map()) -> {ok, [map()]} | {error, retainer_disabled}.
list_cards(Opts) ->
    case emqx_retainer:is_enabled() of
        false ->
            {error, retainer_disabled};
        true ->
            Namespace = maps:get(namespace, Opts, ?global_ns),
            OrgId = maps:get(org_id, Opts, <<"+">>),
            UnitId = maps:get(unit_id, Opts, <<"+">>),
            AgentId = maps:get(agent_id, Opts, <<"+">>),
            Cursor = undefined,
            MatchOpts = #{batch_read_number => all_remaining},
            Filters =
                case Namespace of
                    all ->
                        [
                            discovery_topic(?global_ns, OrgId, UnitId, AgentId),
                            discovery_topic(<<"+">>, OrgId, UnitId, AgentId)
                        ];
                    _ ->
                        [discovery_topic(Namespace, OrgId, UnitId, AgentId)]
                end,
            {ok, match_cards(Filters, Cursor, MatchOpts, [])}
    end.

match_cards([], _Cursor, _MatchOpts, Acc) ->
    lists:map(
        fun(#message{from = From, topic = Topic, payload = PayloadRaw}) ->
            Card = emqx_utils_json:decode(PayloadRaw),
            format_card(Card, PayloadRaw, Topic, From)
        end,
        lists:reverse(Acc)
    );
match_cards([TopicFilter | Rest], Cursor, MatchOpts, Acc) ->
    {ok, Msgs, undefined} = emqx_retainer:match_messages(TopicFilter, Cursor, MatchOpts),
    match_cards(Rest, Cursor, MatchOpts, lists:reverse(Msgs, Acc)).

lookup_agent_status(ClientId) ->
    FormatFn = undefined,
    case emqx_mgmt:lookup_client({clientid, ClientId}, FormatFn) of
        [{_Chan, #{conn_state := connected} = _Info, _Stats}] ->
            ?A2A_PROP_ONLINE_VAL;
        _ ->
            ?A2A_PROP_OFFLINE_VAL
    end.

-spec delete_card(map()) -> ok | {error, retainer_disabled}.
delete_card(Opts) ->
    #{
        namespace := Namespace,
        org_id := OrgId,
        unit_id := UnitId,
        agent_id := AgentId
    } = Opts,
    case emqx_retainer:is_enabled() of
        false ->
            {error, retainer_disabled};
        true ->
            Topic = discovery_topic(Namespace, OrgId, UnitId, AgentId),
            ok = emqx_retainer:delete(Topic)
    end.

write_card(Opts) ->
    #{
        namespace := Namespace,
        org_id := OrgId,
        unit_id := UnitId,
        agent_id := AgentId,
        card_bin := CardBin
    } = Opts,
    maybe
        ok ?= emqx_a2a_registry_utils:validate_id(OrgId, UnitId, AgentId),
        ok ?= emqx_a2a_registry_utils:validate_card_schema(CardBin),
        ok ?= emqx_a2a_registry_utils:validate_namespace_exists(Namespace),
        ClientId = agent_card_clientid(OrgId, UnitId, AgentId),
        OriginalTopic = discovery_topic(?global_ns, OrgId, UnitId, AgentId),
        %% "Mounted" topic
        Topic = discovery_topic(Namespace, OrgId, UnitId, AgentId),
        QoS = 1,
        Flags = #{retain => true},
        Headers = #{original_topic => OriginalTopic},
        Msg = emqx_message:make(ClientId, QoS, Topic, CardBin, Flags, Headers),
        ok ?= store_retained(Msg)
    end.

store_retained(Msg) ->
    case emqx_retainer:store_retained(Msg) of
        {error, no_backend} ->
            {error, retainer_disabled};
        Other ->
            Other
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

agent_card_schema_path() ->
    filename:join([code:lib_dir(emqx_a2a_registry), "priv", "agent_card_schema.json"]).

agent_card_schema_source() ->
    {ok, Source} = file:read_file(agent_card_schema_path()),
    Source.

format_card(Card0, CardRaw, Topic, ClientId) ->
    Card = maps:with(
        [
            <<"name">>,
            <<"description">>,
            <<"version">>
        ],
        Card0
    ),
    case emqx_a2a_registry_utils:parse_a2a_discovery_topic(Topic) of
        {ok, Namespace, {OrgId, UnitId, AgentId}} ->
            ok;
        error ->
            %% Impossible
            Namespace = ?global_ns,
            OrgId = undefined,
            UnitId = undefined,
            AgentId = undefined
    end,
    Status = lookup_agent_status(ClientId),
    Card#{
        <<"namespace">> => Namespace,
        <<"id">> => ClientId,
        <<"org_id">> => OrgId,
        <<"unit_id">> => UnitId,
        <<"agent_id">> => AgentId,
        <<"status">> => Status,
        <<"raw">> => CardRaw
    }.

agent_card_clientid(OrgId, UnitId, AgentId) ->
    emqx_topic:join([OrgId, UnitId, AgentId]).
