%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry).

%% API
-export([
    ensure_card_schema_registered/0,

    discovery_topic/3,

    list_cards/0,
    list_cards/1,
    delete_card/1,

    lookup_agent_status/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

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

discovery_topic(OrgId, UnitId, AgentId) ->
    emqx_topic:join([
        ?A2A_TOPIC_NS,
        ?A2A_TOPIC_V1,
        ?A2A_TOPIC_DISCOVERY,
        OrgId,
        UnitId,
        AgentId
    ]).

list_cards() ->
    list_cards(_Opts = #{}).

list_cards(Opts) ->
    OrgId = maps:get(org_id, Opts, <<"+">>),
    UnitId = maps:get(unit_id, Opts, <<"+">>),
    AgentId = maps:get(agent_id, Opts, <<"+">>),
    TopicFilter = discovery_topic(OrgId, UnitId, AgentId),
    Cursor = undefined,
    MatchOpts = #{batch_read_number => all_remaining},
    {ok, Msgs, undefined} = emqx_retainer:match_messages(TopicFilter, Cursor, MatchOpts),
    lists:map(
        fun(#message{from = From, payload = PayloadRaw}) ->
            Card = emqx_utils_json:decode(PayloadRaw),
            format_card(Card, From)
        end,
        Msgs
    ).

lookup_agent_status(ClientId) ->
    FormatFn = undefined,
    case emqx_mgmt:lookup_client({clientid, ClientId}, FormatFn) of
        [{_Chan, #{conn_state := connected} = _Info, _Stats}] ->
            ?A2A_PROP_ONLINE_VAL;
        _ ->
            ?A2A_PROP_OFFLINE_VAL
    end.

delete_card(Opts) ->
    #{
        org_id := OrgId,
        unit_id := UnitId,
        agent_id := AgentId
    } = Opts,
    Topic = discovery_topic(OrgId, UnitId, AgentId),
    ok = emqx_retainer:delete(Topic),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

agent_card_schema_path() ->
    filename:join([code:lib_dir(emqx_a2a_registry), "priv", "agent_card_schema.json"]).

agent_card_schema_source() ->
    {ok, Source} = file:read_file(agent_card_schema_path()),
    Source.

format_card(Card0, ClientId) ->
    Card = maps:with(
        [
            <<"name">>,
            <<"description">>,
            <<"version">>
        ],
        Card0
    ),
    Status = lookup_agent_status(ClientId),
    Card#{<<"status">> => Status}.
