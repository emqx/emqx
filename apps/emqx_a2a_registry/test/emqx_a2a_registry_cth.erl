%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_cth).

%% API
-export([
    clear_all_cards/0,
    card_count/0,
    all_cards/0,
    sample_card/0,
    agent_clientid/3,
    start_client/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("../src/emqx_a2a_registry_internal.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

clear_all_cards() ->
    emqx_retainer:clean(),
    ok.

card_count() ->
    length(all_cards()).

all_cards() ->
    emqx_retainer:with_backend(fun(Mod, State) ->
        {ok, Msgs, undefined} = Mod:match_messages(
            State,
            emqx_a2a_registry:discovery_topic(<<"+">>, <<"+">>, <<"+">>),
            undefined,
            #{batch_read_number => all_remaining}
        ),
        lists:map(fun(#message{payload = P}) -> emqx_utils_json:decode(P) end, Msgs)
    end).

sample_card() ->
    #{
        <<"name">> => <<"some_agent">>,
        <<"description">> => <<"description">>,
        <<"version">> => <<"1">>,
        <<"url">> => <<"http://httbin.org/get">>,
        <<"skills">> => []
    }.

agent_clientid(OrgId, UnitId, AgentId) ->
    emqx_topic:join([
        OrgId,
        UnitId,
        AgentId
    ]).

start_client(Overrides) ->
    Defaults = #{
        proto_ver => v5,
        clean_start => true,
        properties => #{'Session-Expiry-Interval' => 30}
    },
    Opts = emqx_utils_maps:deep_merge(Defaults, Overrides),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
