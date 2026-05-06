%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: list or introspect AI providers.

-module(emqx_agent_skill_query_providers).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.query_providers">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"name">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Provider name. Omit to list all providers.">>
        }
    }
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"items">> => #{
            <<"type">> => <<"array">>, <<"description">> => <<"Present on list result">>
        },
        <<"item">> => #{
            <<"type">> => <<"object">>, <<"description">> => <<"Present on single-get result">>
        },
        <<"reason">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Present when status=error">>
        }
    },
    <<"required">> => [<<"status">>]
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1]).
-export([on_message_publish/1]).

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

-spec create(map()) -> ok | {error, term()}.
create(#{skill_id := SkillId}) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"Query AI Providers">>,
        description => <<"List all AI providers or look up a specific one by name">>,
        context => #{skill_id => SkillId},
        input_schema => ?INPUT_SCHEMA,
        output_schema => ?OUTPUT_SCHEMA
    }).

-spec destroy(binary()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, description := Desc, input_schema := In, output_schema := Out}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => In,
        <<"output_schema">> => Out
    }.

on_message_publish(Msg) ->
    emqx_agent_skill_helpers:if_skill_request(
        ?SKILL_TYPE,
        fun(SkillId, #message{payload = Payload}) ->
            handle_invoke(SkillId, Payload)
        end,
        Msg
    ).

handle_invoke(SkillId, Payload) ->
    %% TODO: validate
    Request = emqx_utils_json:decode(Payload),
    Args = maps:get(<<"args">>, Request, #{}),
    Result = query(Args),
    reply(SkillId, Request, Result).

query(#{<<"name">> := Name}) ->
    case find_provider(Name) of
        {ok, Provider} ->
            #{<<"status">> => <<"ok">>, <<"item">> => format_provider(Provider)};
        not_found ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"not found">>}
    end;
query(_) ->
    Items = [format_provider(P) || P <- emqx_ai_completion_config:get_providers_raw()],
    #{<<"status">> => <<"ok">>, <<"items">> => Items}.

find_provider(Name) ->
    case
        [P || #{<<"name">> := N} = P <- emqx_ai_completion_config:get_providers_raw(), N =:= Name]
    of
        [Provider] -> {ok, Provider};
        [] -> not_found
    end.

format_provider(Provider) ->
    emqx_utils:redact(Provider).

reply(SkillId, Request, Data) ->
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).
