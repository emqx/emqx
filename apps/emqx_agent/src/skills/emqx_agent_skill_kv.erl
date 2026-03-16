%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Generic key-value skill with optional write access.
%%
%% A single shared ETS table is created by init/0.
%% Each create/1 call registers up to two skill instances:
%%   type kv.lookup — retrieve data by key
%%   type kv.put    — store data by key  (only when allow_put => true)
%%
%% ETS key: {SkillId, Key}  — all instances share one table, namespaced by skill_id.
%%
%% Invoke topics:
%%   cap/invoke/kv.lookup/<skill_id>
%%   cap/invoke/kv.put/<skill_id>
%%
%% Lifecycle:
%%   init()        — create ETS table + register message.publish hook
%%   create(Ctx)   — register skill instance(s)
%%   destroy(Id)   — unregister instance(s) and delete their ETS entries
%%   deinit()      — drop ETS table + remove hook

-module(emqx_agent_skill_kv).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(TAB, emqx_agent_skill_kv).
-define(TYPE_LOOKUP, <<"kv.lookup">>).
-define(TYPE_PUT, <<"kv.put">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-export([init/0, deinit/0, create/1, destroy_lookup/1, destroy_put/1, to_map/1]).

%% Hook callback — must be exported
-export([on_message_publish/1]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    ?TAB = ets:new(?TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    catch ets:delete(?TAB),
    ok.

%% Context keys (kv.lookup):
%%   skill_id    => binary()  — unique identifier
%%   desc        => binary()  — human-readable description of the stored objects
%%   data_schema => map()     — full JSON Schema for the stored value
%%
%% Context keys (kv.put):
%%   skill_id    => binary()
%%   desc        => binary()
%%   data_schema => map()
-spec create(map()) -> ok.
create(#{type := 'kv.lookup', skill_id := SkillId, desc := Desc, data_schema := DataSchema}) ->
    register_lookup(SkillId, Desc, DataSchema);
create(#{type := 'kv.put', skill_id := SkillId, desc := Desc, data_schema := DataSchema}) ->
    register_put(SkillId, Desc, DataSchema).

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, type := ?TYPE_LOOKUP, context := Ctx}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?TYPE_LOOKUP,
        <<"description">> => maps:get(desc, Ctx, <<>>),
        <<"data_schema">> => maps:get(data_schema, Ctx, #{})
    };
to_map(#{skill_id := Id, type := ?TYPE_PUT, context := Ctx}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?TYPE_PUT,
        <<"description">> => maps:get(desc, Ctx, <<>>),
        <<"data_schema">> => maps:get(data_schema, Ctx, #{})
    }.

-spec destroy_lookup(binary()) -> ok.
destroy_lookup(SkillId) ->
    emqx_agent_skill_registry:unregister(?TYPE_LOOKUP, SkillId).

-spec destroy_put(binary()) -> ok.
destroy_put(SkillId) ->
    emqx_agent_skill_registry:unregister(?TYPE_PUT, SkillId),
    ets:match_delete(?TAB, {{SkillId, '_'}, '_'}),
    ok.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<"cap/invoke/kv.lookup/", SkillId/binary>>, payload = Payload} = Message
) ->
    handle_lookup(SkillId, Payload),
    {ok, Message};
on_message_publish(
    #message{topic = <<"cap/invoke/kv.put/", SkillId/binary>>, payload = Payload} = Message
) ->
    handle_put(SkillId, Payload),
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal — skill registration
%%--------------------------------------------------------------------

register_lookup(SkillId, Desc, DataSchema) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?TYPE_LOOKUP,
        display_name => <<Desc/binary, " — Lookup">>,
        description => <<"Look up an entry by key.">>,
        context => #{skill_id => SkillId, desc => Desc, data_schema => DataSchema},
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"key">> => #{<<"type">> => <<"string">>}},
            <<"required">> => [<<"key">>]
        },
        output_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"status">> => #{
                    <<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"not_found">>]
                },
                <<"data">> => DataSchema
            },
            <<"required">> => [<<"status">>]
        }
    }).

register_put(SkillId, Desc, DataSchema) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?TYPE_PUT,
        display_name => <<Desc/binary, " — Put">>,
        description => <<"Store an entry by key.">>,
        context => #{skill_id => SkillId, desc => Desc, data_schema => DataSchema},
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"key">> => #{<<"type">> => <<"string">>},
                <<"data">> => DataSchema
            },
            <<"required">> => [<<"key">>, <<"data">>]
        },
        output_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>]}
            },
            <<"required">> => [<<"status">>]
        }
    }).

%%--------------------------------------------------------------------
%% Internal — invoke handlers
%%--------------------------------------------------------------------

handle_lookup(SkillId, Payload) ->
    case emqx_agent_skill_registry:lookup(?TYPE_LOOKUP, SkillId) of
        {error, not_found} ->
            ok;
        {ok, _Skill} ->
            Request = emqx_utils_json:decode(Payload),
            Args = maps:get(<<"args">>, Request),
            Key = maps:get(<<"key">>, Args),
            Data =
                case ets:lookup(?TAB, {SkillId, Key}) of
                    [{{SkillId, Key}, Value}] -> #{<<"status">> => <<"ok">>, <<"data">> => Value};
                    [] -> #{<<"status">> => <<"not_found">>}
                end,
            publish_reply(SkillId, ?TYPE_LOOKUP, Request, Data)
    end.

handle_put(SkillId, Payload) ->
    case emqx_agent_skill_registry:lookup(?TYPE_PUT, SkillId) of
        {error, not_found} ->
            ok;
        {ok, _Skill} ->
            Request = emqx_utils_json:decode(Payload),
            Args = maps:get(<<"args">>, Request),
            Key = maps:get(<<"key">>, Args),
            Value = maps:get(<<"data">>, Args),
            true = ets:insert(?TAB, {{SkillId, Key}, Value}),
            publish_reply(SkillId, ?TYPE_PUT, Request, #{<<"status">> => <<"ok">>})
    end.

publish_reply(SkillId, Type, Request, Data) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => Type, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Data
    }),
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.
