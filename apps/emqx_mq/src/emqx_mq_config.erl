%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_config).

-export([
    mq_from_raw_config/1,
    mq_to_raw_config/1,
    mq_update_from_raw_config/1,
    raw_api_config/0,
    update_config/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec mq_from_raw_config(map()) -> emqx_mq_types:mq().
mq_from_raw_config(#{<<"topic_filter">> := _TopicFilter} = Config) ->
    Schema = #{roots => emqx_mq_schema:fields(message_queue)},
    MQ = hocon_tconf:check_plain(Schema, Config, #{atom_key => true}),
    MQ.

-spec mq_to_raw_config(emqx_mq_types:mq()) -> map().
mq_to_raw_config(MQ) ->
    MQRaw0 = binary_key_map(MQ),
    MQRaw = maps:remove(<<"id">>, MQRaw0),
    emqx_schema:fill_defaults_for_type(hoconsc:ref(emqx_mq_schema, message_queue), MQRaw).

-spec mq_update_from_raw_config(map()) -> map().
mq_update_from_raw_config(UpdatedMessageQueueRaw) ->
    Schema = #{roots => emqx_mq_schema:fields(message_queue_api_put)},
    UpdatedMessageQueue = hocon_tconf:check_plain(Schema, UpdatedMessageQueueRaw, #{
        atom_key => true
    }),
    UpdatedMessageQueue.

-spec raw_api_config() -> map().
raw_api_config() ->
    RawConfig0 = emqx:get_raw_config([mq]),
    RawConfig = emqx_schema:fill_defaults_for_type(hoconsc:ref(emqx_mq_schema, mq), RawConfig0),
    maps:without([<<"state_db">>, <<"message_db">>], RawConfig).

-spec update_config(emqx_config:update_request()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(UpdateRequest0) ->
    RawConfig = emqx:get_raw_config([mq]),
    UpdateRequest = maps:merge(RawConfig, UpdateRequest0),
    emqx_conf:update([mq], UpdateRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

binary_key_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            maps:put(atom_to_binary(K, utf8), V, Acc)
        end,
        #{},
        Map
    ).
