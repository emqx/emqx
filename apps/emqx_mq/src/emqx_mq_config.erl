%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_config).

-export([
    mq_from_raw_config/1,
    mq_to_raw_config/1,
    mq_update_from_raw_config/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

mq_from_raw_config(#{<<"topic_filter">> := _TopicFilter} = Config) ->
    Schema = #{roots => emqx_mq_schema:fields(message_queue)},
    MQ = hocon_tconf:check_plain(Schema, Config, #{atom_key => true}),
    MQ.

mq_to_raw_config(MQ) ->
    MQRaw0 = binary_key_map(MQ),
    MQRaw = maps:remove(<<"id">>, MQRaw0),
    emqx_schema:fill_defaults_for_type(hoconsc:ref(emqx_mq_schema, message_queue), MQRaw).

mq_update_from_raw_config(UpdatedMessageQueueRaw) ->
    Schema = #{roots => emqx_mq_schema:fields(message_queue_api_put)},
    UpdatedMessageQueue = hocon_tconf:check_plain(Schema, UpdatedMessageQueueRaw, #{
        atom_key => true
    }),
    UpdatedMessageQueue.

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
