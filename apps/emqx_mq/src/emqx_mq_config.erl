%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_config).

-export([
    mq_from_raw_post/1,
    mq_to_raw_get/1,
    mq_update_from_raw_put/1,
    raw_api_config/0,
    update_config/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec mq_from_raw_post(map()) -> emqx_mq_types:mq().
mq_from_raw_post(#{<<"topic_filter">> := _TopicFilter} = Config) ->
    Schema = #{roots => [{mq, emqx_mq_schema:mq_sctype_api_post()}]},
    #{mq := MQ} = hocon_tconf:check_plain(Schema, #{<<"mq">> => Config}, #{atom_key => true}),
    MQ.

-spec mq_to_raw_get(emqx_mq_types:mq()) -> map().
mq_to_raw_get(MQ) ->
    MQRaw0 = binary_key_map(MQ),
    MQRaw = maps:remove(<<"id">>, MQRaw0),
    emqx_schema:fill_defaults_for_type(emqx_mq_schema:mq_sctype_api_get(), MQRaw).

-spec mq_update_from_raw_put(map()) -> map().
mq_update_from_raw_put(UpdatedMessageQueueRaw) ->
    Schema = #{roots => [{mq, emqx_mq_schema:mq_sctype_api_put()}]},
    #{mq := UpdatedMessageQueue} = hocon_tconf:check_plain(
        Schema, #{<<"mq">> => UpdatedMessageQueueRaw}, #{
            atom_key => true
        }
    ),
    UpdatedMessageQueue.

-spec raw_api_config() -> map().
raw_api_config() ->
    RawConfig = emqx:get_raw_config([mq]),
    emqx_schema:fill_defaults_for_type(hoconsc:ref(emqx_mq_schema, mq), RawConfig).

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
