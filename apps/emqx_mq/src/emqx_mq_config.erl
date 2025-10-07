%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_config).

-export([
    mq_from_raw_post/1,
    mq_to_raw_get/1,
    mq_update_from_raw_put/1,
    raw_api_config/0,
    update_config/1,
    is_enabled/0,
    max_queue_count/0
]).

-export([
    pre_config_update/3,
    post_config_update/5
]).

-define(MQ_CONFIG_PATH, [mq]).

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

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config(?MQ_CONFIG_PATH ++ [enable]).

-spec max_queue_count() -> pos_integer().
max_queue_count() ->
    emqx:get_config(?MQ_CONFIG_PATH ++ [max_queue_count]).

%%------------------------------------------------------------------------------
%% Config hooks
%%------------------------------------------------------------------------------

pre_config_update(?MQ_CONFIG_PATH, NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update(?MQ_CONFIG_PATH, _Request, NewConf, OldConf, _AppEnvs) ->
    maybe_enable(NewConf, OldConf).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

maybe_enable(#{enable := Enable} = _NewConf, #{enable := Enable} = _OldConf) ->
    ok;
maybe_enable(#{enable := false} = _NewConf, #{enable := true} = _OldConf) ->
    {error, #{reason => cannot_disable_mq_in_runtime}};
maybe_enable(#{enable := true} = _NewConf, #{enable := false} = _OldConf) ->
    ok = emqx_mq_app:do_start().

binary_key_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            maps:put(atom_to_binary(K, utf8), V, Acc)
        end,
        #{},
        Map
    ).
