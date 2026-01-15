%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_config).

-export([
    mq_from_raw_post/1,
    mq_to_raw_get/1,
    mq_update_from_raw_put/1,
    raw_api_config/0,
    update_config/1,
    is_enabled/0,
    max_queue_count/0,
    auto_create/1
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

-spec auto_create(emqx_mq_types:mq_topic()) -> false | {true, emqx_mq_types:mq()}.
auto_create(Topic) ->
    auto_create(Topic, emqx:get_config(?MQ_CONFIG_PATH ++ [auto_create])).

%%------------------------------------------------------------------------------
%% Config hooks
%%------------------------------------------------------------------------------

pre_config_update(?MQ_CONFIG_PATH, NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update(?MQ_CONFIG_PATH, _Request, NewConf, OldConf, _AppEnvs) ->
    maybe
        ok ?= validate_auto_create(NewConf),
        ok ?= maybe_enable(NewConf, OldConf),
        ok ?= maybe_reschedule_gc(NewConf, OldConf)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

auto_create(Topic, #{regular := #{} = RegularAutoCreate}) ->
    MQ = RegularAutoCreate#{topic_filter => Topic, is_lastvalue => false},
    {true, MQ};
auto_create(Topic, #{lastvalue := #{} = LastvalueAutoCreate}) ->
    MQ = LastvalueAutoCreate#{topic_filter => Topic, is_lastvalue => true},
    {true, MQ};
auto_create(_Topic, _Config) ->
    false.

maybe_enable(#{enable := Enable} = _NewConf, #{enable := Enable} = _OldConf) ->
    ok;
maybe_enable(#{enable := false} = _NewConf, #{enable := true} = _OldConf) ->
    {error, #{reason => cannot_disable_mq_in_runtime}};
maybe_enable(#{enable := true} = _NewConf, #{enable := false} = _OldConf) ->
    ok = emqx_mq_app:do_start().

maybe_reschedule_gc(
    #{gc_interval := GcInterval} = _NewConf, #{gc_interval := GcInterval} = _OldConf
) ->
    ok;
maybe_reschedule_gc(#{enable := true, gc_interval := GcInterval} = _NewConf, _OldConf) ->
    ok = emqx_mq_gc:reschedule(GcInterval);
maybe_reschedule_gc(_NewConf, _OldConf) ->
    ok.

validate_auto_create(
    #{auto_create := #{regular := #{}, lastvalue := #{}}} = _NewConf
) ->
    {error, #{reason => cannot_enable_both_regular_and_lastvalue_auto_create}};
validate_auto_create(_NewConf) ->
    ok.

binary_key_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            maps:put(atom_to_binary(K, utf8), V, Acc)
        end,
        #{},
        Map
    ).
