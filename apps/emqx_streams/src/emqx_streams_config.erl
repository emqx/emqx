%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_config).

-moduledoc """
Utility module to handle configuration of the Message Streams application and
individual streams.
""".

-include("emqx_streams_internal.hrl").

%% Converters
-export([
    stream_from_raw_post/1,
    stream_to_raw_get/1,
    stream_update_from_raw_put/1
]).

%% Config management
-export([
    raw_api_config/0,
    update_config/1
]).

%% emqx_config_handler callbacks
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% Streams config accessors
-export([
    is_enabled/0,
    max_stream_count/0,
    auto_create/1,
    gc_interval/0,
    check_stream_status_interval/0,
    regular_stream_retention_period/0,
    quota_buffer_pool_size/0,
    message_db_tx_retry_interval/0,
    quota_option/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec stream_from_raw_post(map()) -> emqx_streams_types:stream().
stream_from_raw_post(#{<<"topic_filter">> := _TopicFilter} = Config) ->
    Schema = #{roots => [{stream, emqx_streams_schema:stream_sctype_api_post()}]},
    #{stream := Stream} = hocon_tconf:check_plain(Schema, #{<<"stream">> => Config}, #{
        atom_key => true
    }),
    Stream.

-spec stream_to_raw_get(emqx_streams_types:stream()) -> map().
stream_to_raw_get(Stream) ->
    StreamRaw0 = binary_key_map(Stream),
    StreamRaw = maps:remove(<<"id">>, StreamRaw0),
    emqx_schema:fill_defaults_for_type(emqx_streams_schema:stream_sctype_api_get(), StreamRaw).

-spec stream_update_from_raw_put(map()) -> map().
stream_update_from_raw_put(UpdatedStreamRaw) ->
    Schema = #{roots => [{stream, emqx_streams_schema:stream_sctype_api_put()}]},
    #{stream := UpdatedStream} = hocon_tconf:check_plain(
        Schema, #{<<"stream">> => UpdatedStreamRaw}, #{
            atom_key => true
        }
    ),
    UpdatedStream.

-spec raw_api_config() -> map().
raw_api_config() ->
    RawConfig = emqx:get_raw_config([?SCHEMA_ROOT]),
    emqx_schema:fill_defaults_for_type(hoconsc:ref(emqx_streams_schema, ?SCHEMA_ROOT), RawConfig).

-spec update_config(emqx_config:update_request()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(UpdateRequest0) ->
    RawConfig = emqx:get_raw_config([?SCHEMA_ROOT]),
    UpdateRequest = maps:merge(RawConfig, UpdateRequest0),
    emqx_conf:update([?SCHEMA_ROOT], UpdateRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([?SCHEMA_ROOT, enable]).

-spec max_stream_count() -> pos_integer().
max_stream_count() ->
    emqx:get_config([?SCHEMA_ROOT, max_stream_count]).

-spec auto_create(emqx_streams_types:stream_topic()) -> false | {true, emqx_streams_types:stream()}.
auto_create(Topic) ->
    auto_create(Topic, emqx:get_config([?SCHEMA_ROOT, auto_create])).

-spec gc_interval() -> emqx_schema:timeout_duration_ms().
gc_interval() ->
    emqx:get_config([streams, gc_interval]).

-spec check_stream_status_interval() -> emqx_schema:timeout_duration_ms().
check_stream_status_interval() ->
    emqx:get_config([streams, check_stream_status_interval]).

-spec regular_stream_retention_period() -> emqx_schema:timeout_duration_ms().
regular_stream_retention_period() ->
    emqx:get_config([streams, regular_stream_retention_period]).

-spec quota_buffer_pool_size() -> pos_integer().
quota_buffer_pool_size() ->
    emqx:get_config([streams, quota, buffer_pool_size], ?DEFAULT_QUOTA_BUFFER_POOL_SIZE).

%% NOTE
%% All transactions to the lastvalue message db are simple writes, we do not do
%% anything inside. So if we have a conflict, we may retry just after the next flush.
%% We multiply by 2 to avoid races.
-spec message_db_tx_retry_interval() -> emqx_schema:timeout_duration_ms().
message_db_tx_retry_interval() ->
    emqx:get_config([durable_storage, streams_messages, transaction, flush_interval], 100) * 2.

-spec quota_option(atom()) -> term().
quota_option(threshold_percentage) ->
    emqx:get_config([streams, quota, threshold_percentage], ?DEFAULT_QUOTA_THRESHOLD_PERCENTAGE);
quota_option(buffer_max_size) ->
    emqx:get_config([streams, quota, buffer_max_size], ?DEFAULT_QUOTA_BUFFER_MAX_SIZE);
quota_option(buffer_flush_interval) ->
    emqx:get_config([streams, quota, buffer_flush_interval], ?DEFAULT_QUOTA_BUFFER_FLUSH_INTERVAL).

%%------------------------------------------------------------------------------
%% Config hooks
%%------------------------------------------------------------------------------

pre_config_update([?SCHEMA_ROOT], NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update([?SCHEMA_ROOT], _Request, NewConf, OldConf, _AppEnvs) ->
    maybe
        ok ?= validate_auto_create(NewConf),
        ok ?= maybe_enable(NewConf, OldConf)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

auto_create(Topic, #{regular := #{} = RegularAutoCreate}) ->
    Stream = RegularAutoCreate#{topic_filter => Topic, is_lastvalue => false},
    {true, Stream};
auto_create(Topic, #{lastvalue := #{} = LastvalueAutoCreate}) ->
    Stream = LastvalueAutoCreate#{topic_filter => Topic, is_lastvalue => true},
    {true, Stream};
auto_create(_Topic, _Config) ->
    false.

maybe_enable(#{enable := Enable} = _NewConf, #{enable := Enable} = _OldConf) ->
    ok;
maybe_enable(#{enable := false} = _NewConf, #{enable := true} = _OldConf) ->
    {error, #{reason => cannot_disable_streams_in_runtime}};
maybe_enable(#{enable := true} = _NewConf, #{enable := false} = _OldConf) ->
    ok = emqx_streams_app:do_start().

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
