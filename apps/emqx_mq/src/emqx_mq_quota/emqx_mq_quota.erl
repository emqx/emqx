%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota).

-include("emqx_mq_quota.hrl").

-export([
    start/2,
    stop/1,
    add/6
]).

-type base_tx_opts() :: #{
    db := emqx_ds:db(),
    retries := non_neg_integer(),
    retry_interval := non_neg_integer()
}.

-type dynamic_option(T) :: T | fun(() -> T).
-type time_us() :: non_neg_integer().
-type quota_index_persist_error() :: term().
-type message_update_info() :: #{
    message := emqx_message:message(),
    message_size := non_neg_integer()
}.
-type index_opts() :: emqx_mq_message_quota_index:opts().
-type name() :: ecpool:pool_name().
-type tx_key() :: term().
-type key() :: term().

-type read_fun() :: fun((tx_key(), key()) -> binary() | undefined).
-type write_fun() :: fun((tx_key(), key(), binary()) -> ok).

%% NOTE
%% We use dynamic options to allow runtime reconfiguration of the options by passing
%% buffer_max_size => fun() -> emqx:get_config(...) end
-type options() :: #{
    % persist_base_tx_opts := dynamic_option(base_tx_opts()),
    % on_persist_ok := fun((time_us()) -> ok),
    % on_persist_error := fun((quota_index_persist_error()) -> ok),
    pool_size := pos_integer(),
    buffer_max_size := dynamic_option(pos_integer()),
    buffer_flush_interval := dynamic_option(non_neg_integer()),
    tx_fun := fun((tx_key(), fun(() -> ok)) -> ok),
    read_fun := read_fun(),
    write_fun := write_fun()
}.

-export_type([options/0, message_update_info/0, index_opts/0]).

-spec start(name(), options()) -> ok.
start(Name, Options) ->
    ok = emqx_mq_message_quota_buffer:start(Name, Options).

-spec stop(name()) -> ok.
stop(Name) ->
    ok = emqx_mq_message_quota_buffer:stop(Name).

-spec add(
    name(),
    tx_key(),
    key(),
    emqx_mq_quota:index_opts(),
    emqx_maybe:option(emqx_mq_quota:message_update_info()),
    emqx_mq_quota:message_update_info()
) -> ok.
add(Name, Slab, Topic, QuotaIndexOpts, OldMessageInfo, NewMessageInfo) ->
    ok = emqx_mq_message_quota_buffer:add(
        Name,
        Slab,
        Topic,
        QuotaIndexOpts,
        OldMessageInfo,
        NewMessageInfo
    ).

-spec dirty_index(name(), emqx_ds:db(), emqx_ds:slab(), emqx_ds:topic(), emqx_mq_quota:index_opts()) ->
    emqx_mq_message_quota_index:t() | undefined.
dirty_index(Name, {Shard, Generation} = _Slab, Topic, QuotaIndexOpts) ->
    case
        emqx_ds:dirty_read(
            #{db => DB, shard => Shard, generation => Generation},
            Topic
        )
    of
        [] ->
            undefined;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            Opts = emqx_mq_prop:quota_index_opts(MQHandle),
            emqx_mq_message_quota_index:decode(Opts, IndexBin)
    end.


