%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_utils).

-export([
    gen_guid/0,
    gen_api_uuid/0,
    gen_api_uuid_from_hash/1,
    guid_to_uuid/1,
    uuid_to_guid/1,
    decode_base64/1,
    sha256/1,
    expand_topic/3,
    replace_product_key/2,
    expand_topic_partial/2,
    now_sec/0,
    ttl/0,
    submit_pool/2,
    pool_available/1,
    maybe_batch_flush/3,
    cancel_timer/1,
    ensure_ets/2
]).

-include("emqx_bcast.hrl").

%% Batch-flush cadence shared by the ack and pull pools. 10ms keeps the
%% per-client want_next/ack cycle latency low (window=1 makes the drain
%% latency-bound at ~25-33k msg/s with 90k clients); 100 is the immediate
%% flush threshold.
-define(FLUSH_MS, 10).
-define(FLUSH_COUNT, 100).

-spec gen_guid() -> binary().
gen_guid() ->
    emqx_guid:gen().

-spec gen_api_uuid() -> binary().
gen_api_uuid() ->
    Uuid = uuid:get_v4(),
    list_to_binary(uuid:uuid_to_string(Uuid)).

-spec gen_api_uuid_from_hash(binary()) -> binary().
gen_api_uuid_from_hash(Hash) when is_binary(Hash), byte_size(Hash) >= 16 ->
    <<UuidBin:16/binary, _/binary>> = Hash,
    list_to_binary(uuid:uuid_to_string(UuidBin)).

-spec guid_to_uuid(binary()) -> binary().
guid_to_uuid(Guid) when is_binary(Guid) andalso byte_size(Guid) =:= 16 ->
    list_to_binary(uuid:uuid_to_string(Guid)).

-spec uuid_to_guid(binary()) -> {ok, binary()} | error.
uuid_to_guid(UuidStr) when is_binary(UuidStr) andalso byte_size(UuidStr) =:= 36 ->
    try uuid:string_to_uuid(binary_to_list(UuidStr)) of
        <<_:128>> = Guid -> {ok, Guid};
        _ -> error
    catch
        _:_ -> error
    end;
uuid_to_guid(_) ->
    error.

-spec decode_base64(term()) -> {ok, binary()} | {error, invalid_base64}.
decode_base64(Base64) when is_binary(Base64) ->
    try base64:decode(Base64) of
        Payload -> {ok, Payload}
    catch
        _:_ -> {error, invalid_base64}
    end;
decode_base64(_) ->
    {error, invalid_base64}.

-spec sha256(binary()) -> binary().
sha256(Data) when is_binary(Data) ->
    crypto:hash(sha256, Data).

%% Split expansion so a device list sharing one template+product key
%% replaces ${productKey} once (replace_product_key/2) and then only
%% ${deviceName} per device (expand_topic_partial/2) instead of running
%% both binary:replace passes for every device of a large fanout.
-spec expand_topic(binary(), binary(), binary()) -> binary().
expand_topic(Template, ProductKey, DeviceName) ->
    expand_topic_partial(replace_product_key(Template, ProductKey), DeviceName).

-spec replace_product_key(binary(), binary()) -> binary().
replace_product_key(Template, ProductKey) ->
    binary:replace(Template, <<"${productKey}">>, ProductKey, [global]).

-spec expand_topic_partial(binary(), binary()) -> binary().
expand_topic_partial(Partial, DeviceName) ->
    binary:replace(Partial, <<"${deviceName}">>, DeviceName, [global]).

-spec now_sec() -> non_neg_integer().
now_sec() ->
    erlang:system_time(second).

-spec ttl() -> non_neg_integer().
ttl() ->
    emqx_bcast_config:get(msg_ttl).

%% Submit a task to an emqx_pool worker pool. Callers must not run the task
%% inline on failure: pull_pool uses this helper from its gen_server and an
%% inline RPC (do_want_next) would block every cast queued behind it.
%% Returns ok | {error, Reason}; the caller decides whether to log and drop.

-spec pool_available(atom()) -> boolean().
pool_available(Pool) ->
    try gproc_pool:pick_worker(Pool) =/= false of
        Available -> Available
    catch
        _:_ -> false
    end.

-spec submit_pool(atom(), fun()) -> ok | {error, term()}.
submit_pool(Pool, Fun) ->
    %% pool_available/1 is a load-bearing preflight:
    %% emqx_pool:async_submit_to_pool casts to the worker returned by the
    %% pool and can vanish without an error when no worker is registered.
    case pool_available(Pool) of
        false ->
            {error, pool_unavailable};
        true ->
            try emqx_pool:async_submit_to_pool(Pool, Fun) of
                ok -> ok;
                Other -> {error, {submit_return, Other}}
            catch
                Error:Reason -> {error, {Error, Reason}}
            end
    end.

%% Batch-flush timer helper shared by the ack and pull pools: flush when
%% Count reaches the threshold, otherwise arm a timer that flushes after
%% FLUSH_MS. Returns the timer ref (undefined after an immediate flush).

-spec maybe_batch_flush(non_neg_integer(), reference() | undefined, term()) ->
    reference() | undefined.
maybe_batch_flush(Count, TimerRef, FlushMsg) ->
    case Count >= ?FLUSH_COUNT of
        true ->
            _ = cancel_timer(TimerRef),
            self() ! FlushMsg,
            undefined;
        false ->
            case TimerRef of
                undefined -> erlang:send_after(?FLUSH_MS, self(), FlushMsg);
                _ -> TimerRef
            end
    end.

-spec cancel_timer(reference() | undefined) -> ok.
cancel_timer(undefined) ->
    ok;
cancel_timer(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    ok.

-spec ensure_ets(atom(), [term()]) -> ok.
ensure_ets(Name, Opts) ->
    try ets:new(Name, Opts) of
        _Tid -> ok
    catch
        error:badarg -> ok
    end.
