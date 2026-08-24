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
    now_sec/0,
    ttl/0,
    submit_pool/2,
    maybe_batch_flush/3,
    cancel_timer/1
]).

-include("emqx_bcast.hrl").

-define(FLUSH_MS, 50).
-define(FLUSH_COUNT, 100).

gen_guid() ->
    emqx_guid:gen().

gen_api_uuid() ->
    Uuid = uuid:get_v4(),
    list_to_binary(uuid:uuid_to_string(Uuid)).

gen_api_uuid_from_hash(Hash) when is_binary(Hash), byte_size(Hash) >= 16 ->
    <<UuidBin:16/binary, _/binary>> = Hash,
    list_to_binary(uuid:uuid_to_string(UuidBin)).

guid_to_uuid(Guid) when is_binary(Guid) andalso byte_size(Guid) =:= 16 ->
    list_to_binary(uuid:uuid_to_string(Guid)).

uuid_to_guid(UuidStr) when is_binary(UuidStr) andalso byte_size(UuidStr) =:= 36 ->
    try uuid:string_to_uuid(binary_to_list(UuidStr)) of
        <<_:128>> = Guid -> {ok, Guid};
        _ -> error
    catch
        _:_ -> error
    end;
uuid_to_guid(_) ->
    error.

decode_base64(Base64) when is_binary(Base64) ->
    try base64:decode(Base64) of
        Payload -> {ok, Payload}
    catch
        _:_ -> {error, invalid_base64}
    end;
decode_base64(_) ->
    {error, invalid_base64}.

sha256(Data) when is_binary(Data) ->
    crypto:hash(sha256, Data).

expand_topic(Template, ProductKey, DeviceName) ->
    T1 = binary:replace(Template, <<"${productKey}">>, ProductKey, [global]),
    binary:replace(T1, <<"${deviceName}">>, DeviceName, [global]).

now_sec() ->
    erlang:system_time(second).

ttl() ->
    try persistent_term:get({?APP, config}, #{}) of
        Config -> maps:get(msg_ttl, Config, 15 * 86400)
    catch
        _:_ -> 15 * 86400
    end.

%% Submit a task to an emqx_pool worker pool, falling back to running it
%% inline when the pool is unavailable (e.g. before it is started).
submit_pool(Pool, Fun) ->
    try
        emqx_pool:async_submit_to_pool(Pool, Fun)
    catch
        _:_ -> Fun()
    end.

%% Batch-flush timer helper shared by the ack and pull pools: flush when
%% Count reaches the threshold, otherwise arm a timer that flushes after
%% FLUSH_MS. Returns the timer ref (undefined after an immediate flush).
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

cancel_timer(undefined) ->
    ok;
cancel_timer(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    ok.
