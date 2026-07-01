%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_utils).

-export([
    gen_guid/0,
    gen_api_uuid/0,
    decode_base64/1,
    sha256/1,
    expand_topic/3,
    now_sec/0,
    ttl/0
]).

-include("emqx_iot.hrl").

gen_guid() ->
    emqx_guid:gen().

gen_api_uuid() ->
    Uuid = uuid:get_v4(),
    list_to_binary(uuid:uuid_to_string(Uuid)).

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
