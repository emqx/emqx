%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_config_secrets).

-export([redact/1, deobfuscate/2]).

redact(#{<<"headers">> := Headers} = Config) when is_list(Headers) ->
    emqx_utils:redact(Config#{<<"headers">> := lists:map(fun redact_header/1, Headers)});
redact(Config) ->
    emqx_utils:redact(Config).

redact_header(#{<<"name">> := Name, <<"value">> := _} = Header) when is_binary(Name) ->
    case emqx_utils_redact:check_is_sensitive_header(Name) of
        true -> Header#{<<"value">> := emqx_utils_redact:redacted_value()};
        false -> Header
    end;
redact_header(Header) ->
    Header.

deobfuscate(#{<<"headers">> := Headers} = Config, OldConfig) when is_list(Headers) ->
    OldHeaders = maps:get(<<"headers">>, OldConfig, []),
    Deobfuscated = restore_headers(Headers, OldHeaders),
    emqx_utils:deobfuscate(Config#{<<"headers">> := Deobfuscated}, OldConfig);
deobfuscate(Config, OldConfig) ->
    emqx_utils:deobfuscate(Config, OldConfig).

restore_headers(Headers, OldHeaders) ->
    {Restored, _} = lists:foldl(
        fun restore_header/2,
        {[], OldHeaders},
        Headers
    ),
    lists:reverse(Restored).

restore_header(#{<<"name">> := Name, <<"value">> := Value} = Header, {Acc, OldHeaders}) when
    is_binary(Name)
->
    {OldValue, Rest} = take_header(Name, OldHeaders),
    IsMasked = Value =:= emqx_utils_redact:redacted_value(),
    case IsMasked andalso emqx_utils_redact:check_is_sensitive_header(Name) of
        true when OldValue =:= undefined -> {Acc, Rest};
        true -> {[Header#{<<"value">> := OldValue} | Acc], Rest};
        false -> {[Header | Acc], Rest}
    end;
restore_header(Header, {Acc, OldHeaders}) ->
    {[Header | Acc], OldHeaders}.

take_header(Name, [#{<<"name">> := Name, <<"value">> := Value} | Rest]) ->
    {Value, Rest};
take_header(Name, [Header | Rest]) ->
    {Value, Remaining} = take_header(Name, Rest),
    {Value, [Header | Remaining]};
take_header(_Name, []) ->
    {undefined, []}.
