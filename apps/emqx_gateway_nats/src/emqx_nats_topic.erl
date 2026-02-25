%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_topic).

-include("emqx_nats.hrl").

-export([
    nats_to_mqtt/1,
    nats_to_mqtt_publish/1,
    mqtt_to_nats/1,
    validate_nats_subject/1
]).

%% @doc Convert NATS subject to MQTT topic
%% NATS subject format: foo.bar.baz
%% MQTT topic format: foo/bar/baz
%% NATS wildcards: * matches a single token, > matches all remaining tokens
%% MQTT wildcards: + matches a single level, # matches all remaining levels
-spec nats_to_mqtt(binary()) -> binary().
nats_to_mqtt(<<>>) ->
    <<>>;
nats_to_mqtt(Subject) ->
    case has_nats_wildcard_chars(Subject) of
        false ->
            replace_dot_with_slash(Subject);
        true ->
            case binary:last(Subject) of
                $> ->
                    %% Convert NATS '>' to MQTT '#'
                    Base = binary:part(Subject, 0, byte_size(Subject) - 1),
                    BaseMqtt = nats_to_mqtt(Base),
                    <<BaseMqtt/binary, "#">>;
                _ ->
                    %% Convert NATS '*' to MQTT '+'
                    Parts = binary:split(Subject, <<".">>, [global]),
                    iolist_to_binary(
                        lists:join(<<"/">>, [convert_nats_wildcard(Part) || Part <- Parts])
                    )
            end
    end.

%% @doc Convert NATS subject to MQTT topic for publish path
%% Preserve NATS wildcard tokens as literals.
-spec nats_to_mqtt_publish(binary()) -> binary().
nats_to_mqtt_publish(<<>>) ->
    <<>>;
nats_to_mqtt_publish(Subject) ->
    replace_dot_with_slash(Subject).

%% @doc Convert MQTT topic to NATS subject
%% MQTT topic format: foo/bar/baz
%% NATS subject format: foo.bar.baz
%% MQTT wildcards: + matches a single level, # matches all remaining levels
%% NATS wildcards: * matches a single token, > matches all remaining tokens
-spec mqtt_to_nats(binary()) -> binary().
mqtt_to_nats(<<>>) ->
    <<>>;
mqtt_to_nats(Topic) ->
    case binary:last(Topic) of
        $# ->
            %% Convert MQTT '#' to NATS '>'
            Base = binary:part(Topic, 0, byte_size(Topic) - 1),
            BaseNats = mqtt_to_nats(Base),
            <<BaseNats/binary, ">">>;
        _ ->
            %% Convert MQTT '+' to NATS '*'
            Parts = binary:split(Topic, <<"/">>, [global]),
            iolist_to_binary(lists:join(<<".">>, [convert_mqtt_wildcard(Part) || Part <- Parts]))
    end.

%% @doc Convert NATS wildcard to MQTT wildcard
-spec convert_nats_wildcard(binary()) -> binary().
convert_nats_wildcard(<<"*">>) -> <<"+">>;
convert_nats_wildcard(Part) -> Part.

%% @doc Convert MQTT wildcard to NATS wildcard
-spec convert_mqtt_wildcard(binary()) -> binary().
convert_mqtt_wildcard(<<"+">>) -> <<"*">>;
convert_mqtt_wildcard(Part) -> Part.

%% @doc Validate NATS subject
%% Subject names are case-sensitive. They must be non-empty UTF-8
%% strings and cannot contain null characters, whitespace, or the
%% special characters . (period), * (asterisk), and > (greater than sign).
%% By convention, subject names starting with $ (e.g., $SYS., $JS.API., $KV.)
%% are reserved for NATS system use.
-spec validate_nats_subject(binary()) -> {ok, boolean()} | {error, term()}.
validate_nats_subject(<<>>) ->
    {error, empty_subject};
validate_nats_subject(Subject) ->
    case validate_utf8_char(Subject) of
        false ->
            {error, invalid_utf8_string};
        _ ->
            Tokens = binary:split(Subject, <<".">>, [global]),
            case validate_nats_subject_tokens(Tokens, 0, false) of
                true -> {ok, true};
                false -> {ok, false};
                {error, Error} -> {error, Error}
            end
    end.

validate_nats_subject_tokens([], _Lv, HasWildcard) ->
    HasWildcard;
validate_nats_subject_tokens([<<>>], _Lv, _HasWildcard) ->
    {error, ends_with_dot};
validate_nats_subject_tokens([<<>> | _Rest], 0, _HasWildcard) ->
    {error, starts_with_dot};
validate_nats_subject_tokens([<<>> | _Rest], _Lv, _HasWildcard) ->
    {error, consecutive_dots};
validate_nats_subject_tokens([<<">">>], _Lv, _HasWildcard) ->
    true;
validate_nats_subject_tokens([<<">">> | _Rest], _Lv, _HasWildcard) ->
    {error, special_chars_in_middle};
validate_nats_subject_tokens([<<"*">> | Rest], Lv, _HasWildcard) ->
    validate_nats_subject_tokens(Rest, Lv + 1, true);
validate_nats_subject_tokens([Token | Rest], Lv, HasWildcard) ->
    case has_forbidden_chars_in_token(Token) of
        true ->
            {error, special_chars_in_middle};
        false ->
            validate_nats_subject_tokens(Rest, Lv + 1, HasWildcard)
    end.

validate_utf8_char(<<>>) ->
    true;
validate_utf8_char(<<H/utf8, _Rest/binary>>) when
    H >= 16#00, H =< 16#1F;
    H >= 16#7F, H =< 16#9F
->
    false;
validate_utf8_char(<<_H/utf8, Rest/binary>>) ->
    validate_utf8_char(Rest);
validate_utf8_char(<<_BadUtf8, _Rest/binary>>) ->
    false.

has_nats_wildcard_chars(Subject) ->
    binary:match(Subject, <<"*">>) =/= nomatch orelse binary:match(Subject, <<">">>) =/= nomatch.

replace_dot_with_slash(Bin) ->
    binary:replace(Bin, <<".">>, <<"/">>, [global]).

has_forbidden_chars_in_token(<<>>) ->
    false;
has_forbidden_chars_in_token(<<C, _Rest/binary>>) when
    C =:= $\s;
    C =:= $*;
    C =:= $>;
    C =:= $.
->
    true;
has_forbidden_chars_in_token(<<_C, Rest/binary>>) ->
    has_forbidden_chars_in_token(Rest).
