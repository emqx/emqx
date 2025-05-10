%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_topic).

-include("emqx_nats.hrl").

-export([
    nats_to_mqtt/1,
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
    error(badarg);
nats_to_mqtt(Subject) ->
    case binary:last(Subject) of
        $> ->
            %% Convert NATS '>' to MQTT '#'
            Base = binary:part(Subject, 0, byte_size(Subject) - 1),
            <<(binary:replace(Base, <<".">>, <<"/">>, [global]))/binary, "#">>;
        _ ->
            %% Convert NATS '*' to MQTT '+'
            Parts = binary:split(Subject, <<".">>, [global]),
            iolist_to_binary(lists:join(<<"/">>, [convert_nats_wildcard(Part) || Part <- Parts]))
    end.

%% @doc Convert MQTT topic to NATS subject
%% MQTT topic format: foo/bar/baz
%% NATS subject format: foo.bar.baz
%% MQTT wildcards: + matches a single level, # matches all remaining levels
%% NATS wildcards: * matches a single token, > matches all remaining tokens
-spec mqtt_to_nats(binary()) -> binary().
mqtt_to_nats(<<>>) ->
    error(badarg);
mqtt_to_nats(Topic) ->
    case binary:last(Topic) of
        $# ->
            %% Convert MQTT '#' to NATS '>'
            Base = binary:part(Topic, 0, byte_size(Topic) - 1),
            <<(binary:replace(Base, <<"/">>, <<".">>, [global]))/binary, ">">>;
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
%% NATS subject rules:
%% 1. Cannot be empty
%% 2. Cannot start or end with '.'
%% 3. Cannot contain consecutive '.'
%% 4. Cannot contain wildcards in the middle
%% 5. Can only contain: a-z, A-Z, 0-9, '_', '-', '.', '*', '>'
-spec validate_nats_subject(binary()) -> ok | {error, term()}.
validate_nats_subject(<<>>) ->
    {error, empty_subject};
validate_nats_subject(Subject) ->
    case binary:match(Subject, <<"..">>) of
        nomatch ->
            validate_nats_subject_chars(Subject);
        _ ->
            {error, consecutive_dots}
    end.

validate_nats_subject_chars(Subject) ->
    case binary:first(Subject) of
        $. ->
            {error, starts_with_dot};
        _ ->
            case binary:last(Subject) of
                $. -> {error, ends_with_dot};
                _ -> validate_nats_subject_wildcards(Subject)
            end
    end.

validate_nats_subject_wildcards(Subject) ->
    case binary:split(Subject, <<">">>, [global]) of
        [Subject] ->
            validate_nats_subject_chars_only(Subject);
        _ ->
            validate_nats_subject_trailing_wildcard(Subject)
    end.

validate_nats_subject_trailing_wildcard(Subject) ->
    case binary:last(Subject) of
        $> -> validate_nats_subject_chars_only(Subject);
        _ -> {error, invalid_wildcard_position}
    end.

validate_nats_subject_chars_only(Subject) ->
    case re:run(Subject, "^[a-zA-Z0-9_\\-\\.\\*\\>]+$") of
        nomatch -> {error, invalid_characters};
        _ -> ok
    end.
