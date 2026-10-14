%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Properties of the binary topic functions in `emqx_topic'. The binary functions scan the
%% topic in 7-byte words, so the generators produce levels of 0 to 12 bytes, with multi-byte
%% UTF-8 characters, and the reference results come from the level lists.
-module(prop_emqx_topic).

-include_lib("proper/include/proper.hrl").

-define(WORD_BYTES, "abcdefghijklmnopqrstuvwxyz0123456789-_.$").

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

%% `validate/2' accepts a topic if and only if the reference check accepts it.
prop_validate_filter() ->
    ?FORALL(
        Topic,
        raw_topic(),
        reference_valid_filter(Topic) =:= accepts(filter, Topic)
    ).

prop_validate_name() ->
    ?FORALL(
        Topic,
        raw_topic(),
        reference_valid_name(Topic) =:= accepts(name, Topic)
    ).

%% `words/1' splits a valid filter into its levels, and `join/1' builds it back.
prop_words_join() ->
    ?FORALL(
        {Levels, Filter},
        valid_filter_with_levels(),
        emqx_topic:words(Filter) =:= Levels andalso emqx_topic:join(Levels) =:= Filter
    ).

%% A valid filter is a wildcard filter when one of its levels is + or #, and the binary and
%% the list form agree.
prop_wildcard() ->
    ?FORALL(
        {Levels, Filter},
        valid_filter_with_levels(),
        begin
            Expected = lists:any(fun(L) -> L =:= '+' orelse L =:= '#' end, Levels),
            emqx_topic:wildcard(Filter) =:= Expected andalso
                emqx_topic:wildcard(Levels) =:= Expected
        end
    ).

%% `match/2' on binaries gives the same result as on level lists, for a topic name against
%% a filter and for a filter against a filter.
prop_match_binary_equals_match_words() ->
    ?FORALL(
        {Name, Filter},
        {oneof([valid_name(), valid_filter()]), valid_filter()},
        emqx_topic:match(Name, Filter) =:=
            emqx_topic:match(emqx_topic:words(Name), emqx_topic:words(Filter))
    ).

%% A topic name built from a filter by filling in its wildcards matches the filter, unless
%% the name starts with $ and the first level of the filter is a wildcard.
prop_match_filled_filter() ->
    ?FORALL(
        {Filter, Name},
        filter_with_matching_name(),
        begin
            [First | _] = emqx_topic:words(Filter),
            Expected =
                case Name of
                    <<$$, _/binary>> -> First =/= '+' andalso First =/= '#';
                    _ -> true
                end,
            emqx_topic:match(Name, Filter) =:= Expected
        end
    ).

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

%% A level without wildcards: 0 to 12 bytes, ASCII and multi-byte UTF-8 characters.
word() ->
    ?LET(
        Chars,
        list(oneof([oneof(?WORD_BYTES), <<"é"/utf8>>, <<"€"/utf8>>, <<"😀"/utf8>>])),
        iolist_to_binary(lists:sublist(Chars, 12))
    ).

level() ->
    frequency([{6, word()}, {2, '+'}, {1, ''}]).

%% A valid filter as `{Levels, Binary}'.
valid_filter_with_levels() ->
    ?LET(
        {Levels0, Last},
        {non_empty(list(level())), frequency([{2, '#'}, {3, none}])},
        begin
            Levels1 = [to_level(L) || L <- lists:sublist(Levels0, 8)],
            Levels =
                case Last of
                    '#' -> Levels1 ++ ['#'];
                    none -> Levels1
                end,
            {Levels, join_levels(Levels)}
        end
    ).

valid_filter() ->
    ?LET({_Levels, Filter}, valid_filter_with_levels(), Filter).

valid_name() ->
    ?LET(
        Levels,
        non_empty(list(word())),
        join_levels([to_level(L) || L <- lists:sublist(Levels, 9)])
    ).

%% A valid filter and a topic name that fills in its wildcards.
filter_with_matching_name() ->
    ?LET(
        {{Levels, Filter}, Fill, Tail},
        {valid_filter_with_levels(), list(word()), list(word())},
        begin
            {NameLevels, _} = lists:foldl(
                fun
                    ('#', {Acc, Words}) ->
                        {Acc ++ [to_level(W) || W <- lists:sublist(Tail, 3)], Words};
                    ('+', {Acc, [W | Words]}) ->
                        {Acc ++ [to_level(W)], Words};
                    ('+', {Acc, []}) ->
                        {Acc ++ [<<"x">>], []};
                    (L, {Acc, Words}) ->
                        {Acc ++ [L], Words}
                end,
                {[], Fill},
                Levels
            ),
            {Filter, join_levels(NameLevels)}
        end
    ).

%% A topic made of valid and invalid pieces: wildcards inside a level, NUL, bad UTF-8, a #
%% that is not the last level. Most generated topics are invalid.
raw_topic() ->
    ?LET(
        Pieces,
        non_empty(
            list(
                frequency([
                    {8, word()},
                    {6, <<"/">>},
                    {3, <<"+">>},
                    {3, <<"#">>},
                    {1, <<0>>},
                    {1, <<16#FF>>},
                    {1, <<16#C3>>},
                    {1, <<"$">>}
                ])
            )
        ),
        iolist_to_binary(lists:sublist(Pieces, 24))
    ).

to_level(<<>>) -> '';
to_level(L) -> L.

join_levels(Levels) ->
    iolist_to_binary(lists:join($/, [level_bin(L) || L <- Levels])).

level_bin('') -> <<>>;
level_bin('+') -> <<"+">>;
level_bin('#') -> <<"#">>;
level_bin(B) -> B.

%%--------------------------------------------------------------------
%% Reference checks
%%--------------------------------------------------------------------

accepts(Kind, Topic) ->
    try
        emqx_topic:validate({Kind, Topic})
    catch
        error:_ -> false
    end.

reference_valid_filter(Topic) ->
    Levels = binary:split(Topic, <<"/">>, [global]),
    valid_bytes(Topic) andalso
        lists:all(fun valid_filter_level/1, Levels) andalso
        hash_only_last(Levels).

reference_valid_name(Topic) ->
    valid_bytes(Topic) andalso
        binary:match(Topic, [<<"+">>, <<"#">>]) =:= nomatch.

valid_bytes(Topic) ->
    Topic =/= <<>> andalso
        binary:match(Topic, <<0>>) =:= nomatch andalso
        unicode:characters_to_binary(Topic, utf8, utf8) =:= Topic.

valid_filter_level(<<"+">>) -> true;
valid_filter_level(<<"#">>) -> true;
valid_filter_level(Level) -> binary:match(Level, [<<"+">>, <<"#">>]) =:= nomatch.

hash_only_last([]) -> true;
hash_only_last([<<"#">>]) -> true;
hash_only_last([<<"#">> | _]) -> false;
hash_only_last([_ | Rest]) -> hash_only_last(Rest).
