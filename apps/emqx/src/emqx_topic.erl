%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic).

-include("emqx_mqtt.hrl").

%% "$share" plus the two separators around the group name.
-define(SHARE_FILTER_OVERHEAD, 8).

%% APIs
-export([
    match/2,
    match_any/2,
    is_equal/2,
    validate/1,
    validate/2,
    levels/1,
    tokens/1,
    words/1,
    wildcard/1,
    join/1,
    prepend/2,
    feed_var/3,
    systop/1,
    parse/1,
    parse/2,
    intersection/2,
    is_subset/2,
    union/1
]).

-export([
    enable_queue_alias_to_share/1
]).

-export([
    maybe_format_share/1,
    get_shared_real_topic/1,
    make_shared_record/2
]).

-type topic() :: emqx_types:topic().
-type word() :: emqx_types:word().
-type words() :: emqx_types:words().
-type share() :: emqx_types:share().

%% Guards
-define(MULTI_LEVEL_WILDCARD_NOT_LAST(C, REST),
    ((C =:= '#' orelse C =:= <<"#">>) andalso REST =/= [])
).

-define(IS_WILDCARD(W), W =:= '+' orelse W =:= '#').
-define(QUEUE_ALIAS_SUPPORTED_PT, {?MODULE, enable_queue_alias_to_share}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Is wildcard topic?
%% A level is a wildcard when it is +, or when it is # and the last level. The function does
%% not validate the topic. A caller that gets the topic from a client or a user must call
%% validate/1 or validate/2.
-spec wildcard(topic() | share() | words()) -> true | false.
wildcard(#share{topic = Topic}) when is_binary(Topic) ->
    wildcard(Topic);
wildcard(Topic) when is_binary(Topic) ->
    is_wildcard(Topic);
wildcard([]) ->
    false;
wildcard(['#']) ->
    true;
wildcard(['+' | _]) ->
    true;
wildcard([_H | T]) ->
    wildcard(T).

-define(IS_WILDCARD_BYTE(C), (C =:= $+ orelse C =:= $#)).

%% SWAR constants: the same byte in each of the 7 bytes of a 56-bit word, or in each of the
%% 5 bytes of a 40-bit word.
-define(PLUS_BYTES7, 16#2B2B2B2B2B2B2B).
-define(HASH_BYTES7, 16#23232323232323).
-define(SLASH_BYTES7, 16#2F2F2F2F2F2F2F).
-define(LOW7_BYTES7, 16#7F7F7F7F7F7F7F).
-define(HIGH_BYTES7, 16#80808080808080).
-define(PLUS_BYTES5, 16#2B2B2B2B2B).
-define(SLASH_BYTES5, 16#2F2F2F2F2F).
-define(LOW7_BYTES5, 16#7F7F7F7F7F).
-define(HIGH_BYTES5, 16#8080808080).
-define(MASK_BYTES5, 16#FFFFFFFFFF).

%% True when the 56-bit integer V has a zero byte. For each byte B,
%% ((B band 16#7F) + 16#7F) bor B has the high bit set when B is not zero. The sum is at most
%% 16#FE, so no carry goes into the next byte.
-define(HAS_ZERO_BYTE7(V),
    (((((V) band ?LOW7_BYTES7) + ?LOW7_BYTES7) bor (V)) band ?HIGH_BYTES7 =/= ?HIGH_BYTES7)
).
-define(HAS_PLUS_BYTE7(W), ?HAS_ZERO_BYTE7((W) bxor ?PLUS_BYTES7)).
-define(HAS_HASH_BYTE7(W), ?HAS_ZERO_BYTE7((W) bxor ?HASH_BYTES7)).

%% True when the byte C is not +, # or NUL, and not above 127.
-define(IS_PLAIN_BYTE(C), (C > 0 andalso C < 16#80 andalso C =/= $+ andalso C =/= $#)).

%% True when the 56-bit integer W has no +, # or NUL byte and no byte above 127.
-define(IS_PLAIN7(W),
    ((W) band ?HIGH_BYTES7 =:= 0 andalso
        not ?HAS_ZERO_BYTE7(W) andalso
        not ?HAS_PLUS_BYTE7(W) andalso
        not ?HAS_HASH_BYTE7(W))
).

%% A level is a wildcard when it is exactly +, or when it is # and the last level.
is_wildcard(<<>>) ->
    false;
is_wildcard(<<C>>) ->
    ?IS_WILDCARD_BYTE(C);
is_wildcard(Topic) ->
    ends_with_wildcard_level(Topic) orelse has_plus_level(Topic).

%% Topic has at least 2 bytes.
-compile({inline, [ends_with_wildcard_level/1]}).
ends_with_wildcard_level(Topic) ->
    Size = byte_size(Topic) - 2,
    case Topic of
        <<_:Size/binary, $/, C>> -> ?IS_WILDCARD_BYTE(C);
        _ -> false
    end.

%% Topic has at least 2 bytes, and ends_with_wildcard_level/1 checked the last level.
-compile({inline, [has_plus_level/1]}).
has_plus_level(<<$+, $/, _/binary>>) ->
    true;
has_plus_level(Topic) ->
    find_plus_level(Topic).

%% Find "/+/". Skip 6 bytes when a 7-byte word has no + byte. When it has one, check the 5
%% three-byte windows of the word at once, and step 5 bytes.
find_plus_level(<<W:56, _/binary>> = Bin) ->
    case ?HAS_PLUS_BYTE7(W) of
        false ->
            <<_:48, Rest/binary>> = Bin,
            find_plus_level(Rest);
        true ->
            has_slash_plus_slash7(W) orelse
                begin
                    <<_:40, Rest/binary>> = Bin,
                    find_plus_level(Rest)
                end
    end;
find_plus_level(Bin) ->
    check_plus_level(Bin).

check_plus_level(<<$/, $+, $/, _/binary>>) ->
    true;
check_plus_level(<<_, Rest/binary>>) ->
    check_plus_level(Rest);
check_plus_level(<<>>) ->
    false.

%% Return true when the 56-bit integer W has "/+/" in its bytes.
%% Byte K of V (K = 0..4) is zero when bytes K+2, K+1 and K of W are $/, $+ and $/.
%% Byte K+2 comes first in the topic. The zero-byte test is the one in ?HAS_ZERO_BYTE7.
-compile({inline, [has_slash_plus_slash7/1]}).
has_slash_plus_slash7(W) ->
    V =
        (((W bsr 16) bxor ?SLASH_BYTES5) bor
            ((W bsr 8) bxor ?PLUS_BYTES5) bor
            (W bxor ?SLASH_BYTES5)) band ?MASK_BYTES5,
    NonZero = (((V band ?LOW7_BYTES5) + ?LOW7_BYTES5) bor V) band ?HIGH_BYTES5,
    NonZero =/= ?HIGH_BYTES5.

%% @doc Match Topic name with filter.
-spec match(Name, Filter) -> boolean() when
    Name :: topic() | share() | words(),
    Filter :: topic() | words().
match(<<$$, _/binary>>, <<$+, _/binary>>) ->
    false;
match(<<$$, _/binary>>, <<$#, _/binary>>) ->
    false;
match(Name, Filter) when is_binary(Name), is_binary(Filter) ->
    match_levels(Filter, Name);
match(#share{} = Name, Filter) ->
    match_share(Name, Filter);
match(Name, #share{} = Filter) ->
    match_share(Name, Filter);
match(Name, Filter) when is_binary(Name) ->
    match_words(words(Name), Filter);
match(Name, Filter) when is_binary(Filter) ->
    match_words(Name, words(Filter));
match(Name, Filter) ->
    match_words(Name, Filter).

%% Match the binary Name against the binary Filter level by level, without splitting them.
%% Name is `done` when all its levels are matched. The order of the checks is the same as in
%% match_tokens/2: a filter level equal to the name level is matched first, then + and #.
%% Both arguments must be valid topic names or filters (see validate/1): a + or # byte that is
%% not a whole level can give a different result than match_tokens/2.
match_levels(<<"#">>, <<"#/", _/binary>>) ->
    false;
match_levels(<<"#">>, _Name) ->
    true;
match_levels(_Filter, done) ->
    false;
match_levels(<<"+">>, Name) ->
    is_last_level(Name);
match_levels(<<"+/", FilterRest/binary>>, Name) ->
    match_levels(FilterRest, skip_level(Name));
match_levels(Filter, Name) ->
    match_level(Filter, Name).

%% Compare one level, then go on with the next level. 7 equal bytes are skipped at once: in a
%% valid topic filter, + and # are whole levels, so 7 equal bytes in the filter and the name
%% are literal bytes of both. When the 7 bytes end with $/, the next level starts.
match_level(<<W:56, FilterRest/binary>>, <<W:56, NameRest/binary>>) ->
    case W band 16#FF of
        $/ -> match_levels(FilterRest, NameRest);
        _ -> match_level(FilterRest, NameRest)
    end;
match_level(<<$/, FilterRest/binary>>, <<$/, NameRest/binary>>) ->
    match_levels(FilterRest, NameRest);
match_level(<<C, FilterRest/binary>>, <<C, NameRest/binary>>) ->
    match_level(FilterRest, NameRest);
match_level(<<>>, <<>>) ->
    true;
match_level(<<$/, FilterRest/binary>>, <<>>) ->
    match_levels(FilterRest, done);
match_level(_Filter, _Name) ->
    false.

is_last_level(Name) ->
    skip_level(Name) =:= done.

%% Return the levels after the first one, or `done` when there is only one.
skip_level(<<$/, Rest/binary>>) ->
    Rest;
skip_level(<<_, Rest/binary>>) ->
    skip_level(Rest);
skip_level(<<>>) ->
    done.

match_words([<<$$, _/binary>> | _], [W | _]) when ?IS_WILDCARD(W) ->
    false;
match_words(Name, Filter) ->
    match_tokens(Name, Filter).

match_tokens([], []) ->
    true;
match_tokens([H | T1], [H | T2]) ->
    match_tokens(T1, T2);
match_tokens([_H | T1], ['+' | T2]) ->
    match_tokens(T1, T2);
match_tokens([<<>> | T1], ['' | T2]) ->
    match_tokens(T1, T2);
match_tokens(_, ['#']) ->
    true;
match_tokens(_, _) ->
    false.

-spec is_equal(T, T) -> boolean() when T :: topic() | words().
is_equal(T1, T2) when is_binary(T1) andalso is_binary(T2) ->
    T1 =:= T2;
is_equal(T1, T2) when is_list(T1) andalso is_list(T2) ->
    T1 =:= T2;
is_equal(T1, T2) when is_list(T1) ->
    join(T1) =:= T2;
is_equal(T1, T2) when is_list(T2) ->
    join(T2) =:= T1.

%% @doc Finds an intersection between two topics, two filters or a topic and a filter.
%% The function is commutative: reversing parameters doesn't affect the returned value.
%% Two topics intersect only when they are equal.
%% The intersection of a topic and a filter is always either the topic itself or false (no intersection).
%% The intersection of two filters is either false or a new topic filter that would match only those topics,
%% that can be matched by both input filters.
%% For example, the intersection of "t/global/#" and "t/+/1/+" is "t/global/1/+".
-spec intersection(TopicOrFilter, TopicOrFilter) -> topic() | false when
    TopicOrFilter :: topic() | words().
intersection(Topic1, Topic2) when is_binary(Topic1) ->
    intersection(words(Topic1), Topic2);
intersection(Topic1, Topic2) when is_binary(Topic2) ->
    intersection(Topic1, words(Topic2));
intersection(Topic1, Topic2) ->
    case intersect_start(Topic1, Topic2) of
        false -> false;
        Intersection -> join(Intersection)
    end.

intersect_start([<<"$", _/bytes>> | _], [W | _]) when ?IS_WILDCARD(W) ->
    false;
intersect_start([W | _], [<<"$", _/bytes>> | _]) when ?IS_WILDCARD(W) ->
    false;
intersect_start(Words1, Words2) ->
    intersect(Words1, Words2).

intersect(Words1, ['#']) ->
    Words1;
intersect(['#'], Words2) ->
    Words2;
intersect([W1], ['+']) ->
    [W1];
intersect(['+'], [W2]) ->
    [W2];
intersect([W1 | T1], [W2 | T2]) when ?IS_WILDCARD(W1), ?IS_WILDCARD(W2) ->
    intersect_join(wildcard_intersection(W1, W2), intersect(T1, T2));
intersect([W | T1], [W | T2]) ->
    intersect_join(W, intersect(T1, T2));
intersect([W1 | T1], [W2 | T2]) when ?IS_WILDCARD(W1) ->
    intersect_join(W2, intersect(T1, T2));
intersect([W1 | T1], [W2 | T2]) when ?IS_WILDCARD(W2) ->
    intersect_join(W1, intersect(T1, T2));
intersect([], []) ->
    [];
intersect(_, _) ->
    false.

intersect_join(_, false) -> false;
intersect_join(W, Words) -> [W | Words].

wildcard_intersection(W, W) -> W;
wildcard_intersection(_, _) -> '+'.

%% @doc Finds out if topic / topic filter T1 is a subset of topic / topic filter T2.
-spec is_subset(topic() | words(), topic() | words()) -> boolean().
is_subset(T1, T1) ->
    true;
is_subset(T1, T2) when is_binary(T1) ->
    intersection(T1, T2) =:= T1;
is_subset(T1, T2) ->
    intersection(T1, T2) =:= join(T1).

%% @doc Compute the smallest set of topics / topic filters that contain (have as a
%% subset) each given topic / topic filter.
%% Resulting set is not optimal, i.e. it's still possible to have a pair of topic
%% filters with non-empty intersection.
-spec union(_Set :: [topic() | words()]) -> [topic() | words()].
union([Filter | Filters]) ->
    %% Drop filters completely covered by `Filter`.
    Disjoint = [F || F <- Filters, not is_subset(F, Filter)],
    %% Drop `Filter` if completely covered by another filter.
    Head = [Filter || not lists:any(fun(F) -> is_subset(Filter, F) end, Disjoint)],
    Head ++ union(Disjoint);
union([]) ->
    [].

-spec match_share(Name, Filter) -> boolean() when
    Name :: share(),
    Filter :: topic() | share().
match_share(#share{topic = Name}, Filter) when is_binary(Filter) ->
    %% only match real topic filter for normal topic filter.
    match(words(Name), words(Filter));
match_share(#share{group = Group, topic = Name}, #share{group = Group, topic = Filter}) ->
    %% Matching real topic filter When subed same share group.
    match(words(Name), words(Filter));
match_share(#share{}, _) ->
    %% Otherwise, non-matched.
    false;
match_share(Name, #share{topic = Filter}) when is_binary(Name) ->
    %% Only match real topic filter for normal topic_filter/topic_name.
    match(Name, Filter).

-spec match_any(Name, [Filter]) -> boolean() when
    Name :: topic() | words(),
    Filter :: topic() | words().
match_any(Topic, Filters) ->
    lists:any(fun(Filter) -> match(Topic, Filter) end, Filters).

%% @doc Validate topic name or filter
-spec validate(topic() | share() | {name | filter, topic()}) -> true.
validate(#share{group = ShareName, topic = Filter}) when
    byte_size(ShareName) + byte_size(Filter) + ?SHARE_FILTER_OVERHEAD > ?MAX_TOPIC_LEN
->
    error(topic_too_long);
validate(#share{group = <<>>}) ->
    error(?SHARE_EMPTY_GROUP);
validate(#share{topic = <<>>}) ->
    error(?SHARE_EMPTY_FILTER);
validate(#share{group = ShareName, topic = Filter}) ->
    validate_share(ShareName, Filter);
validate(Topic) when is_binary(Topic) ->
    validate(filter, Topic);
validate({Type, Topic}) when Type =:= name; Type =:= filter ->
    validate(Type, Topic).

-spec validate(name | filter, topic()) -> true.
validate(_, <<>>) ->
    %% MQTT-5.0 [MQTT-4.7.3-1]
    error(empty_topic);
validate(_, Topic) when is_binary(Topic) andalso (size(Topic) > ?MAX_TOPIC_LEN) ->
    %% MQTT-5.0 [MQTT-4.7.3-3]
    error(topic_too_long);
validate(filter, SharedFilter = <<?SHARE, "/", _Rest/binary>>) ->
    validate_share(SharedFilter);
validate(filter, Filter) when is_binary(Filter) ->
    is_valid_filter($/, Filter) orelse validate2(words(Filter));
validate(name, Topic) when is_binary(Topic) ->
    is_valid_name(Topic) orelse validate_name(Topic).

validate_name(Topic) ->
    Words = words(Topic),
    validate2(Words) andalso
        (not wildcard(Words)) orelse
        error(topic_name_error).

%% Return true when Filter is a valid topic filter. Return false when it is not valid; the
%% caller then runs the full check, which raises the error.
%% Prev is the byte before Filter, or $/ at the start of the topic.
%% A + level is valid when it is a whole level. A # level is valid only as the whole last
%% level. NUL is not valid. Other bytes must be valid UTF-8.
is_valid_filter(_Prev, <<W:56, Rest/binary>>) when ?IS_PLAIN7(W) ->
    is_valid_filter(W band 16#FF, Rest);
is_valid_filter(_Prev, <<C, Rest/binary>>) when ?IS_PLAIN_BYTE(C) ->
    is_valid_filter(C, Rest);
is_valid_filter($/, <<$+>>) ->
    true;
is_valid_filter($/, <<$+, $/, Rest/binary>>) ->
    is_valid_filter($/, Rest);
is_valid_filter($/, <<$#>>) ->
    true;
is_valid_filter(_Prev, <<C, _/binary>>) when C < 16#80 ->
    false;
is_valid_filter(_Prev, <<_/utf8, Rest/binary>>) ->
    is_valid_filter(16#80, Rest);
is_valid_filter(_Prev, <<>>) ->
    true;
is_valid_filter(_Prev, _) ->
    false.

%% Return true when Topic is a valid topic name: no +, # or NUL byte, and valid UTF-8.
%% Return false when it is not valid; the caller then runs the full check.
is_valid_name(<<W:56, Rest/binary>>) when ?IS_PLAIN7(W) ->
    is_valid_name(Rest);
is_valid_name(<<C, Rest/binary>>) when ?IS_PLAIN_BYTE(C) ->
    is_valid_name(Rest);
is_valid_name(<<C, _/binary>>) when C < 16#80 ->
    false;
is_valid_name(<<_/utf8, Rest/binary>>) ->
    is_valid_name(Rest);
is_valid_name(<<>>) ->
    true;
is_valid_name(_) ->
    false.

validate2([]) ->
    true;
% end with '#'
validate2(['#']) ->
    true;
%% MQTT-5.0 [MQTT-4.7.1-1]
validate2([C | Words]) when ?MULTI_LEVEL_WILDCARD_NOT_LAST(C, Words) ->
    error('topic_invalid_#');
validate2(['' | Words]) ->
    validate2(Words);
validate2(['+' | Words]) ->
    validate2(Words);
validate2([W | Words]) ->
    validate3(W) andalso validate2(Words).

validate3(<<>>) ->
    true;
validate3(<<C/utf8, _Rest/binary>>) when C == $#; C == $+; C == 0 ->
    error('topic_invalid_char');
validate3(<<_/utf8, Rest/binary>>) ->
    validate3(Rest).

validate_share(<<?SHARE, "/", Rest/binary>>) when
    Rest =:= <<>> orelse Rest =:= <<"/">>
->
    %% MQTT-5.0 [MQTT-4.8.2-1]
    error(?SHARE_EMPTY_FILTER);
validate_share(<<?SHARE, "/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        %% MQTT-5.0 [MQTT-4.8.2-1]
        [<<>>, _] ->
            error(?SHARE_EMPTY_GROUP);
        %% MQTT-5.0 [MQTT-4.7.3-1]
        [_, <<>>] ->
            error(?SHARE_EMPTY_FILTER);
        [ShareName, Filter] ->
            validate_share(ShareName, Filter)
    end.

validate_share(_, <<?SHARE, "/", _Rest/binary>>) ->
    error(?SHARE_RECURSIVELY);
validate_share(ShareName, Filter) ->
    case binary:match(ShareName, [<<"+">>, <<"#">>]) of
        %% MQTT-5.0 [MQTT-4.8.2-2]
        nomatch -> is_valid_filter($/, Filter) orelse validate2(words(Filter));
        _ -> error(?SHARE_NAME_INVALID_CHAR)
    end.

%% @doc Prepend a topic prefix.
%% Ensured to have only one / between prefix and suffix.
prepend(undefined, W) ->
    bin(W);
prepend(<<>>, W) ->
    bin(W);
prepend(Parent0, W) ->
    Parent = bin(Parent0),
    case binary:last(Parent) of
        $/ -> <<Parent/binary, (bin(W))/binary>>;
        _ -> <<Parent/binary, $/, (bin(W))/binary>>
    end.

-spec bin(word()) -> binary().
bin('') -> <<>>;
bin('+') -> <<"+">>;
bin('#') -> <<"#">>;
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L).

-spec levels(topic() | share()) -> pos_integer().
levels(#share{topic = Topic}) when is_binary(Topic) ->
    levels(Topic);
levels(Topic) when is_binary(Topic) ->
    count_levels(Topic, 1).

%% Count the $/ bytes 7 at a time, then one at a time.
count_levels(<<W:56, Rest/binary>>, N) -> count_levels(Rest, N + count_slashes7(W));
count_levels(<<$/, Rest/binary>>, N) -> count_levels(Rest, N + 1);
count_levels(<<_, Rest/binary>>, N) -> count_levels(Rest, N);
count_levels(<<>>, N) -> N.

%% Return the number of $/ bytes in the 56-bit integer W.
%% 1. XOR with 16#2F in each byte makes each $/ byte zero.
%% 2. For each byte B, ((B band 16#7F) + 16#7F) bor B has the high bit set when B is not
%%    zero. The sum is at most 16#FE, so no carry goes into the next byte.
%% 3. Move the high bit of each zero byte to bit 0 of that byte, and add the 7 bytes.
%% All values are below 2^57, so they are small integers.
-compile({inline, [count_slashes7/1]}).
count_slashes7(W) ->
    V = W bxor ?SLASH_BYTES7,
    NonZero = (((V band ?LOW7_BYTES7) + ?LOW7_BYTES7) bor V) band ?HIGH_BYTES7,
    case NonZero bxor ?HIGH_BYTES7 of
        0 ->
            0;
        Zero ->
            X = Zero bsr 7,
            S1 = X + (X bsr 8),
            S2 = S1 + (S1 bsr 16),
            (S2 + (S2 bsr 32)) band 16#FF
    end.

%% @doc Split topic to tokens.
-spec tokens(topic()) -> [binary(), ...].
tokens(Topic) ->
    split_levels(Topic, 0, Topic, []).

%% `Level` is the binary from the start of the current level, `N` is the number of bytes
%% of the current level seen so far.
split_levels(<<$/, Rest/binary>>, N, Level, Acc) ->
    <<Token:N/binary, _/binary>> = Level,
    split_levels(Rest, 0, Rest, [Token | Acc]);
split_levels(<<_, Rest/binary>>, N, Level, Acc) ->
    split_levels(Rest, N + 1, Level, Acc);
split_levels(<<>>, _N, Level, Acc) ->
    lists:reverse(Acc, [Level]).

%% @doc Split Topic Path to Words
-spec words(topic() | share()) -> words().
words(#share{topic = Topic}) when is_binary(Topic) ->
    words(Topic);
words(Topic) when is_binary(Topic) ->
    split_words(Topic, 0, Topic, []).

%% The same scan as split_levels/4, but each level becomes a word.
split_words(<<$/, Rest/binary>>, N, Level, Acc) ->
    <<Token:N/binary, _/binary>> = Level,
    split_words(Rest, 0, Rest, [word(Token) | Acc]);
split_words(<<_, Rest/binary>>, N, Level, Acc) ->
    split_words(Rest, N + 1, Level, Acc);
split_words(<<>>, _N, Level, Acc) ->
    lists:reverse(Acc, [word(Level)]).

-spec word(binary()) -> word().
-compile({inline, [word/1]}).
word(<<>>) -> '';
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin) -> Bin.

%% @doc '$SYS' Topic.
-spec systop(atom() | string() | binary()) -> topic().
systop(Name) when is_atom(Name); is_list(Name) ->
    iolist_to_binary(lists:concat(["$SYS/brokers/", node(), "/", Name]));
systop(Name) when is_binary(Name) ->
    iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/", Name]).

-spec feed_var(binary(), binary(), binary()) -> binary().
feed_var(Var, Val, Topic) ->
    feed_var(Var, Val, words(Topic), []).
feed_var(_Var, _Val, [], Acc) ->
    join(lists:reverse(Acc));
feed_var(Var, Val, [Var | Words], Acc) ->
    feed_var(Var, Val, Words, [Val | Acc]);
feed_var(Var, Val, [W | Words], Acc) ->
    feed_var(Var, Val, Words, [W | Acc]).

-spec join(list(word())) -> binary().
join([]) ->
    <<>>;
join([Word | Words]) ->
    iolist_to_binary([bin(Word) | join_rest(Words)]).

join_rest([]) ->
    [];
%% MQTT-5.0 [MQTT-4.7.1-1]
join_rest([C | Words]) when ?MULTI_LEVEL_WILDCARD_NOT_LAST(C, Words) ->
    error('topic_invalid_#');
join_rest([Word | Words]) ->
    [$/, bin(Word) | join_rest(Words)].

-spec parse(TF | {TF, map()}) -> {TF, map()} when
    TF :: topic() | share().
parse(TopicFilter) when ?IS_TOPIC(TopicFilter) ->
    parse(TopicFilter, #{});
parse({TopicFilter, Options}) when ?IS_TOPIC(TopicFilter) ->
    parse(TopicFilter, Options).

-spec parse(topic() | share(), map()) -> {topic() | share(), map()}.
%% <<"$queue/[real_topic_filter]>">> equivalent to <<"$share/$queue/[real_topic_filter]">>
%% So the head of `real_topic_filter` MUST NOT be `<<$queue>>` or `<<$share>>`
parse(#share{topic = Topic = <<?QUEUE, "/", _/binary>>}, _Options) ->
    error({invalid_topic_filter, Topic});
parse(#share{topic = Topic = <<?SHARE, "/", _/binary>>}, _Options) ->
    error({invalid_topic_filter, Topic});
parse(#share{} = T, #{nl := 1} = _Options) ->
    %% Protocol Error and Should Disconnect
    %% MQTT-5.0 [MQTT-3.8.3-4] and [MQTT-4.13.1-1]
    error({invalid_subopts_nl, maybe_format_share(T)});
parse(<<?QUEUE, "/", Topic/binary>> = FullTopic, Options) ->
    case is_queue_alias_to_share_enabled() of
        true ->
            parse(#share{group = <<?QUEUE>>, topic = Topic}, Options);
        false ->
            {FullTopic, Options}
    end;
parse(TopicFilter = <<?SHARE, "/", Rest/binary>>, Options) ->
    case binary:split(Rest, <<"/">>) of
        [_Any] ->
            error({invalid_topic_filter, TopicFilter});
        %% `Group` could be `$share` or `$queue`
        [Group, Topic] ->
            case binary:match(Group, [<<"+">>, <<"#">>]) of
                nomatch -> parse(#share{group = Group, topic = Topic}, Options);
                _ -> error({invalid_topic_filter, TopicFilter})
            end
    end;
parse(TopicFilter = <<"$exclusive/", Topic/binary>>, Options) ->
    case Topic of
        <<>> ->
            error({invalid_topic_filter, TopicFilter});
        _ ->
            {Topic, Options#{is_exclusive => true}}
    end;
parse(TopicFilter, Options) when
    ?IS_TOPIC(TopicFilter)
->
    {TopicFilter, Options}.

get_shared_real_topic(#share{topic = TopicFilter}) ->
    TopicFilter;
get_shared_real_topic(TopicFilter) when is_binary(TopicFilter) ->
    TopicFilter.

make_shared_record(Group, Topic) ->
    #share{group = Group, topic = Topic}.

maybe_format_share(#share{group = <<?QUEUE>>, topic = Topic}) ->
    join([<<?QUEUE>>, Topic]);
maybe_format_share(#share{group = Group, topic = Topic}) ->
    join([<<?SHARE>>, Group, Topic]);
maybe_format_share(Topic) ->
    join([Topic]).

-spec enable_queue_alias_to_share(boolean()) -> ok.
enable_queue_alias_to_share(Enable) ->
    persistent_term:put(?QUEUE_ALIAS_SUPPORTED_PT, Enable).

is_queue_alias_to_share_enabled() ->
    %% For backward compatibility, defaults to true.
    persistent_term:get(?QUEUE_ALIAS_SUPPORTED_PT, true).
