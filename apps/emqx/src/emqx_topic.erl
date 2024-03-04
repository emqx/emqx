%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_topic).

-include("emqx_mqtt.hrl").

%% APIs
-export([
    match/2,
    match_any/2,
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
    parse/2
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

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Is wildcard topic?
-spec wildcard(topic() | share() | words()) -> true | false.
wildcard(#share{topic = Topic}) when is_binary(Topic) ->
    wildcard(Topic);
wildcard(Topic) when is_binary(Topic) ->
    wildcard(words(Topic));
wildcard([]) ->
    false;
wildcard(['#' | _]) ->
    true;
wildcard(['+' | _]) ->
    true;
wildcard([_H | T]) ->
    wildcard(T).

%% @doc Match Topic name with filter.
-spec match(Name, Filter) -> boolean() when
    Name :: topic() | share() | words(),
    Filter :: topic() | words().
match(<<$$, _/binary>>, <<$+, _/binary>>) ->
    false;
match(<<$$, _/binary>>, <<$#, _/binary>>) ->
    false;
match(Name, Filter) when is_binary(Name), is_binary(Filter) ->
    match(words(Name), words(Filter));
match(#share{} = Name, Filter) ->
    match_share(Name, Filter);
match(Name, #share{} = Filter) ->
    match_share(Name, Filter);
match([], []) ->
    true;
match([H | T1], [H | T2]) ->
    match(T1, T2);
match([_H | T1], ['+' | T2]) ->
    match(T1, T2);
match([<<>> | T1], ['' | T2]) ->
    match(T1, T2);
match(_, ['#']) ->
    true;
match(_, _) ->
    false.

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

%% TODO: validate share topic #share{} for emqx_trace.erl
%% @doc Validate topic name or filter
-spec validate(topic() | {name | filter, topic()}) -> true.
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
    validate2(words(Filter));
validate(name, Topic) when is_binary(Topic) ->
    Words = words(Topic),
    validate2(Words) andalso
        (not wildcard(Words)) orelse
        error(topic_name_error).

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
        nomatch -> validate2(words(Filter));
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
    length(tokens(Topic)).

-compile({inline, [tokens/1]}).
%% @doc Split topic to tokens.
-spec tokens(topic()) -> list(binary()).
tokens(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% @doc Split Topic Path to Words
-spec words(topic()) -> words().
words(#share{topic = Topic}) when is_binary(Topic) ->
    words(Topic);
words(Topic) when is_binary(Topic) ->
    [word(W) || W <- tokens(Topic)].

-spec word(binary()) -> word().
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
    do_join(bin(Word), Words).

do_join(TopicAcc, []) ->
    TopicAcc;
%% MQTT-5.0 [MQTT-4.7.1-1]
do_join(_TopicAcc, [C | Words]) when ?MULTI_LEVEL_WILDCARD_NOT_LAST(C, Words) ->
    error('topic_invalid_#');
do_join(TopicAcc, [Word | Words]) ->
    do_join(<<TopicAcc/binary, "/", (bin(Word))/binary>>, Words).

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
parse(<<?QUEUE, "/", Topic/binary>>, Options) ->
    parse(#share{group = <<?QUEUE>>, topic = Topic}, Options);
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
