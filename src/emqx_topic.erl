%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% APIs
-export([ match/2
        , validate/1
        , validate/2
        , levels/1
        , triples/1
        , tokens/1
        , words/1
        , wildcard/1
        , join/1
        , prepend/2
        , feed_var/3
        , systop/1
        , parse/1
        , parse/2
        ]).

-export_type([ group/0
             , topic/0
             , word/0
             , triple/0
             ]).

-type(group() :: binary()).
-type(topic() :: binary()).
-type(word() :: '' | '+' | '#' | binary()).
-type(words() :: list(word())).
-opaque(triple() :: {root | binary(), word(), binary()}).

-define(MAX_TOPIC_LEN, 4096).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Is wildcard topic?
-spec(wildcard(topic() | words()) -> true | false).
wildcard(Topic) when is_binary(Topic) ->
    wildcard(words(Topic));
wildcard([]) ->
    false;
wildcard(['#'|_]) ->
    true;
wildcard(['+'|_]) ->
    true;
wildcard([_H|T]) ->
    wildcard(T).

%% @doc Match Topic name with filter.
-spec(match(Name, Filter) -> boolean() when
      Name   :: topic() | words(),
      Filter :: topic() | words()).
match(<<$$, _/binary>>, <<$+, _/binary>>) ->
    false;
match(<<$$, _/binary>>, <<$#, _/binary>>) ->
    false;
match(Name, Filter) when is_binary(Name), is_binary(Filter) ->
    match(words(Name), words(Filter));
match([], []) ->
    true;
match([H|T1], [H|T2]) ->
    match(T1, T2);
match([_H|T1], ['+'|T2]) ->
    match(T1, T2);
match(_, ['#']) ->
    true;
match([_H1|_], [_H2|_]) ->
    false;
match([_H1|_], []) ->
    false;
match([], [_H|_T2]) ->
    false.

%% @doc Validate topic name or filter
-spec(validate(topic() | {name | filter, topic()}) -> true).
validate(Topic) when is_binary(Topic) ->
    validate(filter, Topic);
validate({Type, Topic}) when Type =:= name; Type =:= filter ->
    validate(Type, Topic).

-spec(validate(name | filter, topic()) -> true).
validate(_, <<>>) ->
    error(empty_topic);
validate(_, Topic) when is_binary(Topic) andalso (size(Topic) > ?MAX_TOPIC_LEN) ->
    error(topic_too_long);
validate(filter, Topic) when is_binary(Topic) ->
    validate2(words(Topic));
validate(name, Topic) when is_binary(Topic) ->
    Words = words(Topic),
    validate2(Words)
        andalso (not wildcard(Words))
            orelse error(topic_name_error).

validate2([]) ->
    true;
validate2(['#']) -> % end with '#'
    true;
validate2(['#'|Words]) when length(Words) > 0 ->
    error('topic_invalid_#');
validate2([''|Words]) ->
    validate2(Words);
validate2(['+'|Words]) ->
    validate2(Words);
validate2([W|Words]) ->
    validate3(W) andalso validate2(Words).

validate3(<<>>) ->
    true;
validate3(<<C/utf8, _Rest/binary>>) when C == $#; C == $+; C == 0 ->
    error('topic_invalid_char');
validate3(<<_/utf8, Rest/binary>>) ->
    validate3(Rest).

%% @doc Topic to triples.
-spec(triples(topic()) -> list(triple())).
triples(Topic) when is_binary(Topic) ->
    triples(words(Topic), root, []).

triples([], _Parent, Acc) ->
    lists:reverse(Acc);
triples([W|Words], Parent, Acc) ->
    Node = join(Parent, W),
    triples(Words, Node, [{Parent, W, Node}|Acc]).

join(root, W) ->
    bin(W);
join(Parent, W) ->
    <<(bin(Parent))/binary, $/, (bin(W))/binary>>.

%% @doc Prepend a topic prefix.
%% Ensured to have only one / between prefix and suffix.
prepend(root, W) -> bin(W);
prepend(undefined, W) -> bin(W);
prepend(<<>>, W) -> bin(W);
prepend(Parent0, W) ->
    Parent = bin(Parent0),
    case binary:last(Parent) of
        $/ -> <<Parent/binary, (bin(W))/binary>>;
        _ -> join(Parent, W)
    end.

bin('')  -> <<>>;
bin('+') -> <<"+">>;
bin('#') -> <<"#">>;
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L).

-spec(levels(topic()) -> pos_integer()).
levels(Topic) when is_binary(Topic) ->
    length(tokens(Topic)).

-compile({inline, [tokens/1]}).
%% @doc Split topic to tokens.
-spec(tokens(topic()) -> list(binary())).
tokens(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% @doc Split Topic Path to Words
-spec(words(topic()) -> words()).
words(Topic) when is_binary(Topic) ->
    [word(W) || W <- tokens(Topic)].

word(<<>>)    -> '';
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin)     -> Bin.

%% @doc '$SYS' Topic.
systop(Name) when is_atom(Name); is_list(Name) ->
    iolist_to_binary(lists:concat(["$SYS/brokers/", node(), "/", Name]));
systop(Name) when is_binary(Name) ->
    iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/", Name]).

-spec(feed_var(binary(), binary(), binary()) -> binary()).
feed_var(Var, Val, Topic) ->
    feed_var(Var, Val, words(Topic), []).
feed_var(_Var, _Val, [], Acc) ->
    join(lists:reverse(Acc));
feed_var(Var, Val, [Var|Words], Acc) ->
    feed_var(Var, Val, Words, [Val|Acc]);
feed_var(Var, Val, [W|Words], Acc) ->
    feed_var(Var, Val, Words, [W|Acc]).

-spec(join(list(binary())) -> binary()).
join([]) ->
    <<>>;
join([W]) ->
    bin(W);
join(Words) ->
    {_, Bin} = lists:foldr(
                 fun(W, {true, Tail}) ->
                         {false, <<W/binary, Tail/binary>>};
                    (W, {false, Tail}) ->
                         {false, <<W/binary, "/", Tail/binary>>}
                 end, {true, <<>>}, [bin(W) || W <- Words]),
    Bin.

-spec(parse(topic() | {topic(), map()}) -> {topic(), #{share => binary()}}).
parse(TopicFilter) when is_binary(TopicFilter) ->
    parse(TopicFilter, #{});
parse({TopicFilter, Options}) when is_binary(TopicFilter) ->
    parse(TopicFilter, Options).

-spec(parse(topic(), map()) -> {topic(), map()}).
parse(TopicFilter = <<"$queue/", _/binary>>, #{share := _Group}) ->
    error({invalid_topic_filter, TopicFilter});
parse(TopicFilter = <<"$share/", _/binary>>, #{share := _Group}) ->
    error({invalid_topic_filter, TopicFilter});
parse(<<"$queue/", TopicFilter/binary>>, Options) ->
    parse(TopicFilter, Options#{share => <<"$queue">>});
parse(TopicFilter = <<"$share/", Rest/binary>>, Options) ->
    case binary:split(Rest, <<"/">>) of
        [_Any] -> error({invalid_topic_filter, TopicFilter});
        [ShareName, Filter] ->
            case binary:match(ShareName, [<<"+">>, <<"#">>]) of
                nomatch -> parse(Filter, Options#{share => ShareName});
                _ -> error({invalid_topic_filter, TopicFilter})
            end
    end;
parse(TopicFilter, Options) ->
    {TopicFilter, Options}.

