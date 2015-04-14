%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqtt topic.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqtt_topic).

-author('feng@emqtt.io').

-import(lists, [reverse/1]).
 
-export([match/2, validate/1, triples/1, words/1, wildcard/1]).

%-type type()   :: static | dynamic.

-type word()   :: '' | '+' | '#' | binary().

-type words()  :: list(word()).

-type triple() :: {root | binary(), word(), binary()}.

-export_type([word/0, triple/0]).

-define(MAX_TOPIC_LEN, 65535).

%%%-----------------------------------------------------------------------------
%% @doc
%% Is wildcard topic?
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec wildcard(binary()) -> true | false.
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

%%%-----------------------------------------------------------------------------
%% @doc
%% Match Topic name with filter.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec match(Name, Filter) -> boolean() when
    Name    :: binary() | words(),
    Filter  :: binary() | words().
match(Name, Filter) when is_binary(Name) and is_binary(Filter) ->
	match(words(Name), words(Filter));
match([], []) ->
	true;
match([H|T1], [H|T2]) ->
	match(T1, T2);
match([<<$$, _/binary>>|_], ['+'|_]) ->
    false;
match([_H|T1], ['+'|T2]) ->
	match(T1, T2);
match([<<$$, _/binary>>|_], ['#']) ->
    false;
match(_, ['#']) ->
	true;
match([_H1|_], [_H2|_]) ->
	false;
match([_H1|_], []) ->
	false;
match([], [_H|_T2]) ->
	false.

%%%-----------------------------------------------------------------------------
%% @doc
%% Validate Topic
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec validate({name | filter, binary()}) -> boolean().
validate({_, <<>>}) ->
	false;
validate({_, Topic}) when is_binary(Topic) and (size(Topic) > ?MAX_TOPIC_LEN) ->
	false;
validate({filter, Topic}) when is_binary(Topic) ->
	validate2(words(Topic));
validate({name, Topic}) when is_binary(Topic) ->
	Words = words(Topic),
	validate2(Words) and (not wildcard(Words)).

validate2([]) ->
    true;
validate2(['#']) -> % end with '#'
    true;
validate2(['#'|Words]) when length(Words) > 0 -> 
    false; 
validate2([''|Words]) ->
    validate2(Words);
validate2(['+'|Words]) ->
    validate2(Words);
validate2([W|Words]) ->
    case validate3(W) of
        true -> validate2(Words);
        false -> false
    end.

validate3(<<>>) ->
    true;
validate3(<<C/utf8, _Rest/binary>>) when C == $#; C == $+; C == 0 ->
    false;
validate3(<<_/utf8, Rest/binary>>) ->
    validate3(Rest).

%%%-----------------------------------------------------------------------------
%% @doc
%% Topic to Triples.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec triples(binary()) -> list(triple()).
triples(Topic) when is_binary(Topic) ->
	triples(words(Topic), root, []).

triples([], _Parent, Acc) ->
    reverse(Acc);

triples([W|Words], Parent, Acc) ->
    Node = join(Parent, W),
    triples(Words, Node, [{Parent, W, Node}|Acc]).

join(root, W) ->
    bin(W);
join(Parent, W) ->
    <<(bin(Parent))/binary, $/, (bin(W))/binary>>.

bin('')  -> <<>>;
bin('+') -> <<"+">>;
bin('#') -> <<"#">>;
bin(B) when is_binary(B) -> B.

%%%-----------------------------------------------------------------------------
%% @doc
%% Split Topic to Words.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec words(binary()) -> words().
words(Topic) when is_binary(Topic) ->
    [word(W) || W <- binary:split(Topic, <<"/">>, [global])].

word(<<>>)    -> '';
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin)     -> Bin.

