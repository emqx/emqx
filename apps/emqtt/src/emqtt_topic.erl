%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_topic).

-author('feng@slimchat.io').

%% ------------------------------------------------------------------------
%% Topic semantics and usage
%% ------------------------------------------------------------------------
%% A topic must be at least one character long.
%%
%% Topic names are case sensitive. For example, ACCOUNTS and Accounts are two different topics.
%%
%% Topic names can include the space character. For example, Accounts payable is a valid topic.
%%
%% A leading "/" creates a distinct topic. For example, /finance is different from finance. /finance matches "+/+" and "/+", but not "+".
%%
%% Do not include the null character (Unicode \x0000) in any topic.
%%
%% The following principles apply to the construction and content of a topic tree:
%%
%% The length is limited to 64k but within that there are no limits to the number of levels in a topic tree.
%%
%% There can be any number of root nodes; that is, there can be any number of topic trees.
%% ------------------------------------------------------------------------

-include("emqtt_topic.hrl").
 
-export([new/1,
		 type/1,
		 match/2,
		 validate/1,
		 triples/1,
		 words/1]).

-define(MAX_LEN, 1024).

-spec new(Name :: binary()) -> topic().
new(Name) when is_binary(Name) ->
	#topic{name=Name, node=node()}.

%% ------------------------------------------------------------------------
%% topic type: direct or wildcard
%% ------------------------------------------------------------------------
-spec type(Topic :: topic()) -> direct | wildcard.
type(#topic{name=Name}) when is_binary(Name) ->
	type(words(Name));
type([]) -> 
	direct;
type([<<>>|T]) -> 
	type(T);
type([<<$#, _/binary>>|_]) ->
	wildcard;
type([<<$+, _/binary>>|_]) ->
	wildcard;
type([_|T]) ->
	type(T).

%% ------------------------------------------------------------------------
%% topic match
%% ------------------------------------------------------------------------
-spec match(B1 :: binary(), B2 :: binary()) -> boolean().
match(B1, B2) when is_binary(B1) and is_binary(B2) ->
	match(words(B1), words(B2));
match([], []) ->
	true;
match([H|T1], [H|T2]) ->
	match(T1, T2);
match([_H|T1], [$+|T2]) ->
	match(T1, T2);
match(_, [$#]) ->
	true;
match([_H1|_], [_H2|_]) ->
	false;
match([], [_H|_T2]) ->
	false.

%% ------------------------------------------------------------------------
%% topic validate
%% ------------------------------------------------------------------------
-spec validate({Type :: subscribe | publish, Topic :: binary()}) -> boolean().
validate({_, <<>>}) ->
	false;
validate({_, Topic}) when is_binary(Topic) and (size(Topic) > ?MAX_LEN) ->
	false;
validate({subscribe, Topic}) when is_binary(Topic) ->
	valid(words(Topic));
validate({publish, Topic}) when is_binary(Topic) ->
	Words = words(Topic),
	valid(Words) and (not include_wildcard(Words)).

triples(Topic) when is_binary(Topic) ->
	triples2(words(Topic), <<>>, []).

triples2([], <<>>, []) ->
	[{root, <<>>, <<>>}];
triples2([], _Path, Acc) ->
	lists:reverse(Acc);
triples2([A|Rest], <<>>, []) ->
	triples2(Rest, A, [{root, A, A}]);
triples2([A|Rest], Path, Acc) ->
	NewPath = case is_integer(A) of
				true -> <<Path/binary, $/, A>>;
				false -> <<Path/binary, $/, A/binary>>
			  end,
	triples2(Rest, NewPath, [{Path, A, NewPath}|Acc]).

words(Topic) when is_binary(Topic) ->
	case binary:split(Topic, <<$/>>, [global]) of
		[H|T] -> [ map_wc(H) | [ map_wc(W) || W <-T, T =/= <<>> ]];
		[] -> []
	end.

map_wc(<<"+">>) -> $+;
map_wc(<<"#">>) -> $#;
map_wc(W) -> W.

valid([<<>>|Words]) -> valid2(Words);
valid(Words) -> valid2(Words).

valid2([<<>>|_Words]) -> false;
valid2([$#]) -> true;
valid2([$#|_]) -> false;
valid2([_|Words]) -> valid2(Words);
valid2([]) -> true.

include_wildcard([]) -> false;
include_wildcard([$#|_T]) -> true;
include_wildcard([$+|_T]) -> true;
include_wildcard([_H|T]) -> include_wildcard(T).


