%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%
-module(emqtt_topic).

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

-include("emqtt_internal.hrl").
 
-export([new/1,
		 type/1,
		 match/2,
		 validate/1,
		 triples/1,
		 words/1]).

-export([test/0]).

-define(MAX_LEN, 64*1024).

new(Name) when is_binary(Name) ->
	#topic{name=Name, node=node()}.

%% ------------------------------------------------------------------------
%% topic type: direct or wildcard
%% ------------------------------------------------------------------------
type(#topic{name=Name}) ->
	type(words(Name));
type([]) ->
	direct;
type([$#]) ->
	wildcard;
type([$+|_T]) ->
	wildcard;
type([_|T]) ->
	type(T).

%% ------------------------------------------------------------------------
%% topic match
%% ------------------------------------------------------------------------
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
validate({_, <<>>}) ->
	false;
validate({_, Topic}) when size(Topic) > ?MAX_LEN ->
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


test() ->
	true = validate({subscribe, <<"a/b/c">>}),
	true = validate({subscribe, <<"/a/b">>}),
	true = validate({subscribe, <<"/+/x">>}),
	true = validate({subscribe, <<"/a/b/c/#">>}),
	false = validate({subscribe, <<"a/#/c">>}),
	ok.

