
-module(emqtt_topic_tests).

-include("emqtt_topic.hrl").

-import(emqtt_topic, [validate/1, type/1, match/2, triples/1, words/1]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

validate_test() ->
	?assert( validate({subscribe, <<"a/b/c">>}) ),
	?assert( validate({subscribe, <<"/a/b">>}) ),
	?assert( validate({subscribe, <<"/+/x">>}) ),
	?assert( validate({subscribe, <<"/a/b/c/#">>}) ),
	?assertNot( validate({subscribe, <<"a/#/c">>}) ).

type_test() ->
	?assertEqual(direct, type(#topic{name = <<"/a/b/cdkd">>})),
	?assertEqual(wildcard, type(#topic{name = <<"/a/+/d">>})),
	?assertEqual(wildcard, type(#topic{name = <<"/a/b/#">>})).

-endif.

