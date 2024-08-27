%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_precondition).
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([verify/3]).
-export([matches/2]).

-export_type([matcher/0, mismatch/0]).

-type matcher() :: #message_matcher{}.
-type mismatch() :: emqx_types:message() | not_found.

-callback lookup_message(_Ctx, matcher()) ->
    emqx_types:message() | not_found | emqx_ds:error(_).

%%

-spec verify(module(), _Ctx, [emqx_ds:precondition()]) ->
    ok | {precondition_failed, mismatch()} | emqx_ds:error(_).
verify(Mod, Ctx, [_Precondition = {Cond, Msg} | Rest]) ->
    case verify_precondition(Mod, Ctx, Cond, Msg) of
        ok ->
            verify(Mod, Ctx, Rest);
        Failed ->
            Failed
    end;
verify(_Mod, _Ctx, []) ->
    ok.

verify_precondition(Mod, Ctx, if_exists, Matcher) ->
    case Mod:lookup_message(Ctx, Matcher) of
        Msg = #message{} ->
            verify_match(Msg, Matcher);
        not_found ->
            {precondition_failed, not_found};
        Error = {error, _, _} ->
            Error
    end;
verify_precondition(Mod, Ctx, unless_exists, Matcher) ->
    case Mod:lookup_message(Ctx, Matcher) of
        Msg = #message{} ->
            verify_nomatch(Msg, Matcher);
        not_found ->
            ok;
        Error = {error, _, _} ->
            Error
    end.

verify_match(Msg, Matcher) ->
    case matches(Msg, Matcher) of
        true -> ok;
        false -> {precondition_failed, Msg}
    end.

verify_nomatch(Msg, Matcher) ->
    case matches(Msg, Matcher) of
        false -> ok;
        true -> {precondition_failed, Msg}
    end.

-spec matches(emqx_types:message(), matcher()) -> boolean().
matches(
    Message,
    #message_matcher{from = From, topic = Topic, payload = Pat, headers = Headers}
) ->
    case Message of
        #message{from = From, topic = Topic} when Pat =:= '_' ->
            matches_headers(Message, Headers);
        #message{from = From, topic = Topic, payload = Pat} ->
            matches_headers(Message, Headers);
        _ ->
            false
    end.

matches_headers(_Message, MatchHeaders) when map_size(MatchHeaders) =:= 0 ->
    true;
matches_headers(#message{headers = Headers}, MatchHeaders) ->
    maps:intersect(MatchHeaders, Headers) =:= MatchHeaders.

%% Basic tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-compile(nowarn_export_all).

conjunction_test() ->
    %% Contradictory preconditions, always false.
    Preconditions = [
        {if_exists, matcher(<<"c1">>, <<"t/1">>, 0, '_')},
        {unless_exists, matcher(<<"c1">>, <<"t/1">>, 0, '_')}
    ],
    ?assertEqual(
        {precondition_failed, not_found},
        verify(?MODULE, [], Preconditions)
    ),
    %% Check that the order does not matter.
    ?assertEqual(
        {precondition_failed, not_found},
        verify(?MODULE, [], lists:reverse(Preconditions))
    ),
    ?assertEqual(
        {precondition_failed, message(<<"c1">>, <<"t/1">>, 0, <<>>)},
        verify(
            ?MODULE,
            [message(<<"c1">>, <<"t/1">>, 0, <<>>)],
            Preconditions
        )
    ).

matches_test() ->
    ?assert(
        matches(
            message(<<"mtest1">>, <<"t/same">>, 12345, <<?MODULE_STRING>>),
            matcher(<<"mtest1">>, <<"t/same">>, 12345, '_')
        )
    ).

matches_headers_test() ->
    ?assert(
        matches(
            message(<<"mtest2">>, <<"t/same">>, 23456, <<?MODULE_STRING>>, #{h1 => 42, h2 => <<>>}),
            matcher(<<"mtest2">>, <<"t/same">>, 23456, '_', #{h2 => <<>>})
        )
    ).

mismatches_headers_test() ->
    ?assertNot(
        matches(
            message(<<"mtest3">>, <<"t/same">>, 23456, <<?MODULE_STRING>>, #{h1 => 42, h2 => <<>>}),
            matcher(<<"mtest3">>, <<"t/same">>, 23456, '_', #{h2 => <<>>, h3 => <<"required">>})
        )
    ).

matcher(ClientID, Topic, TS, Payload) ->
    matcher(ClientID, Topic, TS, Payload, #{}).

matcher(ClientID, Topic, TS, Payload, Headers) ->
    #message_matcher{
        from = ClientID,
        topic = Topic,
        timestamp = TS,
        payload = Payload,
        headers = Headers
    }.

message(ClientID, Topic, TS, Payload) ->
    message(ClientID, Topic, TS, Payload, #{}).

message(ClientID, Topic, TS, Payload, Headers) ->
    #message{
        id = <<>>,
        qos = 0,
        from = ClientID,
        topic = Topic,
        timestamp = TS,
        payload = Payload,
        headers = Headers
    }.

lookup_message(Messages, Matcher) ->
    case lists:search(fun(M) -> matches(M, Matcher) end, Messages) of
        {value, Message} ->
            Message;
        false ->
            not_found
    end.

-endif.
