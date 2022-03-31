%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Copyright (c) 2015 Petr Gotthard <petr.gotthard@centrum.cz>

%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% convenience functions for message construction
-module(emqx_coap_message).

-export([
    request/2, request/3, request/4,
    ack/1,
    response/1, response/2,
    reset/1,
    piggyback/2, piggyback/3,
    response/3
]).

-export([is_request/1]).

-export([
    set/3,
    set_payload/2,
    get_option/2,
    get_option/3,
    set_payload_block/3, set_payload_block/4
]).

-include("src/coap/include/emqx_coap.hrl").

request(Type, Method) ->
    request(Type, Method, <<>>, []).

request(Type, Method, Payload) ->
    request(Type, Method, Payload, []).

request(Type, Method, Payload, Options) when is_binary(Payload) ->
    #coap_message{
        type = Type,
        method = Method,
        payload = Payload,
        options = to_options(Options)
    }.

ack(#coap_message{id = Id}) ->
    #coap_message{type = ack, id = Id}.

reset(#coap_message{id = Id}) ->
    #coap_message{type = reset, id = Id}.

%% just make a response
response(Request) ->
    response(undefined, Request).

response(Method, Request) ->
    response(Method, <<>>, Request).

response(Method, Payload, #coap_message{
    type = Type,
    id = Id,
    token = Token
}) ->
    #coap_message{
        type = Type,
        id = Id,
        token = Token,
        method = Method,
        payload = Payload
    }.

%% make a response which maybe is a piggyback ack
piggyback(Method, Request) ->
    piggyback(Method, <<>>, Request).

piggyback(Method, Payload, Request) ->
    Reply = response(Method, Payload, Request),
    case Reply of
        #coap_message{type = con} ->
            Reply#coap_message{type = ack};
        _ ->
            Reply
    end.

%% omit option for its default value
set(max_age, ?DEFAULT_MAX_AGE, Msg) ->
    Msg;
%% set non-default value
set(Option, Value, Msg = #coap_message{options = Options}) ->
    Msg#coap_message{options = Options#{Option => Value}}.

get_option(Option, Msg) ->
    get_option(Option, Msg, undefined).

get_option(Option, #coap_message{options = Options}, Def) ->
    maps:get(Option, Options, Def).

set_payload(Payload, Msg) when is_binary(Payload) ->
    Msg#coap_message{payload = Payload};
set_payload(Payload, Msg) when is_list(Payload) ->
    Msg#coap_message{payload = list_to_binary(Payload)}.

set_payload_block(Content, Block, Msg = #coap_message{method = Method}) when is_atom(Method) ->
    set_payload_block(Content, block1, Block, Msg);
set_payload_block(Content, Block, Msg = #coap_message{}) ->
    set_payload_block(Content, block2, Block, Msg).

set_payload_block(Content, BlockId, {Num, _, Size}, Msg) ->
    ContentSize = erlang:byte_size(Content),
    OffsetBegin = Size * Num,
    OffsetEnd = OffsetBegin + Size,
    case ContentSize > OffsetEnd of
        true ->
            set(
                BlockId,
                {Num, true, Size},
                set_payload(binary:part(Content, OffsetBegin, Size), Msg)
            );
        _ ->
            set(
                BlockId,
                {Num, false, Size},
                set_payload(binary:part(Content, OffsetBegin, ContentSize - OffsetBegin), Msg)
            )
    end.

is_request(#coap_message{method = Method}) when is_atom(Method) ->
    Method =/= undefined;
is_request(_) ->
    false.

to_options(Opts) when is_map(Opts) ->
    Opts;
to_options(Opts) ->
    maps:from_list(Opts).
