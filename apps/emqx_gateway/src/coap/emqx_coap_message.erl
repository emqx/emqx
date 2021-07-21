%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Copyright (c) 2015 Petr Gotthard <petr.gotthard@centrum.cz>

%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([request/2, request/3, request/4, ack/1, response/1, response/2, response/3]).
-export([set/3, set_payload/2, get_content/1, set_content/2, set_content/3, get_option/2]).

-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

request(Type, Method) ->
    request(Type, Method, <<>>, []).

request(Type, Method, Payload) ->
    request(Type, Method, Payload, []).

request(Type, Method, Payload, Options) when is_binary(Payload) ->
    #coap_message{type = Type, method = Method, payload = Payload, options = Options};

request(Type, Method, Content=#coap_content{}, Options) ->
    set_content(Content,
                #coap_message{type = Type, method = Method, options = Options}).

ack(Request = #coap_message{}) ->
    #coap_message{type = ack,
                  id = Request#coap_message.id}.

response(#coap_message{type = Type,
                       id = Id,
                       token = Token}) ->
    #coap_message{type = Type,
                  id = Id,
                  token = Token}.

response(Method, Request) ->
    set_method(Method, response(Request)).

response(Method, Payload, Request) ->
    set_method(Method,
               set_payload(Payload,
                           response(Request))).

%% omit option for its default value
set(max_age, ?DEFAULT_MAX_AGE, Msg) -> Msg;

%% set non-default value
set(Option, Value, Msg = #coap_message{options = Options}) ->
    Msg#coap_message{options = Options#{Option => Value}}.

get_option(Option, #coap_message{options = Options}) ->
    maps:get(Option, Options, undefined).

set_method(Method, Msg) ->
    Msg#coap_message{method = Method}.

set_payload(Payload = #coap_content{}, Msg) ->
    set_content(Payload, undefined, Msg);

set_payload(Payload, Msg) when is_binary(Payload) ->
    Msg#coap_message{payload = Payload};

set_payload(Payload, Msg) when is_list(Payload) ->
    Msg#coap_message{payload = list_to_binary(Payload)}.

get_content(#coap_message{options = Options, payload = Payload}) ->
    #coap_content{etag = maps:get(etag, Options, undefined),
                  max_age = maps:get(max_age, Options, ?DEFAULT_MAX_AGE),
                  format = maps:get(content_format, Options, undefined),
                  location_path = maps:get(location_path, Options, []),
                  payload = Payload}.

set_content(Content, Msg) ->
    set_content(Content, undefined, Msg).

%% segmentation not requested and not required
set_content(#coap_content{etag = ETag,
                          max_age = MaxAge,
                          format = Format,
                          location_path = LocPath,
                          payload = Payload},
            undefined,
            Msg)
  when byte_size(Payload)  =< ?MAX_BLOCK_SIZE ->
    #coap_message{options = Options} = Msg2 = set_payload(Payload, Msg),
    Options2 = Options#{etag => [ETag],
                        max_age => MaxAge,
                        content_format => Format,
                        location_path => LocPath},
    Msg2#coap_message{options = Options2};

%% segmentation not requested, but required (late negotiation)
set_content(Content, undefined, Msg) ->
    set_content(Content, {0, true, ?MAX_BLOCK_SIZE}, Msg);

%% segmentation requested (early negotiation)
set_content(#coap_content{etag = ETag,
                          max_age = MaxAge,
                          format = Format,
                          payload = Payload},
            Block,
            Msg) ->
    #coap_message{options = Options} = Msg2 = set_payload_block(Payload, Block, Msg),
    Options2 = Options#{etag => [ETag],
                        max => MaxAge,
                        content_format => Format},
    Msg2#coap_message{options = Options2}.

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
            set(BlockId, {Num, true, Size},
                set_payload(binary:part(Content, OffsetBegin, Size), Msg));
        _ ->
            set(BlockId, {Num, false, Size},
                set_payload(binary:part(Content, OffsetBegin, ContentSize - OffsetBegin), Msg))
    end.
