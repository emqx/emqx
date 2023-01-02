%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(prop_emqx_sn_frame).

-include_lib("emqx_sn/include/emqx_sn.hrl").
-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [register/1]}).

-import(emqx_sn_frame,
        [ parse/1
        , serialize/1
        ]).

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_parse_and_serialize() ->
    ?ALL(Msg, mqtt_sn_message(),
         begin
             {ok, Msg} = parse(serialize(Msg)),
             true
         end).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

do_setup() ->
    ok.

do_teardown(_) ->
    ok.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

mqtt_sn_message() ->
    M = emqx_sn_proper_types,
    oneof([ M:'ADVERTISE'(),       M:'SEARCHGW'()
          , M:'GWINFO'(),          M:'CONNECT'()
          , M:'CONNACK'(),         M:'WILLTOTPICREQ'()
          , M:'WILLTOPIC'(),       M:'WILLTOPCI_EMPTY'()
          , M:'WILLMESSAGEREQ'(),  M:'WILLMESSAGE'()
          , M:'REGISTER'(),        M:'REGACK'()
          , M:'PUBLISH'(),         M:'PUBACK'()
          , M:'PUBCOMP_REC_REL'(), M:'SUBSCRIBE'()
          , M:'SUBACK'(),          M:'UNSUBSCRIBE'()
          , M:'UNSUBACK'(),        M:'PINGREQ'()
          , M:'PINGRESP'(),        M:'DISCONNECT'()
          , M:'DISCONNECT'(),      M:'WILLTOPICUPD'()
          , M:'WILLTOPICRESP'(),   M:'WILLMSGUPD'()
          , M:'WILLMSGRESP'()
          ]).

