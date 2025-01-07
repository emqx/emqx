%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ocpp_frame).

-behaviour(emqx_gateway_frame).

-include("emqx_ocpp.hrl").

%% emqx_gateway_frame callbacks
-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

-type parse_state() :: map().

-type parse_result() ::
    {ok, frame(), Rest :: binary(), NewState :: parse_state()}.

-export_type([
    parse_state/0,
    parse_result/0,
    frame/0,
    serialize_options/0
]).

-type serialize_options() :: emqx_gateway_frame:serialize_options().

-dialyzer({nowarn_function, [format/1]}).

-spec initial_parse_state(map()) -> parse_state().
initial_parse_state(_Opts) ->
    #{}.

%% No-TCP-Spliting

-spec parse(binary() | list(), parse_state()) -> parse_result().
parse(Bin, Parser) when is_binary(Bin) ->
    case emqx_utils_json:safe_decode(Bin, [return_maps]) of
        {ok, Json} ->
            parse(Json, Parser);
        {error, {Position, Reason}} ->
            error(
                {badjson, io_lib:format("Invalid json at ~w: ~s", [Position, Reason])}
            );
        {error, Reason} ->
            error(
                {badjson, io_lib:format("Invalid json: ~p", [Reason])}
            )
    end;
%% CALL
parse([?OCPP_MSG_TYPE_ID_CALL, Id, Action, Payload], Parser) ->
    Frame = #{
        type => ?OCPP_MSG_TYPE_ID_CALL,
        id => Id,
        action => Action,
        payload => Payload
    },
    case emqx_ocpp_schemas:validate(upstream, Frame) of
        ok ->
            {ok, Frame, <<>>, Parser};
        {error, ReasonStr} ->
            error({validation_failure, Id, ReasonStr})
    end;
%% CALLRESULT
parse([?OCPP_MSG_TYPE_ID_CALLRESULT, Id, Payload], Parser) ->
    Frame = #{
        type => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        id => Id,
        payload => Payload
    },
    %% TODO: Validate CALLRESULT frame
    %%case emqx_ocpp_schemas:validate(upstream, Frame) of
    %%    ok ->
    %%        {ok, Frame, <<>>, Parser};
    %%    {error, ReasonStr} ->
    %%        error({validation_failure, Id, ReasonStr})
    %%end;
    {ok, Frame, <<>>, Parser};
%% CALLERROR
parse(
    [
        ?OCPP_MSG_TYPE_ID_CALLERROR,
        Id,
        ErrCode,
        ErrDesc,
        ErrDetails
    ],
    Parser
) ->
    {ok,
        #{
            type => ?OCPP_MSG_TYPE_ID_CALLERROR,
            id => Id,
            error_code => ErrCode,
            error_desc => ErrDesc,
            error_details => ErrDetails
        },
        <<>>, Parser}.

-spec serialize_opts() -> serialize_options().
serialize_opts() ->
    #{}.

-spec serialize_pkt(frame(), emqx_gateway_frame:serialize_options()) -> iodata().
serialize_pkt(
    #{
        id := Id,
        type := ?OCPP_MSG_TYPE_ID_CALL,
        action := Action,
        payload := Payload
    },
    _Opts
) ->
    emqx_utils_json:encode([?OCPP_MSG_TYPE_ID_CALL, Id, Action, Payload]);
serialize_pkt(
    #{
        id := Id,
        type := ?OCPP_MSG_TYPE_ID_CALLRESULT,
        payload := Payload
    },
    _Opts
) ->
    emqx_utils_json:encode([?OCPP_MSG_TYPE_ID_CALLRESULT, Id, Payload]);
serialize_pkt(
    #{
        id := Id,
        type := Type,
        error_code := ErrCode,
        error_desc := ErrDesc
    } = Frame,
    _Opts
) when
    Type == ?OCPP_MSG_TYPE_ID_CALLERROR
->
    ErrDetails = maps:get(error_details, Frame, #{}),
    emqx_utils_json:encode([Type, Id, ErrCode, ErrDesc, ErrDetails]).

-spec format(frame()) -> string().
format(Frame) ->
    serialize_pkt(Frame, #{}).

-spec type(frame()) -> atom().
type(_Frame) ->
    %% TODO:
    todo.

-spec is_message(frame()) -> boolean().
is_message(_Frame) ->
    %% TODO:
    true.
