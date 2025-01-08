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

%% The OCPP messsage validator based on JSON-schema
-module(emqx_ocpp_schemas).

-include("emqx_ocpp.hrl").

-export([
    load/0,
    validate/2
]).

-spec load() -> ok.
%% @doc The jesse:load_schemas/2 require the caller process to own an ets table.
%% So, please call it in some a long-live process
load() ->
    case emqx_ocpp_conf:message_format_checking() of
        disable ->
            ok;
        _ ->
            case feedvar(emqx_config:get([gateway, ocpp, json_schema_dir], undefined)) of
                undefined ->
                    ok;
                Dir ->
                    ok = jesse:load_schemas(Dir, fun emqx_utils_json:decode/1)
            end
    end.

-spec validate(upstream | dnstream, emqx_ocpp_frame:frame()) ->
    ok
    | {error, string()}.

%% FIXME: `action` key is absent in OCPP_MSG_TYPE_ID_CALLRESULT frame
validate(Direction, #{type := Type, action := Action, payload := Payload}) when
    Type == ?OCPP_MSG_TYPE_ID_CALL;
    Type == ?OCPP_MSG_TYPE_ID_CALLRESULT
->
    case emqx_ocpp_conf:message_format_checking() of
        all ->
            do_validate(schema_id(Type, Action), Payload);
        upstream_only when Direction == upstream ->
            do_validate(schema_id(Type, Action), Payload);
        dnstream_only when Direction == dnstream ->
            do_validate(schema_id(Type, Action), Payload);
        _ ->
            ok
    end;
validate(_, #{type := ?OCPP_MSG_TYPE_ID_CALLERROR}) ->
    ok.

do_validate(SchemaId, Payload) ->
    case jesse:validate(SchemaId, Payload) of
        {ok, _} ->
            ok;
        %% jesse_database:error/0
        {error, {database_error, Key, Reason}} ->
            {error, format("Validation error: ~s ~s", [Key, Reason])};
        %% jesse_error:error/0
        {error, [{data_invalid, _Schema, Error, _Data, Path} | _]} ->
            {error, format("Validation error: ~s ~s", [Path, Error])};
        {error, [{schema_invalid, _Schema, Error} | _]} ->
            {error, format("Validation error: schema_invalid ~s", [Error])};
        {error, Reason} ->
            {error, io_lib:format("Validation error: ~0p", [Reason])}
    end.

%%--------------------------------------------------------------------
%% internal funcs

%% @doc support vars:
%%  - ${application_priv}
feedvar(undefined) ->
    undefined;
feedvar(Path) ->
    binary_to_list(
        emqx_placeholder:proc_tmpl(
            emqx_placeholder:preproc_tmpl(Path),
            #{application_priv => code:priv_dir(emqx_gateway_ocpp)}
        )
    ).

schema_id(?OCPP_MSG_TYPE_ID_CALL, Action) when is_binary(Action) ->
    emqx_config:get([gateway, ocpp, json_schema_id_prefix]) ++
        binary_to_list(Action) ++
        "Request";
schema_id(?OCPP_MSG_TYPE_ID_CALLRESULT, Action) when is_binary(Action) ->
    emqx_config:get([gateway, ocpp, json_schema_id_prefix]) ++
        binary_to_list(Action) ++
        "Response".

format(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).
