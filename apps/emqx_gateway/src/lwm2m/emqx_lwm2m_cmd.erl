%%--------------------------------------------------------------------
%% Copyright (c) 2016-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_cmd).

-include_lib("emqx/include/logger.hrl").
-include("src/coap/include/emqx_coap.hrl").
-include("src/lwm2m/include/emqx_lwm2m.hrl").

-export([
    mqtt_to_coap/2,
    coap_to_mqtt/4,
    empty_ack_to_mqtt/1,
    coap_failure_to_mqtt/2
]).

-export([path_list/1, extract_path/1]).

-define(STANDARD, 1).

%%-type msg_type() :: <<"create">>
%%                  | <<"delete">>
%%                  | <<"read">>
%%                  | <<"write">>
%%                  | <<"execute">>
%%                  | <<"discover">>
%%                  | <<"write-attr">>
%%                  | <<"observe">>
%%                  | <<"cancel-observe">>.
%%
%%-type cmd() :: #{ <<"msgType">> := msg_type()
%%                , <<"data">> := maps()
%%                %%%% more keys?
%%                }.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"create">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"basePath">>, Data, <<"/">>)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, maps:get(<<"content">>, Data)),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    CoapRequest = emqx_coap_message:request(
        con,
        post,
        Payload,
        [
            {uri_path, FullPathList},
            {uri_query, QueryList},
            {content_format, <<"application/vnd.oma.lwm2m+tlv">>}
        ]
    ),
    {CoapRequest, InputCmd};
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"delete">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {
        emqx_coap_message:request(
            con,
            delete,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"read">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {
        emqx_coap_message:request(
            con,
            get,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"write">>, <<"data">> := Data}) ->
    CoapRequest =
        case maps:get(<<"basePath">>, Data, <<"/">>) of
            <<"/">> ->
                single_write_request(AlternatePath, Data);
            BasePath ->
                batch_write_request(AlternatePath, BasePath, maps:get(<<"content">>, Data))
        end,
    {CoapRequest, InputCmd};
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"execute">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    Args =
        case maps:get(<<"args">>, Data, <<>>) of
            <<"undefined">> -> <<>>;
            undefined -> <<>>;
            Arg1 -> Arg1
        end,
    {
        emqx_coap_message:request(
            con,
            post,
            Args,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList},
                {content_format, <<"text/plain">>}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"discover">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {
        emqx_coap_message:request(
            con,
            get,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList},
                {'accept', ?LWM2M_FORMAT_LINK}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"write-attr">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    Query = attr_query_list(Data),
    {
        emqx_coap_message:request(
            con,
            put,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList},
                {uri_query, Query}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"observe">>, <<"data">> := Data}) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {
        emqx_coap_message:request(
            con,
            get,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList},
                {observe, 0}
            ]
        ),
        InputCmd
    };
mqtt_to_coap(
    AlternatePath, InputCmd = #{<<"msgType">> := <<"cancel-observe">>, <<"data">> := Data}
) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {
        emqx_coap_message:request(
            con,
            get,
            <<>>,
            [
                {uri_path, FullPathList},
                {uri_query, QueryList},
                {observe, 1}
            ]
        ),
        InputCmd
    }.

coap_to_mqtt(_Method = {_, Code}, _CoapPayload, _Options, Ref = #{<<"msgType">> := <<"create">>}) ->
    make_response(Code, Ref);
coap_to_mqtt(_Method = {_, Code}, _CoapPayload, _Options, Ref = #{<<"msgType">> := <<"delete">>}) ->
    make_response(Code, Ref);
coap_to_mqtt(Method, CoapPayload, Options, Ref = #{<<"msgType">> := <<"read">>}) ->
    read_resp_to_mqtt(Method, CoapPayload, data_format(Options), Ref);
coap_to_mqtt(Method, CoapPayload, _Options, Ref = #{<<"msgType">> := <<"write">>}) ->
    write_resp_to_mqtt(Method, CoapPayload, Ref);
coap_to_mqtt(Method, _CoapPayload, _Options, Ref = #{<<"msgType">> := <<"execute">>}) ->
    execute_resp_to_mqtt(Method, Ref);
coap_to_mqtt(Method, CoapPayload, _Options, Ref = #{<<"msgType">> := <<"discover">>}) ->
    discover_resp_to_mqtt(Method, CoapPayload, Ref);
coap_to_mqtt(Method, CoapPayload, _Options, Ref = #{<<"msgType">> := <<"write-attr">>}) ->
    writeattr_resp_to_mqtt(Method, CoapPayload, Ref);
coap_to_mqtt(Method, CoapPayload, Options, Ref = #{<<"msgType">> := <<"observe">>}) ->
    observe_resp_to_mqtt(Method, CoapPayload, data_format(Options), observe_seq(Options), Ref);
coap_to_mqtt(Method, CoapPayload, Options, Ref = #{<<"msgType">> := <<"cancel-observe">>}) ->
    cancel_observe_resp_to_mqtt(Method, CoapPayload, data_format(Options), Ref).

read_resp_to_mqtt({error, ErrorCode}, _CoapPayload, _Format, Ref) ->
    make_response(ErrorCode, Ref);
read_resp_to_mqtt({ok, SuccessCode}, CoapPayload, Format, Ref) ->
    try
        Result = content_to_mqtt(CoapPayload, Format, Ref),
        make_response(SuccessCode, Ref, Format, Result)
    catch
        error:not_implemented ->
            make_response(not_implemented, Ref);
        _:Ex:_ST ->
            ?SLOG(error, #{
                msg => "bad_payload_format",
                payload => CoapPayload,
                reason => Ex,
                stacktrace => _ST
            }),
            make_response(bad_request, Ref)
    end.

empty_ack_to_mqtt(Ref) ->
    make_base_response(maps:put(<<"msgType">>, <<"ack">>, Ref)).

coap_failure_to_mqtt(Ref, MsgType) ->
    make_base_response(maps:put(<<"msgType">>, MsgType, Ref)).

content_to_mqtt(CoapPayload, <<"text/plain">>, Ref) ->
    emqx_lwm2m_message:text_to_json(extract_path(Ref), CoapPayload);
content_to_mqtt(CoapPayload, <<"application/octet-stream">>, Ref) ->
    emqx_lwm2m_message:opaque_to_json(extract_path(Ref), CoapPayload);
content_to_mqtt(CoapPayload, <<"application/vnd.oma.lwm2m+tlv">>, Ref) ->
    emqx_lwm2m_message:tlv_to_json(extract_path(Ref), CoapPayload);
content_to_mqtt(CoapPayload, <<"application/vnd.oma.lwm2m+json">>, _Ref) ->
    emqx_lwm2m_message:translate_json(CoapPayload).

write_resp_to_mqtt({ok, changed}, _CoapPayload, Ref) ->
    make_response(changed, Ref);
write_resp_to_mqtt({ok, content}, CoapPayload, Ref) when CoapPayload =:= <<>> ->
    make_response(method_not_allowed, Ref);
write_resp_to_mqtt({ok, content}, _CoapPayload, Ref) ->
    make_response(changed, Ref);
write_resp_to_mqtt({error, Error}, _CoapPayload, Ref) ->
    make_response(Error, Ref).

execute_resp_to_mqtt({ok, changed}, Ref) ->
    make_response(changed, Ref);
execute_resp_to_mqtt({error, Error}, Ref) ->
    make_response(Error, Ref).

discover_resp_to_mqtt({ok, content}, CoapPayload, Ref) ->
    Links = binary:split(CoapPayload, <<",">>, [global]),
    make_response(content, Ref, <<"application/link-format">>, Links);
discover_resp_to_mqtt({error, Error}, _CoapPayload, Ref) ->
    make_response(Error, Ref).

writeattr_resp_to_mqtt({ok, changed}, _CoapPayload, Ref) ->
    make_response(changed, Ref);
writeattr_resp_to_mqtt({error, Error}, _CoapPayload, Ref) ->
    make_response(Error, Ref).

observe_resp_to_mqtt({error, Error}, _CoapPayload, _Format, _ObserveSeqNum, Ref) ->
    make_response(Error, Ref);
observe_resp_to_mqtt({ok, content}, CoapPayload, Format, 0, Ref) ->
    read_resp_to_mqtt({ok, content}, CoapPayload, Format, Ref);
observe_resp_to_mqtt({ok, content}, CoapPayload, Format, ObserveSeqNum, Ref) ->
    read_resp_to_mqtt({ok, content}, CoapPayload, Format, Ref#{<<"seqNum">> => ObserveSeqNum}).

cancel_observe_resp_to_mqtt({ok, content}, CoapPayload, Format, Ref) ->
    read_resp_to_mqtt({ok, content}, CoapPayload, Format, Ref);
cancel_observe_resp_to_mqtt({error, Error}, _CoapPayload, _Format, Ref) ->
    make_response(Error, Ref).

make_response(Code, Ref = #{}) ->
    BaseRsp = make_base_response(Ref),
    make_data_response(BaseRsp, Code).

make_response(Code, Ref = #{}, _Format, Result) ->
    BaseRsp = make_base_response(Ref),
    make_data_response(BaseRsp, Code, _Format, Result).

%% The base response format is what included in the request:
%%
%%   #{
%%       <<"seqNum">> => SeqNum,
%%       <<"imsi">> => maps:get(<<"imsi">>, Ref, null),
%%       <<"imei">> => maps:get(<<"imei">>, Ref, null),
%%       <<"requestID">> => maps:get(<<"requestID">>, Ref, null),
%%       <<"cacheID">> => maps:get(<<"cacheID">>, Ref, null),
%%       <<"msgType">> => maps:get(<<"msgType">>, Ref, null)
%%   }

make_base_response(Ref = #{}) ->
    remove_tmp_fields(Ref).

make_data_response(BaseRsp, Code) ->
    BaseRsp#{
        <<"data">> => #{
            <<"reqPath">> => extract_path(BaseRsp),
            <<"code">> => code(Code),
            <<"codeMsg">> => Code
        }
    }.

make_data_response(BaseRsp, Code, _Format, Result) ->
    BaseRsp#{
        <<"data">> =>
            #{
                <<"reqPath">> => extract_path(BaseRsp),
                <<"code">> => code(Code),
                <<"codeMsg">> => Code,
                <<"content">> => Result
            }
    }.

remove_tmp_fields(Ref) ->
    maps:remove(observe_type, Ref).

-spec path_list(Path :: binary()) -> {[PathWord :: binary()], [Query :: binary()]}.
path_list(Path) ->
    case binary:split(binary_util:trim(Path, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId, LastPart] ->
            {ResInstId, QueryList} = query_list(LastPart),
            {[ObjId, ObjInsId, ResId, ResInstId], QueryList};
        [ObjId, ObjInsId, LastPart] ->
            {ResId, QueryList} = query_list(LastPart),
            {[ObjId, ObjInsId, ResId], QueryList};
        [ObjId, LastPart] ->
            {ObjInsId, QueryList} = query_list(LastPart),
            {[ObjId, ObjInsId], QueryList};
        [LastPart] ->
            {ObjId, QueryList} = query_list(LastPart),
            {[ObjId], QueryList}
    end.

query_list(PathWithQuery) ->
    case binary:split(PathWithQuery, [<<$?>>], []) of
        [Path] -> {Path, []};
        [Path, Querys] -> {Path, binary:split(Querys, [<<$&>>], [global])}
    end.

attr_query_list(Data) ->
    attr_query_list(Data, valid_attr_keys(), []).

attr_query_list(QueryJson = #{}, ValidAttrKeys, QueryList) ->
    maps:fold(
        fun
            (_K, null, Acc) ->
                Acc;
            (K, V, Acc) ->
                case lists:member(K, ValidAttrKeys) of
                    true ->
                        KV = <<K/binary, "=", V/binary>>,
                        Acc ++ [KV];
                    false ->
                        Acc
                end
        end,
        QueryList,
        QueryJson
    ).

valid_attr_keys() ->
    [<<"pmin">>, <<"pmax">>, <<"gt">>, <<"lt">>, <<"st">>].

data_format(Options) ->
    maps:get(content_format, Options, <<"text/plain">>).

observe_seq(Options) ->
    maps:get(observe, Options, rand:uniform(1000000) + 1).

add_alternate_path_prefix(<<"/">>, PathList) ->
    PathList;
add_alternate_path_prefix(AlternatePath, PathList) ->
    [binary_util:trim(AlternatePath, $/) | PathList].

extract_path(Ref = #{}) ->
    drop_query(
        case Ref of
            #{<<"data">> := Data} ->
                case maps:get(<<"path">>, Data, undefined) of
                    undefined -> maps:get(<<"basePath">>, Data, undefined);
                    Path -> Path
                end;
            #{<<"path">> := Path} ->
                Path
        end
    ).

batch_write_request(AlternatePath, BasePath, Content) ->
    {PathList, QueryList} = path_list(BasePath),
    Method =
        case length(PathList) of
            2 -> post;
            3 -> put
        end,
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, Content),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    emqx_coap_message:request(
        con,
        Method,
        Payload,
        [
            {uri_path, FullPathList},
            {uri_query, QueryList},
            {content_format, <<"application/vnd.oma.lwm2m+tlv">>}
        ]
    ).

single_write_request(AlternatePath, Data) ->
    {PathList, QueryList} = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    %% TO DO: handle write to resource instance, e.g. /4/0/1/0
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, [Data]),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    emqx_coap_message:request(
        con,
        put,
        Payload,
        [
            {uri_path, FullPathList},
            {uri_query, QueryList},
            {content_format, <<"application/vnd.oma.lwm2m+tlv">>}
        ]
    ).

drop_query(Path) ->
    case binary:split(Path, [<<$?>>]) of
        [Path] -> Path;
        [PathOnly, _Query] -> PathOnly
    end.

code(get) -> <<"0.01">>;
code(post) -> <<"0.02">>;
code(put) -> <<"0.03">>;
code(delete) -> <<"0.04">>;
code(created) -> <<"2.01">>;
code(deleted) -> <<"2.02">>;
code(valid) -> <<"2.03">>;
code(changed) -> <<"2.04">>;
code(content) -> <<"2.05">>;
code(continue) -> <<"2.31">>;
code(bad_request) -> <<"4.00">>;
code(unauthorized) -> <<"4.01">>;
code(bad_option) -> <<"4.02">>;
code(forbidden) -> <<"4.03">>;
code(not_found) -> <<"4.04">>;
code(method_not_allowed) -> <<"4.05">>;
code(not_acceptable) -> <<"4.06">>;
code(request_entity_incomplete) -> <<"4.08">>;
code(precondition_failed) -> <<"4.12">>;
code(request_entity_too_large) -> <<"4.13">>;
code(unsupported_content_format) -> <<"4.15">>;
code(internal_server_error) -> <<"5.00">>;
code(not_implemented) -> <<"5.01">>;
code(bad_gateway) -> <<"5.02">>;
code(service_unavailable) -> <<"5.03">>;
code(gateway_timeout) -> <<"5.04">>;
code(proxying_not_supported) -> <<"5.05">>.
