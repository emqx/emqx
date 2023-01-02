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

-module(emqx_lwm2m_cmd_handler).

-include("emqx_lwm2m.hrl").

-include_lib("lwm2m_coap/include/coap.hrl").

-export([ mqtt2coap/2
        , coap2mqtt/4
        , ack2mqtt/1
        , extract_path/1
        ]).

-export([path_list/1]).

-define(LOG(Level, Format, Args), logger:Level("LWM2M-CMD: " ++ Format, Args)).

mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"create">>, <<"data">> := Data}) ->
    PathList = path_list(maps:get(<<"basePath">>, Data, <<"/">>)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, maps:get(<<"content">>, Data)),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    CoapRequest = lwm2m_coap_message:request(con, post, Payload, [{uri_path, FullPathList},
                                                                  {content_format, <<"application/vnd.oma.lwm2m+tlv">>}]),
    {CoapRequest, InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"delete">>, <<"data">> := Data}) ->
    FullPathList = add_alternate_path_prefix(AlternatePath, path_list(maps:get(<<"path">>, Data))),
    {lwm2m_coap_message:request(con, delete, <<>>, [{uri_path, FullPathList}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"read">>, <<"data">> := Data}) ->
    FullPathList = add_alternate_path_prefix(AlternatePath, path_list(maps:get(<<"path">>, Data))),
    {lwm2m_coap_message:request(con, get, <<>>, [{uri_path, FullPathList}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"write">>, <<"data">> := Data}) ->
    Encoding = maps:get(<<"encoding">>, InputCmd, <<"plain">>),
    CoapRequest =
        case maps:get(<<"basePath">>, Data, <<"/">>) of
            <<"/">> ->
                single_write_request(AlternatePath, Data, Encoding);
            BasePath ->
                batch_write_request(AlternatePath, BasePath, maps:get(<<"content">>, Data), Encoding)
        end,
    {CoapRequest, InputCmd};

mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"execute">>, <<"data">> := Data}) ->
    FullPathList = add_alternate_path_prefix(AlternatePath, path_list(maps:get(<<"path">>, Data))),
    Args =
        case maps:get(<<"args">>, Data, <<>>) of
            <<"undefined">> -> <<>>;
            undefined -> <<>>;
            Arg1 -> Arg1
        end,
    {lwm2m_coap_message:request(con, post, Args, [{uri_path, FullPathList}, {content_format, <<"text/plain">>}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"discover">>, <<"data">> := Data}) ->
    FullPathList = add_alternate_path_prefix(AlternatePath, path_list(maps:get(<<"path">>, Data))),
    {lwm2m_coap_message:request(con, get, <<>>, [{uri_path, FullPathList}, {'accept', ?LWM2M_FORMAT_LINK}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"write-attr">>, <<"data">> := Data}) ->
    FullPathList = add_alternate_path_prefix(AlternatePath, path_list(maps:get(<<"path">>, Data))),
    Query = attr_query_list(Data),
    {lwm2m_coap_message:request(con, put, <<>>, [{uri_path, FullPathList}, {uri_query, Query}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"observe">>, <<"data">> := Data}) ->
    PathList = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {lwm2m_coap_message:request(con, get, <<>>, [{uri_path, FullPathList}, {observe, 0}]), InputCmd};
mqtt2coap(AlternatePath, InputCmd = #{<<"msgType">> := <<"cancel-observe">>, <<"data">> := Data}) ->
    PathList = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    {lwm2m_coap_message:request(con, get, <<>>, [{uri_path, FullPathList}, {observe, 1}]), InputCmd}.

coap2mqtt(_Method = {_, Code}, _CoapPayload, _Options, Ref=#{<<"msgType">> := <<"create">>}) ->
    make_response(Code, Ref);
coap2mqtt(_Method = {_, Code}, _CoapPayload, _Options, Ref=#{<<"msgType">> := <<"delete">>}) ->
    make_response(Code, Ref);
coap2mqtt(Method, CoapPayload, Options, Ref=#{<<"msgType">> := <<"read">>}) ->
    coap_read_to_mqtt(Method, CoapPayload, data_format(Options), Ref);
coap2mqtt(Method, _CoapPayload, _Options, Ref=#{<<"msgType">> := <<"write">>}) ->
    coap_write_to_mqtt(Method, Ref);
coap2mqtt(Method, _CoapPayload, _Options, Ref=#{<<"msgType">> := <<"execute">>}) ->
    coap_execute_to_mqtt(Method, Ref);
coap2mqtt(Method, CoapPayload, _Options, Ref=#{<<"msgType">> := <<"discover">>}) ->
    coap_discover_to_mqtt(Method, CoapPayload, Ref);
coap2mqtt(Method, CoapPayload, _Options, Ref=#{<<"msgType">> := <<"write-attr">>}) ->
    coap_writeattr_to_mqtt(Method, CoapPayload, Ref);
coap2mqtt(Method, CoapPayload, Options, Ref=#{<<"msgType">> := <<"observe">>}) ->
    coap_observe_to_mqtt(Method, CoapPayload, data_format(Options), observe_seq(Options), Ref);
coap2mqtt(Method, CoapPayload, Options, Ref=#{<<"msgType">> := <<"cancel-observe">>}) ->
    coap_cancel_observe_to_mqtt(Method, CoapPayload, data_format(Options), Ref).

coap_read_to_mqtt({error, ErrorCode}, _CoapPayload, _Format, Ref) ->
    make_response(ErrorCode, Ref);
coap_read_to_mqtt({ok, SuccessCode}, CoapPayload, Format, Ref) ->
    try
        Result = coap_content_to_mqtt_payload(CoapPayload, Format, Ref),
        make_response(SuccessCode, Ref, Format, Result)
    catch
        throw : {bad_request, Reason} ->
            ?LOG(error, "bad_request, reason=~p, payload=~p", [Reason, CoapPayload]),
            make_response(bad_request, Ref);
        C:R:Stack ->
            ?LOG(error, "bad_request, error=~p, stacktrace=~p~npayload=~p", [{C, R}, Stack, CoapPayload]),
            make_response(bad_request, Ref)
    end.

ack2mqtt(Ref) ->
    make_base_response(Ref).

coap_content_to_mqtt_payload(CoapPayload, <<"text/plain">>, Ref) ->
    emqx_lwm2m_message:text_to_json(extract_path(Ref), CoapPayload);
coap_content_to_mqtt_payload(CoapPayload, <<"application/octet-stream">>, Ref) ->
    emqx_lwm2m_message:opaque_to_json(extract_path(Ref), CoapPayload);
coap_content_to_mqtt_payload(CoapPayload, <<"application/vnd.oma.lwm2m+tlv">>, Ref) ->
    emqx_lwm2m_message:tlv_to_json(extract_path(Ref), CoapPayload);
coap_content_to_mqtt_payload(CoapPayload, <<"application/vnd.oma.lwm2m+json">>, _Ref) ->
    emqx_lwm2m_message:translate_json(CoapPayload).

coap_write_to_mqtt({ok, changed}, Ref) ->
    make_response(changed, Ref);
coap_write_to_mqtt({error, Error}, Ref) ->
    make_response(Error, Ref).

coap_execute_to_mqtt({ok, changed}, Ref) ->
    make_response(changed, Ref);
coap_execute_to_mqtt({error, Error}, Ref) ->
    make_response(Error, Ref).

coap_discover_to_mqtt({ok, content}, CoapPayload, Ref) ->
    Links = binary:split(CoapPayload, <<",">>),
    make_response(content, Ref, <<"application/link-format">>, Links);
coap_discover_to_mqtt({error, Error}, _CoapPayload, Ref) ->
    make_response(Error, Ref).

coap_writeattr_to_mqtt({ok, changed}, _CoapPayload, Ref) ->
    make_response(changed, Ref);
coap_writeattr_to_mqtt({error, Error}, _CoapPayload, Ref) ->
    make_response(Error, Ref).

coap_observe_to_mqtt({error, Error}, _CoapPayload, _Format, _ObserveSeqNum, Ref) ->
    make_response(Error, Ref);
coap_observe_to_mqtt({ok, content}, CoapPayload, Format, 0, Ref) ->
    coap_read_to_mqtt({ok, content}, CoapPayload, Format, Ref);
coap_observe_to_mqtt({ok, content}, CoapPayload, Format, ObserveSeqNum, Ref) ->
    RefWithObserve = maps:put(<<"seqNum">>, ObserveSeqNum, Ref),
    RefNotify = maps:put(<<"msgType">>, <<"notify">>, RefWithObserve),
    coap_read_to_mqtt({ok, content}, CoapPayload, Format, RefNotify).

coap_cancel_observe_to_mqtt({ok, content}, CoapPayload, Format, Ref) ->
    coap_read_to_mqtt({ok, content}, CoapPayload, Format, Ref);
coap_cancel_observe_to_mqtt({error, Error}, _CoapPayload, _Format, Ref) ->
    make_response(Error, Ref).

make_response(Code, Ref=#{}) ->
    BaseRsp = make_base_response(Ref),
    make_data_response(BaseRsp, Code).
make_response(Code, Ref=#{}, _Format, Result) ->
    BaseRsp = make_base_response(Ref),
    make_data_response(BaseRsp, Code, _Format, Result).

%% The base response format is what included in the request:
%%
%%   #{
%%       <<"seqNum">> => SeqNum,
%%       <<"requestID">> => maps:get(<<"requestID">>, Ref, null),
%%       <<"cacheID">> => maps:get(<<"cacheID">>, Ref, null),
%%       <<"msgType">> => maps:get(<<"msgType">>, Ref, null)
%%   }

make_base_response(Ref=#{}) ->
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
        <<"data">> =>  #{
            <<"reqPath">> => extract_path(BaseRsp),
            <<"code">> => code(Code),
            <<"codeMsg">> => Code,
            <<"content">> => Result
        }
    }.

remove_tmp_fields(Ref) ->
    maps:remove(observe_type, Ref).

path_list(Path) ->
    case binary:split(binary_util:trim(Path, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId, ResInstId] -> [ObjId, ObjInsId, ResId, ResInstId];
        [ObjId, ObjInsId, ResId] -> [ObjId, ObjInsId, ResId];
        [ObjId, ObjInsId] -> [ObjId, ObjInsId];
        [ObjId] -> [ObjId]
    end.

attr_query_list(Data) ->
    attr_query_list(Data, valid_attr_keys(), []).
attr_query_list(QueryJson = #{}, ValidAttrKeys, QueryList) ->
    maps:fold(
        fun
            (_K, null, Acc) -> Acc;
            (K, V, Acc) ->
                case lists:member(K, ValidAttrKeys) of
                    true ->
                        Val = bin(V),
                        KV = <<K/binary, "=", Val/binary>>,
                        Acc ++ [KV];
                    false ->
                        Acc
                end
        end, QueryList, QueryJson).

valid_attr_keys() ->
    [<<"pmin">>, <<"pmax">>, <<"gt">>, <<"lt">>, <<"st">>].

data_format(Options) ->
    proplists:get_value(content_format, Options, <<"text/plain">>).
observe_seq(Options) ->
    proplists:get_value(observe, Options, rand:uniform(1000000) + 1 ).

add_alternate_path_prefix(<<"/">>, PathList) ->
    PathList;
add_alternate_path_prefix(AlternatePath, PathList) ->
    [binary_util:trim(AlternatePath, $/) | PathList].

extract_path(Ref = #{}) ->
    case Ref of
        #{<<"data">> := Data} ->
            case maps:get(<<"path">>, Data, nil) of
                nil -> maps:get(<<"basePath">>, Data, undefined);
                Path -> Path
            end;
        #{<<"path">> := Path} ->
            Path
    end.

batch_write_request(AlternatePath, BasePath, Content, Encoding) ->
    PathList = path_list(BasePath),
    Method = case length(PathList) of
                2 -> post;
                3 -> put
             end,
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    Content1 = decoding(Content, Encoding),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, Content1),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    lwm2m_coap_message:request(con, Method, Payload, [{uri_path, FullPathList}, {content_format, <<"application/vnd.oma.lwm2m+tlv">>}]).

single_write_request(AlternatePath, Data, Encoding) ->
    PathList = path_list(maps:get(<<"path">>, Data)),
    FullPathList = add_alternate_path_prefix(AlternatePath, PathList),
    Datas = decoding([Data], Encoding),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, Datas),
    Payload = emqx_lwm2m_tlv:encode(TlvData),
    lwm2m_coap_message:request(con, put, Payload, [{uri_path, FullPathList}, {content_format, <<"application/vnd.oma.lwm2m+tlv">>}]).


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
code(uauthorized) -> <<"4.01">>;
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

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Int) when is_integer(Int) -> integer_to_binary(Int);
bin(Float) when is_float(Float) -> float_to_binary(Float).

decoding(Datas, <<"hex">>) ->
    lists:map(fun(Data = #{<<"value">> := Value}) ->
        Data#{<<"value">> => emqx_misc:hexstr2bin(Value)}
    end, Datas);
decoding(Datas, _) ->
    Datas.
