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

-module(emqx_lwm2m_coap_resource).

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("lwm2m_coap/include/coap.hrl").

-behaviour(lwm2m_coap_resource).

-export([ coap_discover/2
        , coap_get/5
        , coap_post/5
        , coap_put/5
        , coap_delete/4
        , coap_observe/5
        , coap_unobserve/1
        , coap_response/7
        , coap_ack/3
        , handle_info/2
        , handle_call/3
        , handle_cast/2
        , terminate/2
        ]).

-export([parse_object_list/1]).

-include("emqx_lwm2m.hrl").

-define(PREFIX, <<"rd">>).

-define(LOG(Level, Format, Args), logger:Level("LWM2M-RESOURCE: " ++ Format, Args)).

-dialyzer([{nowarn_function, [coap_discover/2]}]).
% we use {'absolute', list(binary()), [{atom(), binary()}]} as coap_uri()
% https://github.com/emqx/lwm2m-coap/blob/258e9bd3762124395e83c1e68a1583b84718230f/src/lwm2m_coap_resource.erl#L61
% resource operations
coap_discover(_Prefix, _Args) ->
    [{absolute, [<<"mqtt">>], []}].

coap_get(ChId, [?PREFIX], Query, Content, Lwm2mState) ->
    ?LOG(debug, "~p ~p GET Query=~p, Content=~p", [self(),ChId, Query, Content]),
    {ok, #coap_content{}, Lwm2mState};
coap_get(ChId, Prefix, Query, Content, Lwm2mState) ->
    ?LOG(error, "ignore bad put request ChId=~p, Prefix=~p, Query=~p, Content=~p", [ChId, Prefix,  Query, Content]),
    {error, bad_request, Lwm2mState}.

% LWM2M REGISTER COMMAND
coap_post(ChId, [?PREFIX], Query, Content = #coap_content{uri_path = [?PREFIX]}, Lwm2mState) ->
    ?LOG(debug, "~p ~p REGISTER command Query=~p, Content=~p", [self(), ChId, Query, Content]),
    case parse_options(Query) of
        {error, {bad_opt, _CustomOption}} ->
            ?LOG(error, "Reject REGISTER from ~p due to wrong option", [ChId]),
            {error, bad_request, Lwm2mState};
        {ok, LwM2MQuery} ->
            process_register(ChId, LwM2MQuery, Content#coap_content.payload, Lwm2mState)
    end;

% LWM2M UPDATE COMMAND
coap_post(ChId, [?PREFIX], Query, Content = #coap_content{uri_path = LocationPath}, Lwm2mState) ->
    ?LOG(debug, "~p ~p UPDATE command location=~p, Query=~p, Content=~p", [self(), ChId, LocationPath, Query, Content]),
    case parse_options(Query) of
        {error, {bad_opt, _CustomOption}} ->
            ?LOG(error, "Reject UPDATE from ~p due to wrong option, Query=~p", [ChId, Query]),
            {error, bad_request, Lwm2mState};
        {ok, LwM2MQuery} ->
            process_update(ChId, LwM2MQuery, LocationPath, Content#coap_content.payload, Lwm2mState)
    end;

coap_post(ChId, Prefix, Query, Content, Lwm2mState) ->
    ?LOG(error, "bad post request ChId=~p, Prefix=~p, Query=~p, Content=~p", [ChId, Prefix, Query, Content]),
    {error, bad_request, Lwm2mState}.

coap_put(_ChId, Prefix, Query, Content, Lwm2mState) ->
    ?LOG(error, "put has error, Prefix=~p, Query=~p, Content=~p", [Prefix, Query, Content]),
    {error, bad_request, Lwm2mState}.

% LWM2M DE-REGISTER COMMAND
coap_delete(ChId, [?PREFIX], #coap_content{uri_path = Location}, Lwm2mState) ->
    LocationPath = binary_util:join_path(Location),
    ?LOG(debug, "~p ~p DELETE command location=~p", [self(), ChId, LocationPath]),
    case get(lwm2m_context) of
        #lwm2m_context{location = LocationPath} ->
            lwm2m_coap_responder:stop(deregister),
            {ok, Lwm2mState};
        undefined ->
            ?LOG(error, "Reject DELETE from ~p, Location: ~p not found", [ChId, Location]),
            {error, forbidden, Lwm2mState};
        TrueLocation ->
            ?LOG(error, "Reject DELETE from ~p, Wrong Location: ~p, registered location record: ~p", [ChId, Location, TrueLocation]),
            {error, not_found, Lwm2mState}
    end;
coap_delete(_ChId, _Prefix, _Content, Lwm2mState) ->
    {error, forbidden, Lwm2mState}.

coap_observe(ChId, Prefix, Name, Ack, Lwm2mState) ->
    ?LOG(error, "unsupported observe request ChId=~p, Prefix=~p, Name=~p, Ack=~p", [ChId, Prefix, Name, Ack]),
    {error, method_not_allowed, Lwm2mState}.

coap_unobserve(Lwm2mState) ->
    ?LOG(error, "unsupported unobserve request: ~p", [Lwm2mState]),
    {ok, Lwm2mState}.

coap_response(ChId, Ref, CoapMsgType, CoapMsgMethod, CoapMsgPayload, CoapMsgOpts, Lwm2mState) ->
    ?LOG(info, "~p, RCV CoAP response, CoapMsgType: ~p, CoapMsgMethod: ~p, CoapMsgPayload: ~p,
                    CoapMsgOpts: ~p, Ref: ~p",
        [ChId, CoapMsgType, CoapMsgMethod, CoapMsgPayload, CoapMsgOpts, Ref]),
    MqttPayload = emqx_lwm2m_cmd_handler:coap2mqtt(CoapMsgMethod, CoapMsgPayload, CoapMsgOpts, Ref),
    Lwm2mState2 = emqx_lwm2m_protocol:send_ul_data(maps:get(<<"msgType">>, MqttPayload), MqttPayload, Lwm2mState),
    {noreply, Lwm2mState2}.

coap_ack(_ChId, Ref, Lwm2mState) ->
    ?LOG(info, "~p, RCV CoAP Empty ACK, Ref: ~p", [_ChId, Ref]),
    AckRef = maps:put(<<"msgType">>, <<"ack">>, Ref),
    MqttPayload = emqx_lwm2m_cmd_handler:ack2mqtt(AckRef),
    Lwm2mState2 = emqx_lwm2m_protocol:send_ul_data(maps:get(<<"msgType">>, MqttPayload), MqttPayload, Lwm2mState),
    {ok, Lwm2mState2}.

%% Batch deliver
handle_info({deliver, Topic, Msgs}, Lwm2mState) when is_list(Msgs) ->
    {noreply, lists:foldl(fun(Msg, NewState) ->
                                  element(2, handle_info({deliver, Topic, Msg}, NewState))
                          end, Lwm2mState, Msgs)};
%% Handle MQTT Message
handle_info({deliver, _Topic, MqttMsg}, Lwm2mState) ->
    Lwm2mState2 = emqx_lwm2m_protocol:deliver(MqttMsg, Lwm2mState),
    {noreply, Lwm2mState2};

%% Deliver Coap Message to Device
handle_info({deliver_to_coap, CoapRequest, Ref}, Lwm2mState) ->
    {send_request, CoapRequest, Ref, Lwm2mState};

handle_info({'EXIT', _Pid, Reason}, Lwm2mState) ->
    ?LOG(info, "~p, received exit from: ~p, reason: ~p, quit now!", [self(), _Pid, Reason]),
    {stop, Reason, Lwm2mState};

handle_info(post_init, Lwm2mState) ->
    Lwm2mState2 = emqx_lwm2m_protocol:post_init(Lwm2mState),
    {noreply, Lwm2mState2};

handle_info(auto_observe, Lwm2mState) ->
    Lwm2mState2 = emqx_lwm2m_protocol:auto_observe(Lwm2mState),
    {noreply, Lwm2mState2};

handle_info({life_timer, expired}, Lwm2mState) ->
    ?LOG(debug, "lifetime expired, shutdown", []),
    {stop, life_timer_expired, Lwm2mState};

handle_info({shutdown, Error}, Lwm2mState) ->
    {stop, Error, Lwm2mState};

handle_info({shutdown, conflict, {ClientId, NewPid}}, Lwm2mState) ->
    ?LOG(warning, "lwm2m '~s' conflict with ~p, shutdown", [ClientId, NewPid]),
    {stop, conflict, Lwm2mState};

handle_info({suback, _MsgId, [_GrantedQos]}, Lwm2mState) ->
    {noreply, Lwm2mState};

handle_info(emit_stats, Lwm2mState) ->
    {noreply, Lwm2mState};

handle_info(Message, Lwm2mState) ->
    ?LOG(error, "Unknown Message ~p", [Message]),
    {noreply, Lwm2mState}.


handle_call(info, _From, Lwm2mState) ->
    {Info, Lwm2mState2} = emqx_lwm2m_protocol:get_info(Lwm2mState),
    {reply, Info, Lwm2mState2};

handle_call(stats, _From, Lwm2mState) ->
    {Stats, Lwm2mState2} = emqx_lwm2m_protocol:get_stats(Lwm2mState),
    {reply, Stats, Lwm2mState2};

handle_call(kick, _From, Lwm2mState) ->
    {stop, kick, Lwm2mState};

handle_call({set_rate_limit, _Rl}, _From, Lwm2mState) ->
    ?LOG(error, "set_rate_limit is not support", []),
    {reply, ok, Lwm2mState};

handle_call(get_rate_limit, _From, Lwm2mState) ->
    ?LOG(error, "get_rate_limit is not support", []),
    {reply, ok, Lwm2mState};

handle_call(session, _From, Lwm2mState) ->
    ?LOG(error, "get_session is not support", []),
    {reply, ok, Lwm2mState};

handle_call(Request, _From, Lwm2mState) ->
    ?LOG(error, "adapter unexpected call ~p", [Request]),
    {reply, ok, Lwm2mState}.

handle_cast(Msg, Lwm2mState) ->
    ?LOG(error, "unexpected cast ~p", [Msg]),
    {noreply, Lwm2mState, hibernate}.

terminate(Reason, Lwm2mState) ->
    emqx_lwm2m_protocol:terminate(Reason, Lwm2mState).

%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%
process_register(ChId, LwM2MQuery, LwM2MPayload, Lwm2mState) ->
    Epn = maps:get(<<"ep">>, LwM2MQuery, undefined),
    LifeTime = maps:get(<<"lt">>, LwM2MQuery, undefined),
    Ver = maps:get(<<"lwm2m">>, LwM2MQuery, undefined),
    case check_lwm2m_version(Ver) of
        false ->
            ?LOG(error, "Reject REGISTER from ~p due to unsupported version: ~p", [ChId, Ver]),
            lwm2m_coap_responder:stop(invalid_version),
            {error, precondition_failed, Lwm2mState};
        true ->
            case check_epn(Epn) andalso check_lifetime(LifeTime) of
                true ->
                    init_lwm2m_emq_client(ChId, LwM2MQuery, LwM2MPayload, Lwm2mState);
                false ->
                    ?LOG(error, "Reject REGISTER from ~p due to wrong parameters, epn=~p, lifetime=~p", [ChId, Epn, LifeTime]),
                    lwm2m_coap_responder:stop(invalid_query_params),
                    {error, bad_request, Lwm2mState}
            end
    end.

process_update(ChId, LwM2MQuery, Location, LwM2MPayload, Lwm2mState) ->
    LocationPath = binary_util:join_path(Location),
    case get(lwm2m_context) of
        #lwm2m_context{location = LocationPath} ->
            RegInfo = append_object_list(LwM2MQuery, LwM2MPayload),
            Lwm2mState2 = emqx_lwm2m_protocol:update_reg_info(RegInfo, Lwm2mState),
            ?LOG(info, "~p, UPDATE Success, assgined location: ~p", [ChId, LocationPath]),
            {ok, changed, #coap_content{}, Lwm2mState2};
        undefined ->
            ?LOG(error, "Reject UPDATE from ~p, Location: ~p not found", [ChId, Location]),
            {error, forbidden, Lwm2mState};
        TrueLocation ->
            ?LOG(error, "Reject UPDATE from ~p, Wrong Location: ~p, registered location record: ~p", [ChId, Location, TrueLocation]),
            {error, not_found, Lwm2mState}
    end.

init_lwm2m_emq_client(ChId, LwM2MQuery = #{<<"ep">> := Epn}, LwM2MPayload, _Lwm2mState = undefined) ->
    RegInfo = append_object_list(LwM2MQuery, LwM2MPayload),
    case emqx_lwm2m_protocol:init(self(), Epn, ChId, RegInfo) of
        {ok, Lwm2mState} ->
            LocationPath = assign_location_path(Epn),
            ?LOG(info, "~p, REGISTER Success, assgined location: ~p", [ChId, LocationPath]),
            {ok, created, #coap_content{location_path = LocationPath}, Lwm2mState};
        {error, Error} ->
            lwm2m_coap_responder:stop(Error),
            ?LOG(error, "~p, REGISTER Failed, error: ~p", [ChId, Error]),
            {error, forbidden, undefined}
    end;
init_lwm2m_emq_client(ChId, LwM2MQuery = #{<<"ep">> := Epn}, LwM2MPayload, Lwm2mState) ->
    RegInfo = append_object_list(LwM2MQuery, LwM2MPayload),
    LocationPath = assign_location_path(Epn),
    ?LOG(info, "~p, RE-REGISTER Success, location: ~p", [ChId, LocationPath]),
    Lwm2mState2 = emqx_lwm2m_protocol:replace_reg_info(RegInfo, Lwm2mState),
    {ok, created, #coap_content{location_path = LocationPath}, Lwm2mState2}.

append_object_list(LwM2MQuery, <<>>) when map_size(LwM2MQuery) == 0 -> #{};
append_object_list(LwM2MQuery, <<>>) -> LwM2MQuery;
append_object_list(LwM2MQuery, LwM2MPayload) when is_binary(LwM2MPayload) ->
    {AlterPath, ObjList} = parse_object_list(LwM2MPayload),
    LwM2MQuery#{
        <<"alternatePath">> => AlterPath,
        <<"objectList">> => ObjList
    }.

parse_options(InputQuery) ->
    parse_options(InputQuery, maps:new()).

parse_options([], Query) -> {ok, Query};
parse_options([<<"ep=", Epn/binary>>|T], Query) ->
    parse_options(T, maps:put(<<"ep">>, Epn, Query));
parse_options([<<"lt=", Lt/binary>>|T], Query) ->
    parse_options(T, maps:put(<<"lt">>, binary_to_integer(Lt), Query));
parse_options([<<"lwm2m=", Ver/binary>>|T], Query) ->
    parse_options(T, maps:put(<<"lwm2m">>, Ver, Query));
parse_options([<<"b=", Binding/binary>>|T], Query) ->
    parse_options(T, maps:put(<<"b">>, Binding, Query));
parse_options([CustomOption|T], Query) ->
    case binary:split(CustomOption, <<"=">>) of
        [OptKey, OptValue] when OptKey =/= <<>> ->
            ?LOG(debug, "non-standard option: ~p", [CustomOption]),
            parse_options(T, maps:put(OptKey, OptValue, Query));
        _BadOpt ->
            ?LOG(error, "bad option: ~p", [CustomOption]),
            {error, {bad_opt, CustomOption}}
    end.

parse_object_list(<<>>) -> {<<"/">>, <<>>};
parse_object_list(ObjLinks) when is_binary(ObjLinks) ->
    parse_object_list(binary:split(ObjLinks, <<",">>, [global]));

parse_object_list(FullObjLinkList) when is_list(FullObjLinkList) ->
    case drop_attr(FullObjLinkList) of
        {<<"/">>, _} = RootPrefixedLinks ->
            RootPrefixedLinks;
        {AlterPath, ObjLinkList} ->
            LenAlterPath = byte_size(AlterPath),
            WithOutPrefix =
                lists:map(
                    fun
                        (<<Prefix:LenAlterPath/binary, Link/binary>>) when Prefix =:= AlterPath ->
                            trim(Link);
                        (Link) -> Link
                    end, ObjLinkList),
            {AlterPath, WithOutPrefix}
    end.

drop_attr(LinkList) ->
    lists:foldr(
        fun(Link, {AlternatePath, LinkAcc}) ->
            {MainLink, LinkAttrs} = parse_link(Link),
            case is_alternate_path(LinkAttrs) of
                false -> {AlternatePath, [MainLink | LinkAcc]};
                true  -> {MainLink, LinkAcc}
            end
        end, {<<"/">>, []}, LinkList).

is_alternate_path(#{<<"rt">> := ?OMA_ALTER_PATH_RT}) -> true;
is_alternate_path(_) -> false.

parse_link(Link) ->
    [MainLink | Attrs] = binary:split(trim(Link), <<";">>, [global]),
    {delink(trim(MainLink)), parse_link_attrs(Attrs)}.

parse_link_attrs(LinkAttrs) when is_list(LinkAttrs) ->
    lists:foldl(
        fun(Attr, Acc) ->
            case binary:split(trim(Attr), <<"=">>) of
                [AttrKey, AttrValue] when AttrKey =/= <<>> ->
                    maps:put(AttrKey, AttrValue, Acc);
                _BadAttr -> throw({bad_attr, _BadAttr})
            end
        end, maps:new(), LinkAttrs).

trim(Str)-> binary_util:trim(Str, $ ).
delink(Str) ->
    Ltrim = binary_util:ltrim(Str, $<),
    binary_util:rtrim(Ltrim, $>).

check_lwm2m_version(<<"1">>)   -> true;
check_lwm2m_version(<<"1.", _PatchVerNum/binary>>) -> true;
check_lwm2m_version(_)         -> false.

check_epn(undefined) -> false;
check_epn(_)         -> true.

check_lifetime(undefined) -> false;
check_lifetime(LifeTime) when is_integer(LifeTime) ->
    Max = proplists:get_value(lifetime_max, lwm2m_coap_responder:options(), 315360000),
    Min = proplists:get_value(lifetime_min, lwm2m_coap_responder:options(), 0),
    if
        LifeTime >= Min, LifeTime =< Max ->
            true;
        true ->
            false
    end;
check_lifetime(_) -> false.


assign_location_path(Epn) ->
    %Location = list_to_binary(io_lib:format("~.16B", [rand:uniform(65535)])),
    %LocationPath = <<"/rd/", Location/binary>>,
    Location = [<<"rd">>, Epn],
    put(lwm2m_context, #lwm2m_context{epn = Epn, location = binary_util:join_path(Location)}),
    Location.
