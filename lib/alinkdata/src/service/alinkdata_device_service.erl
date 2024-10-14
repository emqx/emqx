%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 3月 2023 下午9:42
%%%-------------------------------------------------------------------
-module(alinkdata_device_service).
-author("yqfclid").

%% API
-export([
    device_status/4,
    device_children/4,
    product_type_device/4,
    kick_device/4,
    scene_device/4,
    import/4,
    import_template/4,
    write_to_device/4,
    send_cmd/4
]).

%%%===================================================================
%%% API
%%%===================================================================
device_status(_OperationID, #{<<"addr">> := RAddr},  _Context, Req) when RAddr =/= undefined ->
    Addrs0 = binary:split(RAddr, <<",">>, [global]),
    Addrs = lists:filter(fun(X) -> X =/= <<>> end, lists:usort(Addrs0)),
    Statuses = lists:map(fun get_status_format/1, Addrs),
    Res = alinkdata_ajax_result:success_result(Statuses),
    alinkdata_common_service:response(Res, Req);
device_status(_OperationID, _,  _Context, Req) ->
    Res = alinkdata_ajax_result:success_result([]),
    alinkdata_common_service:response(Res, Req).



device_children(OperationID, Args,  Context, Req) ->
    NArgs = change_order(Args),
    alinkdata_common_service:handle_with_dao(select_device_children, OperationID, NArgs,  Context, Req).


product_type_device(OperationID, Args,  Context, Req) ->
    NArgs = change_order(Args),
    alinkdata_common_service:handle_with_dao(select_product_type_children, OperationID, NArgs,  Context, Req).


scene_device(OperationID, Args,  Context, Req) ->
    NArgs = change_order(Args),
    alinkdata_common_service:handle_with_dao(select_scene_device, OperationID, NArgs,  Context, Req).


kick_device(_OperationID, #{<<"addr">> := Addr},  _Context, Req) when Addr =/= undefined ->
    Res =
        case catch modbus_server:kick_call(Addr) of
            {'EXIT', {timeout, _}} ->
                alinkdata_ajax_result:error_result(<<"响应超时"/utf8>>);
            _ ->
                alinkdata_ajax_result:success_result()
        end,
    alinkdata_common_service:response(Res, Req);
kick_device(_OperationID, _,  _Context, Req) ->
    Res = alinkdata_ajax_result:success_result([]),
    alinkdata_common_service:response(Res, Req).



send_cmd(_OperationID, #{<<"addr">> := _Addr},  _Context, _Req) ->
    alinkdata_ajax_result:success_result().


import(_OperationID, #{<<"path">> := Path} = Args, #{token := Token} = _Context, Req2) ->
    UpdateSupportB = alinkutil_type:to_binary(maps:get(<<"updateSupport">>, Args, <<"false">>)),
    #{<<"user">> := #{<<"userId">> := UserId, <<"deptId">> := DeptId}} = ehttpd_auth:get_session(Token),
%%    {ok, _Headers, Req1} = cowboy_req:read_part(Req),
%%    {ok, FileContent, Req2} = cowboy_req:read_part_body(Req1),
    {ok, UploadFilePre} = application:get_env(ehttpd, docroot),
    UploadFile = filename:join([UploadFilePre, "iotapi", alinkutil_type:to_list(Path)]),
    {ok, FileContent} = file:read_file(UploadFile),
    Res =
        case alinkdata_xlsx:from_file_data(FileContent, <<"device">>) of
            [] ->
                alinkdata_ajax_result:error_result(<<"导入设备数据不能为空！"/utf8>>);
            Devices ->
                {SuDevices, FaDevices} =
                    lists:foldl(
                        fun(#{<<"addr">> := Addr} = Device, {SuccessDevices, FailDevices}) ->
                            case alinkdata_dao:query_no_count('QUERY_device', #{<<"addr">> => Addr}) of
                                {ok, []} ->
                                    InsertDevice = Device#{<<"owner">> => UserId, <<"ownerDept">> => DeptId},
                                    case alinkdata_dao:query_no_count('POST_device', InsertDevice) of
                                        ok ->
                                            {[InsertDevice|SuccessDevices], FailDevices};
                                        {error, Reason} ->
                                            logger:error("insert device ~p failed: ~p", [Addr, Reason]),
                                            {SuccessDevices, [InsertDevice|FailDevices]}
                                    end;
                                {ok, [_ODevice]} when UpdateSupportB =:= <<"true">> ->
                                    InsertDevice = Device#{<<"owner">> => UserId, <<"ownerDept">> => DeptId},
                                    case alinkdata_dao:query_no_count('PUT_device', InsertDevice) of
                                        ok ->
                                            {[InsertDevice|SuccessDevices], FailDevices};
                                        {error, Reason} ->
                                            logger:error("insert addr ~p failed: ~p", [Addr, Reason]),
                                            {SuccessDevices, [InsertDevice|FailDevices]}
                                    end;
                                {ok, [_ODevice]} ->
                                    logger:error("device ~p already exist and do not need update", [Addr]),
                                    {SuccessDevices, [Device|FailDevices]};
                                {error, Reason} ->
                                    logger:error("query addr ~p failed:~p", [Addr, Reason]),
                                    {SuccessDevices, [Device|FailDevices]}
                            end;
                           (Device, {SuccessDevices, FailDevices}) ->
                            logger:error("device data ilegal, missing addr field"),
                            {SuccessDevices, [Device|FailDevices]}
                        end, {[], []}, Devices),
                case FaDevices of
                    [] ->
                        SuNumB = integer_to_binary(length(SuDevices)),
                        Msg0 = <<"全部导入成功,一共导入"/utf8>>,
                        Msg1 = <<"条数据"/utf8>>,
                        Msg = <<Msg0/binary, SuNumB/binary, Msg1/binary>>,
                        (alinkdata_ajax_result:success_result())#{<<"msg">> => Msg};
                    FDevices ->
                        SuNumB = integer_to_binary(length(SuDevices)),
                        FaNumB = integer_to_binary(length(FDevices)),
                        Msg0 = <<"导入成功"/utf8>>,
                        Msg1 = <<"条数据， 失败"/utf8>>,
                        Msg2 = <<"条数据， 失败数据如下："/utf8>>,
                        FailDataMsg = jiffy:encode(FDevices),
                        Msg = <<Msg0/binary, SuNumB/binary, Msg1/binary, FaNumB/binary, Msg2/binary, FailDataMsg/binary>>,
                        (alinkdata_ajax_result:success_result())#{<<"msg">> => Msg}
                end
        end,
    alinkdata_common_service:response(Res, Req2).


import_template(_OperationID, _Args, _Context, _Req) ->
    FileName = <<"device_template.xlsx">>,
    {ok, Content} = alinkdata_xlsx:file_template(FileName, <<"device">>),
    Headers = alinkdata_common:file_headers(Content, <<"\"", FileName/binary, "\"">>),
    {200, Headers, Content}.


write_to_device(_OperationID, #{<<"addr">> := Addr, <<"name">> := Name, <<"value">> := Value},  _Context, Req) when Addr =/= undefined ->
    Payload = jiffy:encode(#{<<"type">> => <<"write">>, <<"name">> => Name, <<"value">> => Value}),
    Message = emqx_message:from_map(#{
        topic => <<"p/in/", Addr/binary>>,
        payload => Payload,
        id => 1,
        qos => 0,
        from => <<"SYSTEM">>,
        flags => #{},
        headers => #{},
        timestamp => erlang:system_time()
    }),
    case alinkcore_cache:lookup(session, Addr) of
        {ok, Session} ->
            alinkcore_message_forward:dispatch(Session, Message);
        undefined ->
            ok
    end,
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_status_format(Addr) ->
    Stat = get_stat(Addr),
    Stat#{<<"addr">> => Addr}.


get_stat(Addr) ->
    case alinkcore_cache:get_session(Addr) of
        {ok, #{connect_time := ConnectTime}} ->
            #{<<"online">> => 1, <<"connectTime">> => ConnectTime};
        undefined ->
            #{<<"online">> => 0}
    end.

change_order(Args) ->
    case maps:get(<<"orderBy[desc]">>, Args, undefined) of
        undefined ->
            case maps:get(<<"orderBy[asc]">>, Args, undefined) of
                undefined ->
                    Args;
                OrderBy ->
                    Args#{<<"orderBy[asc]">> => change_order_snake(OrderBy, <<>>)}
            end;
        OrderBy ->
            Args#{<<"orderBy[desc]">> => change_order_snake(OrderBy, <<>>)}
    end.

change_order_snake(<<>>, Acc) ->
    Acc;
change_order_snake(<<A:8, Rest/binary>>, Acc) when A >= $A andalso A =< $Z->
    LowA = A + 32,
    NAcc = <<Acc/binary, "_", LowA>>,
    change_order_snake(Rest, NAcc);
change_order_snake(<<A:1/binary, Rest/binary>>, Acc) ->
    NAcc = <<Acc/binary, A/binary>>,
    change_order_snake(Rest, NAcc).