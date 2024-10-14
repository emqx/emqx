%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 02. 4月 2023 下午4:28
%%%-------------------------------------------------------------------
-module(rion_hm705_server).
-protocol(<<"RION-HM705">>).


-include("alinkcore.hrl").
%% API
-export([
    init/1,
    handle_in/3,
    handle_out/3
]).


%%%===================================================================
%%% API
%%%===================================================================
init([_Addr, _ConnInfo]) ->
    ok.



handle_in(ProductId, Addr, #{payload := Payload}) ->
    case jiffy:decode(Payload, [return_maps]) of
        #{<<"params">> := Params} ->
            case build_data(Params) of
                {ok, Data} ->
                    alinkcore_data:handle(ProductId, Addr, Data);
                {error, Reason} ->
                    logger:error("build param ~p to data failed:~p", [Params, Reason])
            end;
        _ ->
            ok
    end.



handle_out(ProductId, _Addr, #{payload := Payload} = Message) ->
    case jiffy:decode(Payload, [return_maps]) of
        #{<<"name">> := Name, <<"value">> := Value} ->
            NPayload =
                case check_pin(Name, ProductId) of
                    true ->
                        NName = list_to_binary(string:uppercase(binary_to_list(Name))),
                        jiffy:encode(#{<<"params">> => #{NName => Value}});
                    false ->
                        jiffy:encode(#{<<"params">> => #{}})
                end,
            emqx_message:from_map(Message#{payload => NPayload});
        _ ->
            emqx_message:from_map(Message)
    end.
%% ==================================================================
%%% Internal functions
%%%===================================================================

build_data(RData) when is_map(RData) ->
    Now = erlang:system_time(second),
    Data =
        maps:fold(
            fun(K, V, Acc) ->
                case lists:member(K, keys()) of
                    true ->
                        NK = list_to_binary(string:lowercase(binary_to_list(K))),
                        Acc#{NK => #{<<"value">> => V, <<"ts">> => Now}};
                    _ ->
                        Acc
                end
        end, #{<<"ts">> => Now}, RData),
    {ok, Data};
build_data(_) ->
    {error, badarg}.



keys() ->
    [
        <<"INDEX">>,
        <<"IMEI">>,
        <<"PID">>,
        <<"XANG">>,
        <<"YANG">>,
        <<"ZANG">>,
        <<"XACC">>,
        <<"YACC">>,
        <<"ZACC">>,
        <<"ALARM_FLG">>,
        <<"ALARM_TYPE">> ,
        <<"ANG">>,
        <<"ACC">>,
        <<"CLR_FLG">>,
        <<"FIX">> ,
        <<"FREQ">>,
        <<"RFAA">>,
        <<"ACTC">>,
        <<"CSQ">>,
        <<"EQC">>,
        <<"TEMP">>,
        <<"LNGTD">>,
        <<"LATTD">>,
        <<"SOFTVERSION">>
    ].



check_pin(Name, ProductId) ->
    case alinkcore_cache:query_product(ProductId) of
        {error, Reason} ->
            logger:error("query product ~p failed ~p", [ProductId, Reason]),
            false;
        {ok, #{ <<"thing">> := Things }} ->
            lists:any(
                fun(#{<<"name">> := N} = Thing) when N =:= Name ->
                    lists:member(maps:get(<<"access">>, Thing, <<"read">>), [<<"rw">>, <<"write">>]);
                   (_) ->
                    false
            end, Things)
    end.
