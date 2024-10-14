%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 19. 6月 2023 下午9:12
%%%-------------------------------------------------------------------
-module(alinkdata_yinshi_service).

%% API
-export([
    get_live_address/4
]).

%%%===================================================================
%%% API
%%%===================================================================
get_live_address(_OperationID, #{<<"addr">> := Addr}, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count('QUERY_device', #{<<"addr">> => Addr}) of
            {ok, [#{<<"config">> := Config, <<"product">> := ProductId}|_]} ->
                case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
                    {ok, [#{<<"pk">> := Key, <<"ps">> := Secret}]} ->
                        case alinkdata_yinshi:get_access_token_raw_return(Key, Secret) of
                            {ok, Data} ->
                                alinkdata_ajax_result:success_result(Data#{<<"config">> => Config});
                            {error, Reason} ->
                                logger:error("get ~p live adress failed ~p", [Addr, Reason]),
                                alinkdata_ajax_result:error_result(<<"Internal Error">>)
                        end;
                    {ok, _} ->
                        alinkdata_ajax_result:error_result(<<"设备类型错误"/utf8>>);
                    {error, Reason} ->
                        logger:error("get addr ~p live address failed ~p", [Addr, Reason]),
                        alinkdata_ajax_result:error_result(<<"Internal Error">>)
                end;
            {ok, _} ->
                alinkdata_ajax_result:error_result(<<"设备类型错误"/utf8>>);
            {error, Reason} ->
                logger:error("get addr ~p live address failed ~p", [Addr, Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).




%%%===================================================================
%%% Internal functions
%%%===================================================================
find_key_from_config(_Name, []) ->
    undefined;
find_key_from_config(Name, [#{<<"name">> := Name} = ConfigDetail|_]) ->
    ConfigDetail;
find_key_from_config(Name, [_|T]) ->
    find_key_from_config(Name, T).
