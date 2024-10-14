%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2023 下午1:16
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine_subscribe).
-author("yqfclid").

%% API
-export([
    load/0,
    unload/0,
    sync_product/3,
    data_publish/3
]).

%%%===================================================================
%%% API
%%%===================================================================
load() ->
    ehttpd_hook:add('PUT_product', {?MODULE, sync_product}),
    ehttpd_hook:add('POST_product', {?MODULE, sync_product}),
%%    alinkdata_hooks:add('data.publish', {?MODULE, data_publish, []}),
    ets:insert(alinkdata_dao_cache, {{route, get_table_history_list}, {alinkiot_tdengine, query_history}}),
    ets:insert(alinkdata_dao_cache, {{route, post_export_history}, {alinkiot_tdengine, export_history}}),
    ok.


unload() ->
    ehttpd_hook:del('PUT_product', {?MODULE, sync_product}),
    ehttpd_hook:del('POST_product', {?MODULE, sync_product}),
%%    alinkdata_hooks:del('data.publish', {?MODULE, data_publish}),
    ets:delete(alinkdata_dao_cache, {route, get_table_history_list}),
    ets:delete(alinkdata_dao_cache, {route, post_export_history}),
    ok.


sync_product('PUT_product', #{<<"id">> := Id}, _) ->
    alinkiot_tdengine_sync:sync(alinkutil_type:to_integer(Id)),
    ok;
sync_product('POST_product', #{<<"thing">> := Thing}, _) when Thing =/= <<>> ->
    alinkiot_tdengine_sync:sync_add(),
    ok;
sync_product(_, _, _) ->
    ok.


data_publish(ProductId, Addr, Data) ->
    case alinkcore_cache:query_product(ProductId) of
        {ok, #{<<"thing">> := Thing}} ->
            case lists:member(Thing, [undefined, null, <<>>, []]) of
                true ->
                    ok;
                false ->
                    ThingNames = lists:map(fun(#{<<"name">> := Name}) -> Name end, Thing),
                    NData =
                        maps:fold(
                            fun(K, V, Acc) ->
                                case lists:member(K, ThingNames)  of
                                    true ->
                                        case maps:get(<<"value">>, V, <<"alink_ignore">>) of
                                            <<"alink_ignore", _/binary>> ->
                                                Acc;
                                            _ ->
                                                Acc#{K => V}
                                        end;
                                    false ->
                                        Acc
                                end
                        end, #{}, Data),
                    case maps:size(NData) > 0 of
                        true ->
                            InserData =
                                case maps:is_key(<<"alarm_rule">>, Data) of
                                    true ->
                                        NData#{
                                            <<"addr">> => Addr,
                                            <<"productId">> => ProductId,
                                            <<"ts">> => maps:get(<<"ts">>, Data, erlang:system_time(second)),
                                            <<"alarm_rule">> => maps:get(<<"alarm_rule">>, Data)
                                        };
                                    false ->
                                        NData#{
                                            <<"addr">> => Addr,
                                            <<"productId">> => ProductId,
                                            <<"ts">> => maps:get(<<"ts">>, Data, erlang:system_time(second))
                                        }
                                end,
                            alinkiot_tdengine_worker:insert(InserData);
                        _ ->
                            ignore
                    end
            end;
        {error, Reason} ->
            logger:error("get product thing error ~p", [Reason])
    end.
%%%===================================================================
%%% Application callbacks
%%%===================================================================
