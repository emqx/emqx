%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 09. 3月 2023 上午12:44
%%%-------------------------------------------------------------------
-module(alinkalarm_consumer).
-author("yqfclid").

%% API
-export([start/0, stop/0]).

-export([
    clear_rule/3,
    clear_device/3,
    clear_product/3,
    message_publish/3,
    device_log_notify/1
]).

%%%===================================================================
%%% API
%%%===================================================================
%% add hook
start() ->
    ehttpd_hook:add('PUT_rule', {?MODULE, clear_rule}),
    ehttpd_hook:add('DELETE_rule', {?MODULE, clear_rule}),
    ehttpd_hook:add('POST_rule', {?MODULE, clear_rule}),
    ehttpd_hook:add('PUT_product', {?MODULE, clear_product}),
    ehttpd_hook:add('DELETE_product', {?MODULE, clear_product}),
    ehttpd_hook:add('PUT_device', {?MODULE, clear_device}),
    ehttpd_hook:add('DELETE_device', {?MODULE, clear_device}),
    alinkdata_hooks:add('data.publish', {?MODULE, message_publish, []}),
    ok.


%% add hook
stop() ->
    ehttpd_hook:del('PUT_rule', {?MODULE, clear_rule}),
    ehttpd_hook:del('DELETE_rule', {?MODULE, clear_rule}),
    ehttpd_hook:del('POST_rule', {?MODULE, clear_rule}),
    ehttpd_hook:del('PUT_product', {?MODULE, clear_product}),
    ehttpd_hook:del('DELETE_product', {?MODULE, clear_product}),
    ehttpd_hook:del('PUT_device', {?MODULE, clear_device}),
    ehttpd_hook:del('DELETE_device', {?MODULE, clear_device}),
    alinkdata_hooks:del('data.publish', {?MODULE, message_publish}),
    ok.
%%%===================================================================
%%% Internal functions
%%%===============================================================
clear_product(_, #{<<"id">> := Ids}, _) when is_list(Ids) ->
    lists:foreach(
        fun(Id) ->
            alinkalarm_cache:clear(product, alinkutil_type:to_integer(Id))
    end, Ids);
clear_product(_, #{<<"id">> := Id}, _) ->
    alinkalarm_cache:clear(product, alinkutil_type:to_integer(Id));
clear_product(_, _, _) ->
    ok.


clear_device(_, #{<<"addr">> := Addrs}, _) when is_list(Addrs) ->
    lists:foreach(
        fun(Addr) ->
            alinkalarm_cache:clear(device, Addr)
        end, Addrs);
clear_device(_, #{<<"addr">> := Addr}, _) ->
    alinkalarm_cache:clear(device, Addr);
clear_device(_, _, _) ->
    ok.



clear_rule('POST_rule', #{<<"product">> := ProductId}, _) ->
    alinkalarm_product_rules:update_rules_by_product(ProductId);
clear_rule('POST_rule', _, _) ->
    ok;
clear_rule(Action, #{<<"id">> := Ids}, _) when is_list(Ids) ->
    lists:foreach(
        fun(Id) ->
            alinkalarm_cache:clear(rule, alinkutil_type:to_integer(Id)),
            case Action of
                'DELETE_rule' ->
                    alinkalarm_cache:clear(trigger_rule, alinkutil_type:to_integer(Id));
                _ ->
                    ok
            end,
            alinkalarm_product_rules:update_rules_by_id(Id)
        end, Ids);
clear_rule(Action, #{<<"id">> := Id}, _) ->
    alinkalarm_cache:clear(rule, alinkutil_type:to_integer(Id)),
    case Action of
        'DELETE_rule' ->
            alinkalarm_cache:clear(trigger_rule, alinkutil_type:to_integer(Id));
        _ ->
            ok
    end,
    alinkalarm_product_rules:update_rules_by_id(Id).


message_publish(ProductId, Addr, Data) ->
    MsgStat = #{
        <<"stat">> => Data,
        <<"deviceAddr">> => Addr,
        <<"productId">> => ProductId
    },
    alinkalarm_handler:handle(MsgStat).


device_log_notify(#{
    <<"addr">> := Addr,
    <<"productId">> := ProductId,
    <<"time">> := Time,
    <<"result">> := 1,
    <<"event">> := <<"auth">>}) ->
    message_publish(ProductId, Addr, #{<<"ts">> => Time, <<"status">> => #{<<"value">> => <<"online">>}});
device_log_notify(#{
    <<"addr">> := Addr,
    <<"productId">> := ProductId,
    <<"time">> := Time,
    <<"event">> := <<"logout">>}) ->
    case alinkcore_cache:query_product(ProductId) of
        {ok, #{<<"node_type">> := NodeType}} when NodeType =/= <<"children">> ->
            message_publish(ProductId, Addr, #{<<"ts">> => Time, <<"status">> => #{<<"value">> => <<"offline">>}});
        _ ->
            ok
    end;
device_log_notify(_) ->
    ok.


