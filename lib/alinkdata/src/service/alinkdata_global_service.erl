%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 15. 5月 2023 下午10:35
%%%-------------------------------------------------------------------
-module(alinkdata_global_service).

%% API
-export([
    site_info/4,
    file_template/4
]).

%%%===================================================================
%%% API
%%%===================================================================
site_info(_OperationID, Args, _Context, Req) ->
    Query = case maps:get(<<"project">>, Args, undefined) of
                undefined -> #{ <<"project">> => <<"default">> };
                Project -> #{ <<"project">> => Project }
            end,
    case alinkdata_dao:query_no_count(select_config, Query) of
        {ok, []} ->
            alinkdata_common_service:response(alinkdata_ajax_result:error_result(404, <<"not found">>), Req);
        {ok, ConfigList} ->
            SiteInfo = trans_configs(ConfigList),
            alinkdata_common_service:response(alinkdata_ajax_result:success_result(SiteInfo), Req);
        {error, Reason} ->
            logger:info("get config failed ~p", [Reason]),
            alinkdata_common_service:response(alinkdata_ajax_result:error_result(<<"Internal Error">>), Req)
    end.

file_template(_OperationID, #{<<"table">> := <<"device">>} = Args, Context, Req) ->
    alinkdata_device_service:import_template(_OperationID, Args, Context, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
trans_configs(ConfigList) ->
    lists:foldl(fun foldl/2, #{}, ConfigList).


foldl(#{<<"configValue">> := <<>>}, Acc) -> Acc;
foldl(#{<<"configType">> := <<"Y">>} = Item, Acc) ->
    format_config(Item, Acc);
foldl(_, Acc) -> Acc.



format_config(#{<<"configKey">> := <<"site_info">>, <<"configValue">> := ConfigV}, Acc) ->
    Info = jiffy:decode(ConfigV, [return_maps]),
    maps:merge(Acc, Info);
format_config(#{ <<"configKey">> := Key } = Item, Acc) when
    Key == <<"menu">>; Key == <<"layout_setting">> ->
    Acc1 = maps:get(Key, Acc, []),
    Acc#{Key => [Item | Acc1]};
format_config(#{<<"configKey">> := ConfigK, <<"configValue">> := ConfigV}, Acc) ->
    Acc#{ConfigK => ConfigV}.
