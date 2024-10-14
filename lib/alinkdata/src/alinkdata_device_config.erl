%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 12月 2023 上午10:32
%%%-------------------------------------------------------------------
-module(alinkdata_device_config).
-author("yqfclid").

%% API
-export([
    get_config/1,
    set_config/4
]).

%%%===================================================================
%%% API
%%%===================================================================
get_config(Addr) ->
    case alinkdata:query_mysql_format_map(default, <<"select config from sys_device where addr = '", Addr/binary, "'">>) of
        {ok, [#{<<"config">> := Config}]} when is_binary(Config) andalso Config =/= <<>> ->
            jiffy:decode(Config, [return_maps]);
        {ok, [_]} ->
            [];
        Err ->
            Err
    end.

set_config(Addr, Name, Type, Value) ->
    CoonfigC = #{<<"name">> => Name, <<"type">> => Type, <<"value">> => Value},
    case get_config(Addr) of
        OldConfig when is_list(OldConfig) ->
            RemainConfig =
                lists:filter(
                    fun(#{<<"name">> := N}) ->
                        Name =/= N
                end,  OldConfig),
            ConfigB = jiffy:encode([CoonfigC|RemainConfig]),
            alinkdata:query_mysql_format_map(default, <<"update sys_device set config = '", ConfigB/binary, "' where addr = '", Addr/binary, "'">>);
        Err ->
           Err
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================