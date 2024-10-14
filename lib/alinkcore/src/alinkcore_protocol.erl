-module(alinkcore_protocol).
-export([get_mod/1, load/0, get_childeren_mod/2]).

get_mod(Protocol) ->
    case lists:keyfind(Protocol, 1, load()) of
        false -> undefined;
        {Protocol, Mod} -> Mod
    end.


get_childeren_mod(Addr,RProtocol) ->
    Protocol =
        case alinkcore_cache:query_children(Addr) of
            {ok, Children} ->
                lists:foldl(
                    fun(#{<<"addr">> := SAddr}, Acc) ->
                        case alinkcore_cache:query_device(SAddr) of
                            {ok, #{<<"product">> := SProductId}} ->
                                case alinkcore_cache:query_product(SProductId) of
                                    {ok, #{<<"protocol">> := <<"Modbus-JY-R100">>}} ->
                                        <<"Modbus-JY-R100">>;
                                    {ok, #{<<"protocol">> := <<"rouxingcexie-huasi">>}} ->
                                        <<"rouxingcexie-huasi">>;
                                    _ ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end, RProtocol, Children);
            _ ->
                RProtocol
        end,
    case lists:keyfind(Protocol, 1, load()) of
        false -> undefined;
        {Protocol, Mod} -> Mod
    end.

path() ->
    filename:dirname(code:which(?MODULE)).

load() ->
    Path = path(),
    lists:foldl(
        fun(Mod, Acc) ->
            Attributes = Mod:module_info(attributes),
            lists:concat([[{Protocol, Mod} || {protocol, [Protocol]} <- Attributes], Acc])
        end, [], get_modules(Path)).

get_modules(Dir) ->
    case file:list_dir(Dir) of
        {ok, FS} ->
            lists:foldl(
                fun(FileName, Acc) ->
                    case filename:extension(FileName) == ".beam" of
                        true ->
                            Mod = filename:basename(FileName, ".beam"),
                            [list_to_atom(Mod) | Acc];
                        false ->
                            Acc
                    end
                end, [], FS);
        {error, _Reason} ->
            []
    end.
