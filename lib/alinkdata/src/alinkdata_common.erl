%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 12. 8月 2022 上午12:24
%%%-------------------------------------------------------------------
-module(alinkdata_common).
-author("yqfclid").

%% API
-export([
    is_admin/1,
    file_headers/2,
    to_file_content/1,
    from_file_data/1,
    to_binary/1,
    remove_password/1
]).

%%%===================================================================
%%% API
%%%===================================================================
is_admin(<<"1">>) -> true;
is_admin(1) -> true;
is_admin(_) -> false.


file_headers(_FileBin, FileName) ->
    #{
        <<"content-type">> => <<"application/octet-stream">>,
%%        <<"Accept-Ranges">> => <<"bytes">>,
%%        <<"Accept-Length">> => byte_size(FileBin),
        <<"Content-Disposition">> => <<"attachment; filename=", FileName/binary>>
    }.


to_file_content([]) ->
    [];
to_file_content(Maps) ->
    Fields = proplists:get_keys(maps:to_list(hd(Maps))),
    FieldsContent =
        lists:foldl(
            fun(Field, <<>>) -> Field;
                (Field, ACC) -> <<ACC/binary, ",", Field/binary>>
        end, <<>>, Fields),
    Contents =
        lists:foldl(
            fun(Map, Acc) ->
                Content =
                    lists:foldl(
                        fun(Field, <<>>) ->
                            to_binary(maps:get(Field, Map, <<>>));
                           (Field, Acc1) ->
                            V = to_binary(maps:get(Field, Map, <<>>)),
                            <<Acc1/binary, ",", V/binary>>
                    end, <<>>, Fields),
                <<Acc/binary, "\n", Content/binary>>
        end, <<>>, Maps),
    <<FieldsContent/binary, Contents/binary>>.


from_file_data(RawContents) ->
    [FieldLine0|ContentLines] = binary:split(RawContents, <<"\n">>, [global]),
    FieldLine = binary:replace(FieldLine0, <<"\r">>, <<>>, [global]),
    Fields = binary:split(FieldLine, <<",">>, [global]),
    lists:foldl(
        fun(ContentLine0, Acc) ->
            ContentLine = binary:replace(ContentLine0, <<"\r">>, <<>>, [global]),
            case binary:split(ContentLine, <<",">>, [global]) of
                [<<>>] ->
                    Acc;
                ContentValues ->
                    Len =
                        case length(Fields) > length(ContentValues) of
                            true ->
                                length(ContentValues);
                            false ->
                                length(Fields)
                        end,
                    Content =
                        lists:foldl(
                            fun(Idx, Acc1) ->
                                K = lists:nth(Idx, Fields),
                                case lists:nth(Idx, ContentValues) of
                                    <<"null">> -> Acc1;
                                    V -> Acc1#{K => V}
                                end
                            end, #{}, lists:seq(1, Len)),
                    [Content|Acc]
            end
    end, [], ContentLines).


remove_password(Map) ->
    maps:remove(<<"password">>, Map).
%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec(to_binary(S :: any()) -> binary()).
to_binary(S) when is_atom(S) ->
    atom_to_binary(S);
to_binary(S) when is_list(S) ->
    list_to_binary(S);
to_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(S) when is_float(S) ->
    float_to_binary(S);
to_binary(S) when is_binary(S) ->
    S;
to_binary(S) ->
    throw({badarg, S}).