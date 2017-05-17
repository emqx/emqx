-module (emqttd_cli_format).

-behavior(clique_writer).

%% API
-export([write/1]).

write([{text, Text}]) ->
    Json = jsx:encode([{text, lists:flatten(Text)}]),
    {io_lib:format("~p~n", [Json]), []};

write([{table, Table}]) ->
    Json = jsx:encode(Table),
    {io_lib:format("~p~n", [Json]), []};

write([{list, Key, [Value]}| Tail]) ->
    Table = lists:reverse(write(Tail, [{Key, Value}])),
    Json = jsx:encode(Table),
    {io_lib:format("~p~n", [Json]), []};

write(_) ->
    {io_lib:format("error~n", []), []}.

write([], Acc) ->
    Acc;
write([{list, Key, [Value]}| Tail], Acc) ->
    write(Tail, [{Key, Value}| Acc]).