-module(mod_config_template).

-export([render/1]).

render(Config) ->
    render_entry(proplists:delete(placeholders, Config),
                 proplists:get_value(placeholders, Config)).

render_entry(Config, PlcHdlrs) ->
    render_entry(Config, PlcHdlrs, Config).

render_entry(Entry, PlcHdlrs, Config) when is_tuple(Entry) ->
    list_to_tuple(render_entry(tuple_to_list(Entry), PlcHdlrs, Config));
render_entry(Entry, PlcHdlrs, Config) when is_list(Entry) ->
    lists:foldl(fun(Item, Acc) ->
            case render_item(Item, PlcHdlrs, Config) of
                {var, Fun} when is_function(Fun) ->
                    Acc ++ [Fun(Config)];
                {var, Var} ->
                    Acc ++ [render_entry(Var, PlcHdlrs, Config)];
                {elems, Fun} when is_function(Fun) ->
                    Acc ++ Fun(Config);
                {elems, Elems} ->
                    Acc ++ render_entry(Elems, PlcHdlrs, Config)
            end
        end, [], Entry);
render_entry(Entry, _PlcHdlrs, _Config) ->
    Entry.

render_item("${"++Key0 = Entry0, PlcHdlrs, Config) ->
    Key = string:trim(Key0, trailing, "}"),
    case lists:keyfind(Key, 1, PlcHdlrs) of
        false -> {var, Entry0};
        {_, Type, Entry} ->
            {Type, render_entry(Entry, PlcHdlrs, Config)}
    end;
render_item(Entry, PlcHdlrs, Config) when is_tuple(Entry); is_list(Entry) ->
    {var, render_entry(Entry, PlcHdlrs, Config)};
render_item(Entry, _PlcHdlrs, _Config) ->
    {var, Entry}.
