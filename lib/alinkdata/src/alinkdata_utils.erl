-module(alinkdata_utils).

%% API
-export([create_tree/2, create_tree/4, join/2]).

-export([to_binary/1]).

create_tree(Rows, Format) ->
    create_tree(Rows, <<"pid">>, <<"id">>, Format).

create_tree(Rows, ParentKey, IdKey, Format) ->
    Acc = create_group(Rows, ParentKey, Format, #{}),
    Idx = lists:min(maps:keys(Acc)),
    RootNodes = maps:get(Idx, Acc, []),
    [create_node(IdKey, Node, Acc) || Node <- RootNodes].

create_group([], _, _, Acc) -> Acc;
create_group([Row0 | Rows], ParentKey, Format, Acc) ->
    Row = Format(Row0),
    ParentId = maps:get(ParentKey, Row),
    Children = maps:get(ParentId, Acc, []),
    create_group(Rows, ParentKey, Format, Acc#{ParentId => [Row | Children]}).

create_node(IdKey, Node, Acc) ->
    Id = maps:get(IdKey, Node),
    case maps:get(Id, Acc, []) of
        [] ->
            Node;
        Children ->
            Node#{<<"children">> => [create_node(IdKey, Child, Acc) || Child <- Children]}
    end.

%%query_ets(Tab, PageNo, PageSize, RowFun) ->
%%    Qh = qlc:q([R || R <- ets:tab2list(Tab)]),
%%    Cursor = qlc:cursor(Qh),
%%    case PageNo > 1 of
%%        true -> qlc:next_answers(Cursor, (PageNo - 1) * PageSize);
%%        false -> ok
%%    end,
%%    Rows = qlc:next_answers(Cursor, PageSize),
%%    qlc:delete_cursor(Cursor),
%%    [RowFun(Row) || Row <- Rows].

join(_Sep, []) -> [];
join(Sep, [H | T]) -> [H | join_prepend(Sep, T)].

join_prepend(_Sep, []) -> [];
join_prepend(Sep, [H | T]) when is_binary(H) ->
    [Sep, binary_to_list(H) | join_prepend(Sep, T)];
join_prepend(Sep, [H | T]) ->
    [Sep, H | join_prepend(Sep, T)].



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
