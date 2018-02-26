
-define(record_to_map(Def, Rec),
        maps:from_list(?record_to_proplist(Def, Rec))).

-define(record_to_map(Def, Rec, Fields),
        maps:from_list(?record_to_proplist(Def, Rec, Fields))).

-define(record_to_proplist(Def, Rec),
        lists:zip(record_info(fields, Def), tl(tuple_to_list(Rec)))).

-define(record_to_proplist(Def, Rec, Fields),
        [{K, V} || {K, V} <- ?record_to_proplist(Def, Rec), lists:member(K, Fields)]).

