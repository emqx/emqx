%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_RECORD_TO_MAP_HRL).
-define(EMQX_RECORD_TO_MAP_HRL, true).

-define(record_to_map(RECORD, VALUE),
    (fun(Val) ->
        Fields = record_info(fields, RECORD),
        [_Tag | Values] = tuple_to_list(Val),
        maps:from_list(lists:zip(Fields, Values))
    end)(
        VALUE
    )
).

-endif.
