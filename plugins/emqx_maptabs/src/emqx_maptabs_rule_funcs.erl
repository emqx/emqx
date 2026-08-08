%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Rule SQL functions exported by the emqx_maptabs plugin.
%%
%% Registered via `emqx_rule_engine:register_external_functions/1':
%% every `rsf_'-prefixed arity-1 export becomes a rule SQL function,
%% and the SQL-level arguments arrive as a single list.
-module(emqx_maptabs_rule_funcs).

-export([rsf_maptab_lookup/1]).

%% maptab_lookup(Table, Key) -> row value map | undefined
%% maptab_lookup(Table, Key, DefaultRow) -> row value map | DefaultRow
%%   (the third argument is a map, e.g. map_new() or a json_decode'd
%%   fallback row; a field name is always a string, so the two
%%   three-argument forms cannot be confused)
%% maptab_lookup(Table, Key, Field) -> field value | undefined
%% maptab_lookup(Table, Key, Field, Default) -> field value | Default
rsf_maptab_lookup([Table, Key]) ->
    emqx_maptabs:lookup(Table, Key);
rsf_maptab_lookup([Table, Key, DefaultRow]) when is_map(DefaultRow) ->
    case emqx_maptabs:lookup(Table, Key) of
        undefined -> DefaultRow;
        Values -> Values
    end;
rsf_maptab_lookup([Table, Key, Field]) ->
    emqx_maptabs:lookup(Table, Key, Field);
rsf_maptab_lookup([Table, Key, Field, Default]) ->
    emqx_maptabs:lookup(Table, Key, Field, Default).
