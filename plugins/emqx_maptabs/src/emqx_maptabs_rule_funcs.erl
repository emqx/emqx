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
%% maptab_lookup(Table, Key, Field) -> field value | undefined
%% maptab_lookup(Table, Key, Field, Default) -> field value | Default
rsf_maptab_lookup([Table, Key]) ->
    emqx_maptabs:lookup(Table, Key);
rsf_maptab_lookup([Table, Key, Field]) ->
    emqx_maptabs:lookup(Table, Key, Field);
rsf_maptab_lookup([Table, Key, Field, Default]) ->
    emqx_maptabs:lookup(Table, Key, Field, Default).
