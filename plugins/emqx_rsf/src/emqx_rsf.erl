%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rsf).

-moduledoc """
The rule SQL function `rsf`. Its first argument is a program name. The other
arguments go to the program:

```sql
SELECT rsf('temp-convert.py', payload) AS t FROM "sensors/#"
```
""".

-export([register_sql_function/0, unregister_sql_function/0]).

%% Called by the rule engine.
-export([rsf/1]).

-spec register_sql_function() -> ok.
register_sql_function() ->
    emqx_rule_engine:register_external_functions([{rsf, {?MODULE, rsf}}]).

-spec unregister_sql_function() -> ok.
unregister_sql_function() ->
    emqx_rule_engine:unregister_external_functions([rsf]).

-doc """
The rule engine calls an external function with one argument: the list of the
evaluated SQL arguments.
""".
-spec rsf([term()]) -> term().
rsf([Name | Args]) when is_binary(Name) ->
    emqx_rsf_client:call(Name, Args);
rsf(Args) ->
    error({bad_rsf_call, #{expected => <<"rsf('program-name', arg, ...)">>, got => Args}}).
