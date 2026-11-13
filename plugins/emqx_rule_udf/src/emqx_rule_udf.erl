%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_udf).

-moduledoc """
The rule SQL function `udf`. Its first argument is a program name. The other
arguments go to the program:

```sql
SELECT udf('temp-convert.py', payload) AS t FROM "sensors/#"
```
""".

-export([register_sql_function/0, unregister_sql_function/0]).

%% Called by the rule engine.
-export([udf/1]).

-spec register_sql_function() -> ok.
register_sql_function() ->
    emqx_rule_engine:register_external_functions([{udf, {?MODULE, udf}}]).

-spec unregister_sql_function() -> ok.
unregister_sql_function() ->
    emqx_rule_engine:unregister_external_functions([udf]).

-doc """
The rule engine calls an external function with one argument: the list of the
evaluated SQL arguments.
""".
-spec udf([term()]) -> term().
udf([Name | Args]) when is_binary(Name) ->
    emqx_rule_udf_client:call(Name, Args);
udf(Args) ->
    error({bad_udf_call, #{expected => <<"udf('program-name', arg, ...)">>, got => Args}}).
