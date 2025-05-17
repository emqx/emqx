%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_utils).

-export([
    split/2
]).

-doc """
Split a string by a separator character, ignoring escaped separators.

## Examples

```erlang
emqx_ldap_utils:split("a*\**b", "*").
% ["a", "*", "b"]

```
""".
-spec split(string(), string()) -> [string()].
split(String, SepChar) ->
    split(String, SepChar, "", []).

split([SepChar | Rest], SepChar, CurAcc, Acc) ->
    split(Rest, SepChar, "", [lists:reverse(CurAcc) | Acc]);
split([], _SepChar, CurAcc, Acc) ->
    lists:reverse([lists:reverse(CurAcc) | Acc]);
split([$\\, Char | Rest], SepChar, CurAcc, Acc) ->
    split(Rest, SepChar, [Char, $\\ | CurAcc], Acc);
split([Char | Rest], SepChar, CurAcc, Acc) ->
    split(Rest, SepChar, [Char | CurAcc], Acc).
