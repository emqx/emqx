%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_lib).

%% API
-export([
    fold_namespace_configs/3,
    fold_config/3
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

fold_namespace_configs(Fn0, Acc0, NsConfigs) ->
    maps:fold(
        fun(Namespace, RawConfig, Acc1) ->
            Fn = fun(Stack, Value, Acc) -> Fn0(Namespace, Stack, Value, Acc) end,
            fold_config(Fn, Acc1, RawConfig)
        end,
        Acc0,
        NsConfigs
    ).

fold_config(FoldFun, AccIn, Config) ->
    fold_config(FoldFun, AccIn, [], Config).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

fold_config(FoldFun, AccIn, Stack, Config) when is_map(Config) ->
    maps:fold(
        fun(K, SubConfig, Acc) ->
            fold_subconf(FoldFun, Acc, [K | Stack], SubConfig)
        end,
        AccIn,
        Config
    );
fold_config(FoldFun, Acc, Stack, []) ->
    fold_confval(FoldFun, Acc, Stack, []);
fold_config(FoldFun, Acc, Stack, Config) when is_list(Config) ->
    fold_confarray(FoldFun, Acc, Stack, 1, Config);
fold_config(FoldFun, Acc, Stack, Config) ->
    fold_confval(FoldFun, Acc, Stack, Config).

fold_confarray(FoldFun, AccIn, StackIn, I, [H | T]) ->
    Acc = fold_subconf(FoldFun, AccIn, [I | StackIn], H),
    fold_confarray(FoldFun, Acc, StackIn, I + 1, T);
fold_confarray(_FoldFun, Acc, _Stack, _, []) ->
    Acc.

fold_subconf(FoldFun, AccIn, Stack, SubConfig) ->
    case FoldFun(Stack, SubConfig, AccIn) of
        {cont, Acc} ->
            fold_config(FoldFun, Acc, Stack, SubConfig);
        {stop, Acc} ->
            Acc
    end.

fold_confval(FoldFun, AccIn, Stack, ConfVal) ->
    {_, Acc} = FoldFun(Stack, ConfVal, AccIn),
    Acc.
