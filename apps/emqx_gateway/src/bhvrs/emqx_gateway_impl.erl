%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_impl).

-export_type([state/0]).

-include("emqx_gateway.hrl").

-type state() :: map().
-type reason() :: any().

%% @doc
-callback on_gateway_load(
    Gateway :: gateway(),
    Ctx :: emqx_gateway_ctx:context()
) ->
    {error, reason()}
    | {ok, [ChildPid :: pid()], GwState :: state()}
    %% TODO: v0.2 The child spec is better for restarting child process
    | {ok, [Childspec :: supervisor:child_spec()], GwState :: state()}.

%% @doc
-callback on_gateway_update(
    Config :: emqx_config:config(),
    Gateway :: gateway(),
    GwState :: state()
) ->
    ok
    | {ok, [ChildPid :: pid()], NGwState :: state()}
    | {ok, [Childspec :: supervisor:child_spec()], NGwState :: state()}
    | {error, reason()}.

%% @doc
-callback on_gateway_unload(Gateway :: gateway(), GwState :: state()) -> ok.
