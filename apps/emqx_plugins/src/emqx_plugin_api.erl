%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugin_api).

-export([dispatch/5]).
-export_type([callback_response/0]).

-callback handle(
    Method :: atom(),
    PathRemainder :: [binary()],
    Request :: map(),
    Context :: map()
) ->
    callback_response().

-type callback_response() ::
    {ok, Status :: pos_integer(), Headers :: map() | [{binary(), iodata()}], Body :: term()}
    | {error, Code :: atom() | binary() | string(), Msg :: iodata()}
    | {error, Status :: pos_integer(), Headers :: map() | [{binary(), iodata()}], Body :: term()}.

-spec dispatch(module(), atom(), [binary()], map(), map()) -> callback_response().
dispatch(Module, Method, PathRemainder, Request, Context) ->
    Module:handle(Method, PathRemainder, Request, Context).
