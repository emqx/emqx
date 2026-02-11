%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugin_api).

-export([dispatch/5]).
-export_type([callback_response/0]).

-type header_name() :: atom() | binary() | string().
-type header_value() :: iodata().
-type headers_map() :: #{header_name() => header_value()}.
-type headers_list() :: [{header_name(), header_value()}].
-type headers() :: headers_map() | headers_list().
-type status_code() :: pos_integer().
-type error_code() :: atom() | binary() | string().

-callback handle(
    Method :: atom(),
    PathRemainder :: [binary()],
    Request :: map(),
    Context :: map()
) ->
    callback_response().

-type callback_response() ::
    {ok, Status :: status_code(), Headers :: headers(), Body :: term()}
    | {error, Code :: error_code(), Msg :: iodata()}
    | {error, Status :: status_code(), Headers :: headers(), Body :: term()}.

-spec dispatch(module(), atom(), [binary()], map(), map()) -> callback_response().
dispatch(Module, Method, PathRemainder, Request, Context) ->
    Module:handle(Method, PathRemainder, Request, Context).
