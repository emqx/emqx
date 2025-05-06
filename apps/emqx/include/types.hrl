%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-type option(T) :: undefined | T.

-type startlink_ret() :: {ok, pid()} | ignore | {error, term()}.

-type ok_or_error(Reason) :: ok | {error, Reason}.

-type ok_or_error(Value, Reason) :: {ok, Value} | {error, Reason}.

-type mfargs() :: {module(), atom(), [term()]}.
