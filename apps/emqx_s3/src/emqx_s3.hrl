%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(VIA_GPROC(Id), {via, gproc, {n, l, Id}}).

-define(SAFE_CALL_VIA_GPROC(Id, Message, Timeout, NoProcError),
    try gen_server:call(?VIA_GPROC(Id), Message, Timeout) of
        Result -> Result
    catch
        exit:{noproc, _} -> {error, NoProcError}
    end
).
