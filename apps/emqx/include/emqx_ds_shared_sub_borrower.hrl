%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_SHARED_SUB_BORROWER_HRL).
-define(EMQX_DS_SHARED_SUB_BORROWER_HRL, true).

-record(message_to_borrower, {
    borrower_id :: emqx_ds_shared_sub_proto:borrower_id(),
    message :: term()
}).

-endif.
