%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(NODE1_PORT, 18085).
-define(NODE2_PORT, 18086).
-define(NODE3_PORT, 18087).
-define(api_base_url(_Port_), ("http://127.0.0.1:" ++ (integer_to_list(_Port_)))).

-define(UPLOAD_EE_BACKUP, "emqx-export-upload-ee.tar.gz").
-define(UPLOAD_CE_BACKUP, "emqx-export-upload-ce.tar.gz").
-define(BAD_UPLOAD_BACKUP, "emqx-export-bad-upload.tar.gz").
-define(BAD_IMPORT_BACKUP, "emqx-export-bad-file.tar.gz").
-define(DASHBOARD_USER, <<"admin_for_test">>).
-define(DASHBOARD_PASS, <<"public_for_test_1">>).
-define(VIEWER_USER, <<"viewer_for_test">>).
-define(VIEWER_PASS, <<"public_for_test_1">>).
-define(NS_ADMIN_USER, <<"ns_admin_for_test">>).
-define(NS_ADMIN_PASS, <<"public_for_test_1">>).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).
