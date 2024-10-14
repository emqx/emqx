%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 04. 8月 2022 下午3:46
%%%-------------------------------------------------------------------
-module(alinkdata_service).
-author("yqfclid").

%% API
-export([select_service/4]).

-define(COMMON_SERVICE_HANDLE, {alinkdata_common_service, common_handle}).


%%%===================================================================
%%% API
%%%===================================================================
select_service(OperationID, _Args, Context, _Req) ->
    case maps:get(OperationID, handlers_mod(), no_service) of
        no_service ->
            get_action_dao(Context);
        ServiceHandler ->
            ServiceHandler
    end.

get_action_dao(#{extend := #{<<"table">> := Table}}) ->
    case alinkdata_dao_cache:is_generated_table(Table) of
        true ->
            {alinkdata_generated_table_service, handle};
        false ->
            no_service
    end;
get_action_dao(_) ->
    no_service.



handlers_mod() ->
    #{
        get_table_user_list => {alinkdata_user_service, select_user},
        get_row_user_userid => {alinkdata_user_service, get_info},
        post_row_user =>  {alinkdata_user_service, add_user},
        put_row_user => {alinkdata_user_service, edit_user},
        delete_row_user_userid => {alinkdata_user_service, remove_user},
        put_table_user_changestatus => {alinkdata_user_service, change_status},
        get_table_user_profile => {alinkdata_user_service, get_profile},
        put_table_user_profile => {alinkdata_user_service, update_profile},
        put_table_user_profile_updatePwd => {alinkdata_user_service, update_pwd},
        post_export_user => {alinkdata_user_service, export},
        put_table_user_profile_updatepwd => {alinkdata_user_service, update_pwd},
        post_table_user_importtemplate => {alinkdata_user_service, import_template},
        post_table_user_importdata => {alinkdata_user_service, import},
        put_table_user_resetpwd => {alinkdata_user_service, reset_pwd},
        get_table_user_authrole_userid => {alinkdata_user_service, get_auth_role},
        put_table_user_authrole => {alinkdata_user_service, edit_auth_role},
        get_table_user_wechat_qr => {alinkdata_auth_user_service, wechat_qr},
        post_table_user_wechat_verify => {alinkdata_auth_user_service, verify_wechat},
        post_table_user_wechat_bind => {alinkdata_auth_user_service, bind_wechat},
        post_table_user_wechat_unbind => {alinkdata_auth_user_service, unbind_wechat},
        post_system_user_wechat_login => {alinkdata_auth_user_service, login_with_wechat},

        get_table_role_list => {alinkdata_role_service, select_role_list},
        get_row_role_roleid => {alinkdata_role_service, query_role},
        post_row_role =>  {alinkdata_role_service, add_role},
        put_row_role => {alinkdata_role_service, edit_role},
        delete_row_role_roleid => {alinkdata_role_service, remove_role},
        put_table_role_changestatus => {alinkdata_role_service, change_status},
        put_table_role_datascope => {alinkdata_role_service, data_scope},
        post_export_role => {alinkdata_role_service, export},
        get_table_role_optionselect => {alinkdata_role_service, option_select},
        get_table_role_authuser_allocatedlist => {alinkdata_auth_user_service, select_allocated_list},
        get_table_role_authuser_unallocatedlist => {alinkdata_auth_user_service, select_unallocated_list},
        put_table_role_authuser_cancel => {alinkdata_auth_user_service, cancel},
        put_table_role_authuser_cancelall => {alinkdata_auth_user_service, cancel_all},
        put_table_role_authuser_selectall => {alinkdata_auth_user_service, select_all},

        get_table_post_list => ?COMMON_SERVICE_HANDLE,
        get_row_post_postid => {alinkdata_post_service, query_post},
        post_row_post =>  {alinkdata_post_service, add_post},
        put_row_post => {alinkdata_post_service, edit_post},
        delete_row_post_postid => {alinkdata_post_service, remove_post},
        get_table_post_optionselect => ?COMMON_SERVICE_HANDLE,
        post_export_post => {alinkdata_post_service, export},


        get_table_notice_list => ?COMMON_SERVICE_HANDLE,
        get_row_notice_noticeid => {alinkdata_notice_service, query_notice},
        post_row_notice =>  {alinkdata_notice_service, add_notice},
        put_row_notice => {alinkdata_notice_service, edit_notice},
        delete_row_notice_noticeid => {alinkdata_notice_service, remove_notice},


        get_table_menu_list => {alinkdata_menu_service, select_menu_list},
        get_row_menu_menuid => {alinkdata_menu_service, query_menu},
        post_row_menu =>  {alinkdata_menu_service, add_menu},
        put_row_menu => {alinkdata_menu_service, edit_menu},
        delete_row_menu_menuid => {alinkdata_menu_service, remove_menu},


        get_table_dept_list => {alinkdata_dept_service, list_dept},
        get_row_dept_deptid => {alinkdata_dept_service, query_dept},
        post_row_dept =>  {alinkdata_dept_service, add_dept},
        put_row_dept => {alinkdata_dept_service, edit_dept},
        delete_row_dept_deptid => {alinkdata_dept_service, remove_dept},
        get_row_dept_roledepttreeselect_roleid => {alinkdata_dept_service, select_role_dept_tree},
        get_table_dept_list_exclude_deptid => {alinkdata_dept_service, list_exclude_dept},


        get_table_dict_type_list => ?COMMON_SERVICE_HANDLE,
        get_row_dict_type_dictid => {alinkdata_dict_type_service, query_dict_type},
        post_row_dict_type =>  {alinkdata_dict_type_service, add_dict_type},
        put_row_dict_type => {alinkdata_dict_type_service, edit_dict_type},
        delete_row_dict_type_dictid => {alinkdata_dict_type_service, remove_dict_type},
        get_table_dict_type_optionselect => ?COMMON_SERVICE_HANDLE,
        post_export_dict_type => {alinkdata_dict_type_service, export},


        get_table_dict_data_list => ?COMMON_SERVICE_HANDLE,
        get_row_dict_data_dictcode => {alinkdata_dict_data_service, query_dict_data},
        post_row_dict_data =>  {alinkdata_dict_data_service, add_dict_data},
        put_row_dict_data => {alinkdata_dict_data_service, edit_dict_data},
        delete_row_dict_data_dictcode => {alinkdata_dict_data_service, remove_dict_data},
        post_export_dict_data => {alinkdata_dict_data_service, export},

        get_table_config_list => ?COMMON_SERVICE_HANDLE,
        get_row_config_configid => {alinkdata_config_service, query_config},
        post_row_config =>  {alinkdata_config_service, add_config},
        put_row_config => {alinkdata_config_service, edit_config},
        delete_row_config_configid => {alinkdata_config_service, remove_config},
        post_export_config => {alinkdata_config_service, export},


        get_table_oper_log_list => ?COMMON_SERVICE_HANDLE,
        delete_row_oper_log_operid => {alinkdata_log_service, remove_oper_log},
        post_export_oper_log => {alinkdata_log_service, export_oper_log},

        get_table_logininfor_list => ?COMMON_SERVICE_HANDLE,
        delete_row_logininfor_infoid => {alinkdata_log_service, remove_loginfo},
        post_export_logininfor => {alinkdata_log_service, export_loginfo},

        get_table_device_cehouanalyze => {alinkdata_cehou_service, analyze},
        post_table_import_device => {alinkdata_device_service, import},
        get_table_device_online => {alinkdata_device_service, device_status},
        post_table_device_instruction => {alinkdata_device_service, write_to_device},
        get_table_device_children => {alinkdata_device_service, device_children},
        post_table_device_kick => {alinkdata_device_service, kick_device},
        post_system_control_device => {alinkdata_device_service, send_cmd},
        get_system_devbyscene_list => {alinkdata_device_service, scene_device},
        get_system_device_history_last => {alinkiot_tdengine, get_device_last_row},
        get_system_device_log_list => {alinkiot_tdengine, query_device_log},
        get_system_device_camera_live => {alinkdata_yinshi_service, get_live_address},
        post_table_device_importtemplate => {alinkdata_device_service, import_template},
        post_system_importtemplate => {alinkdata_global_service, file_template},

        get_system_user_phone_code => {alinkdata_auth_user_service, get_verify_code},
        post_system_user_phone_login => {alinkdata_auth_user_service, login_with_phone},
        post_system_user_mini_login => {alinkdata_auth_user_service, login_with_mini},
        post_system_user_mini_phone_bind => {alinkdata_auth_user_service, bind_mini_phone_number},
        get_siteinfo => {alinkdata_global_service, site_info},
        post_system_user_app_login => {alinkdata_app_service, login_with_appid},
        post_system_gettoken => {alinkdata_app_service, login_with_appid},
        get_system_metrics_statsinfo => {alinkdata_collect_service, stats_info},
        get_system_metrics_device => {alinkdata_collect_service, metrics_device},
        get_system_metrics_alarm => {alinkdata_collect_service, metrics_alarm},
        post_table_user_mini_bind => {alinkdata_auth_user_service, bind_mini},

        post_table_app_sync => {alinkdata_app_service, sync_data},
        post_table_project_noticesync => {alinkdata_project_service, sync_data}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================
