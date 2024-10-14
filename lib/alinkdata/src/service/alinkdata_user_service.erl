%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 8月 2022 下午11:03
%%%-------------------------------------------------------------------
-module(alinkdata_user_service).
-author("yqfclid").

%% API
-export([
    select_user/4,
    get_info/4,
    add_user/4,
    edit_user/4,
    remove_user/4,
    change_status/4,
    get_profile/4,
    update_profile/4,
    update_pwd/4,
    export/4,
    import/4,
    import_template/4,
    reset_pwd/4,
    get_auth_role/4,
    edit_auth_role/4
]).


-export([check_user_name_unique/1]).

%%%===================================================================
%%% API
%%%===================================================================
select_user(OperationID, Args, #{token := Token} = Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, <<"u">>),
    alinkdata_common_service:common_handle(OperationID, NArgs, Context, Req).



get_info(_OperationID, Args, _Context, Req) ->
    UserId = maps:get(<<"userId">>, Args, <<"0">>),
    {ok, Roles} = alinkdata_dao:query_no_count(select_role_list, #{}),
    {ok, Posts} = alinkdata_dao:query_no_count(select_post_all, #{}),
    FilterdRoles =
        case alinkdata_common:is_admin(UserId) of
            true ->
                Roles;
            false ->
                lists:filter(fun(Role) ->
                        not alinkdata_common:is_admin(maps:get(<<"roleId">>, Role))
                end, Roles)
        end,
    case UserId =:= <<"0">> orelse UserId =:= 0 of
        true ->
            Res = (alinkdata_ajax_result:success_result())#{
                <<"posts">> => Posts,
                <<"roles">> => FilterdRoles
            },
            alinkdata_common_service:response(Res, Req);
        _ ->
            {ok, [#{<<"roles">> := UserRoles} = User]} = select_sys_user(select_user_by_id, Args),
            {ok, UserPostIds} = alinkdata_dao:query_no_count(select_post_list_by_user_id, Args),
            Res = (alinkdata_ajax_result:success_result(alinkdata_common:remove_password(User)))#{
                <<"posts">> => Posts,
                <<"roles">> => FilterdRoles,
                <<"postIds">> => lists:map(fun(Post) -> maps:get(<<"postId">>, Post) end, UserPostIds),
                <<"roleIds">> => lists:map(fun(#{<<"roleId">> := RId}) -> RId end, UserRoles)
            },
            alinkdata_common_service:response(Res, Req)
    end.



add_user(_OperationID, Args, #{token := Token} = _Context, Req) ->
    UserName = maps:get(<<"userName">>, Args),
    Email = maps:get(<<"email">> , Args, undefined),
    Phone = maps:get(<<"phonenumber">> , Args, undefined),
    Res =
        case check_unique(UserName, Email, Phone) of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(insert_user, Args#{<<"createBy">> => SelfUserName}),
                {ok, [#{<<"userId">> := UserId}]} = select_sys_user(select_user_by_user_name, Args),
                case  maps:get(<<"postIds">>, Args, []) of
                    [] ->
                        ok;
                    PostIds ->
                        PostIterms =
                            lists:map(fun(PostId) ->
                                    #{<<"postId">> => PostId, <<"userId">> => UserId}
                                end, PostIds),
                        UserPostArgs = #{<<"iterms">> => PostIterms},
                        ok = alinkdata_dao:query_no_count(batch_user_post, UserPostArgs)
                end,
                case  maps:get(<<"roleIds">>, Args, []) of
                    [] ->
                        ok;
                    RoleIds ->
                        RoleIterms =
                            lists:map(fun(RoleId) ->
                                    #{<<"roleId">> => RoleId, <<"userId">> => UserId}
                                end, RoleIds),
                        UserRoleArgs = #{<<"iterms">> => RoleIterms},
                        ok = alinkdata_dao:query_no_count(batch_user_role, UserRoleArgs)
                end,
                alinkdata_ajax_result:success_result();
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).






edit_user(_OperationID, Args, #{token := Token} = _Context, Req) ->
    UserId = maps:get(<<"userId">>, Args, undefined),
    Email = maps:get(<<"email">> , Args, undefined),
    Phone = maps:get(<<"phonenumber">> , Args, undefined),
    #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
    CheckAllowed =
        case check_user_allowed(UserId) of
            ok ->
                case check_user_data_scope(UserId, Token) of
                    ok ->
                        case check_phone_unique(Phone, UserId) of
                            ok ->
                                case check_email_unique(Email, UserId) of
                                    ok ->
                                        ok;
                                    {error, Reason} ->
                                        {error, Reason}
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case CheckAllowed of
            ok ->
                NArgs = Args#{<<"updateBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(delete_user_role_by_userId, NArgs),
                ok = alinkdata_dao:query_no_count(delete_user_post_by_userId, NArgs),
                case lists:filter(fun(PostId) -> PostId =/= null end, maps:get(<<"postIds">>, Args, [])) of
                    [] ->
                        ok;
                    PostIds ->
                        PostIterms =
                            lists:map(fun(PostId) ->
                                #{<<"postId">> => PostId, <<"userId">> => UserId}
                                      end, PostIds),
                        UserPostArgs = #{<<"iterms">> => PostIterms},
                        ok = alinkdata_dao:query_no_count(batch_user_post, UserPostArgs)
                end,
                case lists:filter(fun(RoleId) -> RoleId =/= null end, maps:get(<<"roleIds">>, Args, [])) of
                    [] ->
                        ok;
                    RoleIds ->
                        RoleIterms =
                            lists:map(fun(RoleId) ->
                                #{<<"roleId">> => RoleId, <<"userId">> => UserId}
                                      end, RoleIds),
                        UserRoleArgs = #{<<"iterms">> => RoleIterms},
                        ok = alinkdata_dao:query_no_count(batch_user_role, UserRoleArgs)
                end,
                ok = alinkdata_dao:query_no_count(update_user, NArgs),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).


remove_user(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := SelfUserId}} = ehttpd_auth:get_session(Token),
    UserIds = binary:split(maps:get(<<"userId">>, Args, []), <<",">>, [global]),
    CheckAllowed =
        case lists:member(SelfUserId, UserIds) of
            true ->
                {error, <<"当前用户不能删除"/utf8>>};
            false ->
                lists:foldl(
                    fun(UserId, ok) ->
                        case check_user_allowed(UserId) of
                            ok ->
                                case check_user_data_scope(UserId, Token) of
                                    ok ->
                                        ok;
                                    {error, Reason} ->
                                        {error, Reason}
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end;
                       (_, {error, Reason}) ->
                           {error, Reason}
                end , ok, UserIds)
        end,
    Res =
        case CheckAllowed of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_user_post, #{<<"userIds">> => UserIds}),
                ok = alinkdata_dao:query_no_count(delete_user_role, #{<<"userIds">> => UserIds}),
                ok = alinkdata_dao:query_no_count(delete_user_by_ids, #{<<"userIds">> => UserIds}),
                alinkdata_ajax_result:success_result();
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).



change_status(_OperationID, #{<<"userId">> := UserId} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_user_allowed(UserId) of
            ok ->
                case check_user_data_scope(UserId, Token) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"updateBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(update_user, NArgs),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).

get_profile(_OperationID, _Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := User} = ehttpd_auth:get_session(Token),
    #{<<"userName">> := UserName} = User,
    Res = alinkdata_ajax_result:success_result(User),
    {ok, Roles} = alinkdata_dao:query_no_count(select_roles_by_user_name, #{<<"userName">> => UserName}),
    RoleGroup =
        lists:foldl(
            fun(#{<<"roleName">> := RoleName}, <<>>) ->
                RoleName;
               (#{<<"roleName">> := RoleName}, Acc) ->
                <<Acc/binary, ",", RoleName/binary>>
        end, <<>>, Roles),
    {ok, Posts} = alinkdata_dao:query_no_count(select_posts_by_user_name, #{<<"userName">> => UserName}),
    PostGroup =
        lists:foldl(
            fun(#{<<"postName">> := PostName}, <<>>) ->
                PostName;
                (#{<<"postName">> := PostName}, Acc) ->
                    <<Acc/binary, ",", PostName/binary>>
            end, <<>>, Posts),

    NRes = Res#{<<"roleGroup">> => RoleGroup, <<"postGroup">> => PostGroup},
    alinkdata_common_service:response(NRes, Req).


update_profile(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := User} = UserInfo =  ehttpd_auth:get_session(Token),
    UserId = maps:get(<<"userId">>, User, undefined),
    Email = maps:get(<<"email">> , Args, undefined),
    Phone = maps:get(<<"phonenumber">> , Args, undefined),
    CheckAllowed =
        case check_phone_unique(Phone, UserId) of
            ok ->
                case check_email_unique(Email, UserId) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case CheckAllowed of
            ok ->
                ok = alinkdata_dao:query_no_count(update_user, Args#{<<"userId">> => UserId}),
                {ok, [NUser]} = select_sys_user(select_user_by_id, #{<<"userId">> => UserId}),
                TTL = application:get_env(expire, ehttpd, 18000),
                ehttpd_auth:put_session(Token, UserInfo#{<<"user">> => alinkdata_common:remove_password(NUser)}, TTL),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).


update_pwd(_OperationID, #{<<"oldPassword">> := OPwd, <<"newPassword">> := NPwd}, #{token := Token} = _Context, Req) ->
    #{<<"user">> := User} = UserInfo =  ehttpd_auth:get_session(Token),
    UserName = maps:get(<<"userName">>, User, undefined),
    UserId = maps:get(<<"userId">>, User, undefined),
    OPwdMd5 = alinkutil_alg:md5(OPwd),
    NPwdMd5 = alinkutil_alg:md5(NPwd),
    CheckAllowed =
        case  alinkdata_dao:query_no_count(select_user_by_id, #{<<"userId">> => UserId}) of
            {ok, [#{<<"password">> := PwdMd5}]} when PwdMd5 =/= OPwdMd5 ->
                {error, <<"修改密码失败，旧密码错误"/utf8>>};
            {ok, [#{<<"password">> := PwdMd5}]} when PwdMd5 =:= NPwdMd5 ->
                {error, <<"新密码不能与旧密码相同"/utf8>>};
            {ok, [_]} ->
                ok;
            {ok, _} ->
                {error, <<"用户配置错误"/utf8>>};
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case CheckAllowed of
            ok ->
                ok = alinkdata_dao:query_no_count(reset_user_pwd, #{<<"userName">> => UserName, <<"password">> => NPwdMd5}),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).



export(_OperationID, Args, #{token := Token}, _Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, <<"u">>),
    {ok,Users} = alinkdata_dao:query_no_count(select_user_list, NArgs),
    ContentUsers = lists:map(fun alinkdata_common:remove_password/1, Users),
    FileName = <<"user.xlsx">>,
    {ok, Content} = alinkdata_xlsx:to_xlsx_file_content(FileName, <<"user">>, ContentUsers),
    Headers = alinkdata_common:file_headers(Content, <<"\"", FileName/binary, "\"">>),
    {200, Headers, Content}.


import_template(_OperationID, Args, _Context, _Req) ->
    {ok,TableDesc} = alinkdata_dao:query_no_count(describe_user, Args),
    Fields =
        lists:map(
            fun(#{<<"Field">> := Field}) ->
                alinkdata_dao:transform_k(Field)
        end, TableDesc),
    ContentHeader =
        lists:foldl(
            fun(Field, <<>>) -> Field;
                (Field, ACC) -> <<ACC/binary, ",", Field/binary>>
            end, <<>>, Fields),
    Content = <<ContentHeader/binary, "\n">>,
    Headers = alinkdata_common:file_headers(Content, <<"\"user_template.csv\""/utf8>>),
    {200, Headers, Content}.

%%TODO
import(_OperationID, Args, #{token := Token} = _Context, Req) ->
    UpdateSupport = maps:get(<<"updateSupport">>, Args, <<"false">>),
    #{<<"user">> := #{<<"userName">> := SelfUsername}} = ehttpd_auth:get_session(Token),
    {ok, _Headers, Req1} = cowboy_req:read_part(Req),
    {ok, FileContent, Req2} = cowboy_req:read_part_body(Req1),
    Res =
        case alinkdata_common:from_file_data(FileContent) of
            [] ->
                alinkdata_ajax_result:error_result(<<"导入用户数据不能为空！"/utf8>>);
            Users ->
                {SuUsers, FaUsers} =
                    lists:foldl(
                        fun(User, {SuccessUsers, FailUsers}) ->
                            case select_sys_user(select_user_by_user_name, User) of
                                {ok, []} ->
                                    case alinkdata_dao:query_no_count(insert_user, User#{<<"createBy">> => SelfUsername}) of
                                        ok ->
                                            {[User|SuccessUsers], FailUsers};
                                        {error, Reason} ->
                                            logger:error("insert user ~p failed: ~p", [User, Reason]),
                                            {SuccessUsers, [User|FailUsers]}
                                    end;
                                {ok, [_OUser]} when UpdateSupport =:= <<"true">> ->
                                    case alinkdata_dao:query_no_count(update_user, User#{<<"createBy">> => SelfUsername}) of
                                        ok ->
                                            {[User|SuccessUsers], FailUsers};
                                        {error, Reason} ->
                                            logger:error("insert user ~p failed: ~p", [User, Reason]),
                                            {SuccessUsers, [User|FailUsers]}
                                    end;
                                {ok, [_OUsers]} ->
                                    logger:error("user ~p already exist and do not need update", [User]),
                                    {SuccessUsers, [User|FailUsers]};
                                {error, Reason} ->
                                    logger:error("query user ~p failed:~p", [User, Reason]),
                                    {SuccessUsers, [User|FailUsers]}
                            end
                    end, {[], []}, Users),
                case FaUsers of
                    [] ->
                        SuNumB = integer_to_binary(length(SuUsers)),
                        Msg0 = <<"全部导入成功,一共导入"/utf8>>,
                        Msg1 = <<"条数据"/utf8>>,
                        Msg = <<Msg0/binary, SuNumB/binary, Msg1/binary>>,
                        (alinkdata_ajax_result:success_result())#{<<"msg">> => Msg};
                    FUsers ->
                        SuNumB = integer_to_binary(length(SuUsers)),
                        FaNumB = integer_to_binary(length(FUsers)),
                        Msg0 = <<"导入成功"/utf8>>,
                        Msg1 = <<"条数据， 失败"/utf8>>,
                        Msg2 = <<"条数据， 失败数据如下："/utf8>>,
                        FailDataMsg = jiffy:encode(FUsers),
                        Msg = <<Msg0/binary, SuNumB/binary, Msg1/binary, FaNumB/binary, Msg2/binary, FailDataMsg/binary>>,
                        (alinkdata_ajax_result:success_result())#{<<"msg">> => Msg}
                end
        end,
    alinkdata_common_service:response(Res, Req2).


reset_pwd(_OperationID, Args, #{token := Token} = _Context, Req) ->
    UserId = maps:get(<<"userId">>, Args, undefined),
    #{<<"user">> := #{<<"userName">> := SelfUsername}} = ehttpd_auth:get_session(Token),
    NArgs = Args#{<<"updateBy">> => SelfUsername},
    CheckAllowed =
        case check_user_allowed(UserId) of
            ok ->
                case check_user_data_scope(UserId, Token) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,

    Res =
        case CheckAllowed of
            ok ->
                NArgs1 =
                    case maps:get(<<"password">>, NArgs, undefined) of
                        undefined ->
                            NArgs;
                        Pass ->
                            NArgs#{<<"password">> => alinkutil_alg:md5(Pass)}
                    end,
                ok = alinkdata_dao:query_no_count(update_user, NArgs1),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).

get_auth_role(_OperationID, #{<<"userId">> := UserId} = Args, #{token := Token} =_Context, Req) ->
    {ok, [User]} = select_sys_user(select_user_by_id, Args),
    Roles = select_role_by_user_id(Token, UserId),
    RetRoles =
        case alinkdata_common:is_admin(UserId) of
            true ->
                Roles;
            _ ->
                lists:filter(fun(#{<<"roleId">> := R}) -> not (alinkdata_common:is_admin(R)) end, Roles)
        end,
    NUser = alinkdata_common:remove_password(User),
    Res = (alinkdata_ajax_result:success_result())#{<<"user">> => NUser, <<"roles">> => RetRoles},
    alinkdata_common_service:response(Res, Req).

edit_auth_role(_OperationID, #{<<"userId">> := UserId, <<"roleIds">> := RoleIds} = Args, #{token := Token} =_Context, Req) ->
    CheckAllowed = check_user_data_scope(UserId, Token),
    NRoleIds =
        case RoleIds of
            <<>> ->
                [];
            _ ->
                lists:map(fun erlang:binary_to_integer/1, binary:split(RoleIds, <<",">>, [global]))
        end,
    Res =
        case CheckAllowed of
            ok when NRoleIds =:= []->
                ok = alinkdata_dao:query_no_count(delete_user_role_by_userId, Args),
                alinkdata_ajax_result:success_result();
            ok ->
                ok = alinkdata_dao:query_no_count(delete_user_role_by_userId, Args),
                BatchRoles =
                    lists:map(
                        fun(RId) ->
                            #{<<"userId">> => UserId, <<"roleId">> => RId}
                    end, NRoleIds),
                ok = alinkdata_dao:query_no_count(batch_user_role, #{<<"iterms">> => BatchRoles}),
                alinkdata_ajax_result:success_result();
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_unique(UserName, Email, Phone) ->
    case check_user_name_unique(UserName) of
        ok ->
            case check_phone_unique(Phone, undefined) of
                ok ->
                    case check_email_unique(Email, undefined) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


check_user_name_unique(UserName) ->
    case alinkdata_dao:query_no_count(check_user_name_unique, #{<<"userName">> => UserName}) of
        {ok, [#{<<"count(1)">> := 0}]} ->
            ok;
        {ok, _} ->
            {error, <<"用户名已存在"/utf8>>};
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            {error, Msg}
    end.

check_phone_unique(<<>>, _UserId) ->
    ok;
check_phone_unique(undefined, _UserId) ->
    ok;
check_phone_unique(Phone, UserId) ->
    case alinkdata_dao:query_no_count(check_phone_unique, #{<<"phonenumber">> => Phone}) of
        {ok, [#{<<"userId">> := UserId}]} when UserId =/= undefined ->
            ok;
        {ok, []} ->
            ok;
        {ok, _} ->
            {error, <<"手机号名已存在"/utf8>>};
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            {error, Msg}
    end.


check_email_unique(<<>>, _UserId) ->
    ok;
check_email_unique(undefined, _UserId) ->
    ok;
check_email_unique(Email, UserId) ->
    case alinkdata_dao:query_no_count(check_email_unique, #{<<"email">> => Email}) of
        {ok, [#{<<"userId">> := UserId}]} when UserId =/= undefined ->
            ok;
        {ok, []} ->
            ok;
        {ok, _} ->
            {error, <<"邮箱已存在"/utf8>>};
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            {error, Msg}
    end.

check_user_allowed(UserId) ->
    case lists:member(UserId, [<<>>, undefined]) orelse alinkdata_common:is_admin(UserId) of
        true ->
            {error, <<"无权操作该用户"/utf8>>};
        false ->
            ok
    end.



check_user_data_scope(UserId, Token) ->
    #{<<"user">> := #{<<"userId">> := SelfUserId}} = ehttpd_auth:get_session(Token),
    case alinkdata_common:is_admin(SelfUserId) of
        true ->
            ok;
        false ->
            Args = alinkdata_common_service:handle_data_scope(#{<<"userId">> => UserId}, Token, <<"d">>, <<"u">>),
            case alinkdata_dao:query_no_count(select_user_list, Args) of
                {ok, []} ->
                    {error, <<"没有权限访问用户数据！"/utf8>>};
                {error, Reason} ->
                    {error, Reason};
                _ ->
                    ok
            end
    end.



select_sys_user(DaoId, Args) ->
    case alinkdata_dao:query_no_count(DaoId, Args) of
        {ok, []} ->
            {ok, []};
        {ok, RawUsers} ->
            {ok, [build_sys_user(RawUsers)]};
        {error, Reason} ->
            {error, Reason}
    end.



build_sys_user([FirstUser|_] = Users) ->
    BaseProps =
        [
            <<"userId">>,
            <<"deptId">>,
            <<"userName">>,
            <<"nickName">>,
            <<"email">>,
            <<"phonenumber">>,
            <<"sex">>,
            <<"avatar">>,
            <<"password">>,
            <<"status">>,
            <<"delFlag">>,
            <<"loginIp">>,
            <<"loginDate">>,
            <<"createBy">>,
            <<"createTime">>,
            <<"updateBy">>,
            <<"updateTime">>,
            <<"remark">>,
            <<"openId">>
        ],
    DeptProps =
        [
            <<"deptId">>,
            <<"parentId">>,
            <<"deptName">>,
            <<"orderNum">>,
            <<"leader">>,
            <<"status">>
        ],
    RolesProps = [
        <<"roleId">>,
        <<"roleName">>,
        <<"roleKey">>,
        <<"dataScope">>,
        <<"status">>
    ],
    BaseUser =
        lists:foldl(
            fun(K, Acc) ->
                case maps:get(K, FirstUser, undefined) of
                    undefined ->
                        Acc;
                    V ->
                        Acc#{K => V}
                end
        end, #{}, BaseProps),
    Dept =
        lists:foldl(
            fun(K, Acc) ->
                case maps:get(K, FirstUser, undefined) of
                    undefined ->
                        Acc;
                    V ->
                        Acc#{K => V}
                end
        end, #{}, DeptProps),
    Roles =
        lists:map(
            fun(User) ->
                lists:foldl(
                    fun(K, Acc) ->
                        case maps:get(K, User, undefined) of
                            undefined ->
                                Acc;
                            V ->
                                Acc#{K => V}
                        end
                end, #{}, RolesProps)
        end, Users),
    BaseUser#{<<"dept">> => Dept, <<"roles">> => Roles}.


select_role_by_user_id(Token, UserId) ->
    {ok, UserRoles} = alinkdata_dao:query_no_count(select_role_permission_by_user_id, #{<<"userId">> => UserId}),
    NArgs = alinkdata_common_service:handle_data_scope(#{<<"userId">> => UserId}, Token, <<"d">>, undefined),
    {ok, Roles} = alinkdata_dao:query_no_count(select_role_all, NArgs),
    lists:map(
        fun(#{<<"roleId">> := RoleId} = Role) ->
            case lists:any(
                fun(#{<<"roleId">> := UserRoleId}) ->
                    UserRoleId =:= RoleId
            end, UserRoles) of
                true ->
                    Role#{<<"falg">> => true};
                false ->
                    Role
            end
    end, Roles).


