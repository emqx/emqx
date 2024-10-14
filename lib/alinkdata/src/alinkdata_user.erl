-module(alinkdata_user).
-include("alinkdata.hrl").
%% API
-export([start/0, login/4, logout/3, get_info/2, get_routers/3, register/3, log/3]).

-export([
    login_by_wechat/2,
    query_user_by_open_id/1,
    record_login_info/4,
    get_admin_open_id/0,
    query_user_by_phone_number/1,
    query_user_by_id/1,
    query_user_by_mini_union_id/1,
    query_user_by_mini/1,
    change_poassword_to_md5/0,
    select_menu_tree_by_userId/2,
    get_project_routes/1
]).

-spec start() -> ok.
start() ->
    ehttpd_hook:add('user.login', {?MODULE, login}),
    ehttpd_hook:add('user.logout', {?MODULE, logout}),
    ehttpd_hook:add('user.get_info', {?MODULE, get_info}),
    ehttpd_hook:add('user.get_routers', {?MODULE, get_routers}),
    ehttpd_hook:add('user.register', {?MODULE, register}),
    ehttpd_hook:add('log.request', {?MODULE, log}),
    ehttpd_hook:add('user.login_by_wechat', {?MODULE, login_by_wechat}),
    alinkdata_wechat:fresh_wechat_env(),
    alinkdata_sms:fresh_sms_env(),
    alinkdata_wechat_mini:fresh_wechat_mini_env(),
    ok.


register(#{<<"username">> := UserName,
          <<"confirmPassword">> := Password,
          <<"password">> := Password} = Args, _Context, Result)  ->
    case alinkdata_dao:query_no_count(select_config, #{<<"configKey">> => <<"registerUser">>}) of
        {ok, [#{<<"configValue">> := ConfigV}]} when ConfigV =/= <<"true">> ->
            {ok, Result#{code => 500, msg => <<"当前系统没有开启注册功能！"/utf8>>}};
        {ok, _} when UserName =:= <<>> ->
            {ok, Result#{code => 500, msg => <<"用户名不能为空"/utf8>>}};
        {ok, _} when Password =:= <<>> ->
            {ok, Result#{code => 500, msg => <<"密码不能为空"/utf8>>}};
        {ok, _} ->
            case {byte_size(UserName), byte_size(Password)} of
                {UL, _} when UL < 2 orelse UL > 20 ->
                    {ok, Result#{code => 500, msg => <<"账户长度必须在2到20个字符之间"/utf8>>}};
                {_, PL} when PL < 2 orelse PL > 20 ->
                    {ok, Result#{code => 500, msg => <<"密码长度必须在2到20个字符之间"/utf8>>}};
                _ ->
                    case alinkdata_user_service:check_user_name_unique(UserName) of
                        ok ->
                            InsertArg = #{
                                <<"createBy">> => UserName,
                                <<"nickName">> => maps:get(<<"nickName">>, Args, UserName),
                                <<"userName">> => UserName,
                                <<"password">> => alinkutil_alg:md5(Password),
                                <<"wechat">> => maps:get(<<"wechat">>, Args, 0),
                                <<"phonenumber">> => maps:get(<<"phone">>, Args, <<>>)
                            },
                            case alinkdata_dao:query_no_count(insert_user, InsertArg) of
                                ok ->
                                    {ok, [#{<<"userId">> := UserId}]} = alinkdata_dao:query_user_by_user_name(#{<<"userName">> => UserName}),
                                    insert_default_role(UserId),
                                    {ok, alinkdata_ajax_result:success_result()};
                                {error, Reason} ->
                                    logger:error("insert user with username ~p password ~p failed: ~p",
                                        [UserName, Password, Reason]),
                                    {ok, Result#{code => 500, msg => <<"注册失败,请联系系统管理人员"/utf8>>}}
                            end;
                        {error, Reason} ->
                            {ok, Result#{code => 500, msg => Reason}}
                    end
            end;
        {error, Reason} ->
            logger:error("get config key sys.account.registerUser error :~p", [Reason]),
            {ok, Result#{code => 500, msg => <<"internal error">>}}
    end;
register(#{<<"confirmPassword">> := _P1, <<"password">> := _P2} = _Args, _Context, Result) ->
    {ok, Result#{code => 500, msg => <<"两次输入的密码不一致"/utf8>>}};
register(_Arg, _Context, Result) ->
    {ok, Result#{code => 500, msg => <<"internal error">>}}.



login_by_wechat(WechatSession, Log) ->
    case alinkdata_wechat:get_session(WechatSession) of
        {ok, #{<<"open_id">> := OpenId}} ->
            case query_user_by_open_id(OpenId) of
                {ok, #{<<"user">> := #{<<"userName">> := UserName}} = UserInfo} ->
                    Ts = os:system_time(second),
                    Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                    TTL = ehttpd_server:get_env(default, expire, 1800),
                    ehttpd_auth:put_session(Token, UserInfo, TTL),
                    record_login_info(true, UserName, Log, <<"登录成功"/utf8>>),
                    {ok, Token};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

login(UserName, Password, Log, Result) ->
    case check_continue_login(UserName) of
        true ->
            {ok, Result#{
                code => 500,
                msg => <<"登录过于频繁，请稍后重试"/utf8>>
            }};
        false ->
            case query_user(UserName, Password) of
                {ok, UserInfo} ->
                    Ts = os:system_time(second),
                    Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                    TTL = ehttpd_server:get_env(default, expire, 1800),
                    ehttpd_auth:put_session(Token, UserInfo, TTL),
                    record_login_info(true, UserName, Log, <<"登录成功"/utf8>>),
                    {ok, Result#{
                        code => 200,
                        token => Token
                    }};
                {error, Reason} ->
                    record_login_info(false, UserName, Log, Reason),
                    {stop, Result#{
                        code => 500,
                        msg => Reason
                    }}
            end
    end.


get_info(#{ user := UserInfo }, Result) ->
    Result1 = maps:merge(UserInfo, Result),
    {ok, Result1#{
        code => 200,
        msg => <<"success">>
    }}.


get_routers(#{<<"user">> := #{<<"userId">> := UserId}} = _Context, Req, Result) ->
    Project = ehttpd_req:get_qs(<<"project">>, Req),
    case select_menu_tree_by_userId(UserId, Project) of
        {ok, Menus} ->
            {ok, Result#{code => 200,  data => Menus}};
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            {stop, Result#{code => 500, msg => Msg}}
    end.
%%get_routers(_Context, Result) ->
%%    Table = <<?PREFIX/binary, "menu">>,
%%    Query = #{ <<"pageSize">> => 1000 },
%%    case alinkdata_mysql:query(default, Table, Query) of
%%        {ok, Rows} ->
%%            {ok, Result#{
%%                code => 200,
%%                data => create_route(Rows, #{})
%%            }};
%%        {error, Reason} ->
%%            Msg = list_to_binary(io_lib:format("~p", [Reason])),
%%            {stop, Result#{
%%                code => 500,
%%                msg => Msg
%%            }}
%%    end.

logout(_Args, Context, _Result) ->
    case maps:get(token, Context, undefined) of
        undefined ->
            {ok, alinkdata_ajax_result:success_result()};
        Token ->
            ehttpd_auth:delete_session(Token),
            {ok, alinkdata_ajax_result:success_result()}
    end.


log(UserInfo, Log, _Env) ->
    catch case record_oper_log(UserInfo, Log) of
              {'EXIT', Reason} ->
                  logger:error("record oper log crash ~p", [Reason]);
              _ ->
                  ok
          end,
    ok.

query_user_by_mini(MiniId) ->
    case alinkdata_dao:query_user_by_mini_id(#{<<"mini">> => MiniId}) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"登陆用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"登陆用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"登陆用户不存在"/utf8>>};
        {ok, [#{<<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {error, Reason} ->
            {error, Reason}
    end.


query_user_by_phone_number(PhoneNumber) ->
    case alinkdata_dao:query_user_by_phone_number(#{<<"phonenumber">> => PhoneNumber}) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"登陆用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"登陆用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"登陆用户不存在"/utf8>>};
        {ok, [#{<<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {error, Reason} ->
            {error, Reason}
    end.

query_user_by_open_id(OpenId) ->
    case alinkdata_dao:query_user_by_open_id(OpenId) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"登陆用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"登陆用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"登陆用户不存在"/utf8>>};
        {ok, [#{<<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {error, Reason} ->
            {error, Reason}
    end.


query_user_by_mini_union_id(UnionId) ->
    case alinkdata_dao:query_user_by_mini_union_id(UnionId) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"登陆用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"登陆用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"登陆用户不存在"/utf8>>};
        {ok, [#{<<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {error, Reason} ->
            {error, Reason}
    end.


query_user(UserName, Password) ->
    PasswordMd5 = alinkutil_alg:md5(Password),
    case alinkdata_dao:query_user_by_user_name(#{<<"userName">> => UserName}) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"登陆用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"登陆用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"登陆用户不存在"/utf8>>};
        {ok, [#{<<"password">> := PasswordMd5,
                <<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {ok, [_User]} ->
            {error, <<"密码错误"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


query_user_by_id(UserId) ->
    case alinkdata_dao:query_user_by_user_id(#{<<"userId">> => UserId}) of
        {ok, [#{<<"delFlag">> := <<"1">>}]} ->
            {error, <<"用户已被停用"/utf8>>};
        {ok, [#{<<"delFlag">> := <<"2">>}]} ->
            {error, <<"用户已被删除"/utf8>>};
        {ok, []} ->
            {error, <<"用户不存在"/utf8>>};
        {ok, [#{<<"userId">> := UserId} = User]} ->
            UserInfo =
                #{
                    <<"permissions">> => get_permissions(UserId),
                    <<"roles">> => get_roles(UserId),
                    <<"user">> => alinkdata_common:remove_password(User)
                },
            {ok, UserInfo};
        {ok, [_User]} ->
            {error, <<"请求错误"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


select_menu_tree_by_userId(UserId, Project) ->
    Dao =
        case alinkdata_common:is_admin(UserId) of
            true ->
                select_menu_tree_all;
            false ->
                select_menu_tree_by_user_id
        end,
    case alinkdata_dao:query_no_count(Dao, #{<<"userId">> => UserId}) of
        {ok, Menus} ->
            case catch get_project_routes(Project) of
                {Err, Reason} when Err == 'EXIT'; Err == error ->
                    {error, Reason};
                ProjectMenus ->
                    {ok, create_route(concat_menus(Menus, ProjectMenus), #{})}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

concat_menus(Menus, ProjectMenus) ->
    L = lists:filter(
        fun(Menu) ->
            maps:get(<<"path">>, Menu, undefined) == <<"index">>
        end, ProjectMenus),
    case length(L) > 0 of
        false ->
            Menus ++ ProjectMenus;
        true ->
            L1 = lists:filter(
                fun(Map) ->
                    maps:get(<<"path">>, Map, undefined) =/= <<"index">>
                end, Menus),
            L1 ++ ProjectMenus
    end.

get_project_routes(undefined) -> [];
get_project_routes(Project) ->
    Query = #{ <<"where">> => #{ <<"project">> => Project, <<"config_key">> => <<"menu">> }},
    case alinkdata_mysql:query(default, <<"sys_config">>,  Query) of
        {error, Reason} ->
            {error, Reason};
        {ok, Rows} ->
            lists:foldl(
                fun(Row, Acc) ->
                   case catch decode_menu(Project, Row) of
                       {'EXIT', Reason} ->
                           logger:error("decode menu error,~p,~p", [Row, Reason]),
                           Acc;
                       Map ->
                           [Map|Acc]
                   end
                end, [], Rows)
    end.

decode_menu(Project, #{ <<"config_id">> := Id, <<"config_value">> := Data }) ->
    Map = jiffy:decode(Data, [return_maps]),
    Query = maps:get(<<"query">>, Map, #{}),
    Map#{
        <<"project">> => Project,
        <<"query">> => jiffy:encode(Query),
        <<"menuId">> => list_to_binary(lists:concat(["p", Id]))
    }.


create_route([], Acc) ->
    Menus = maps:get(0, Acc, []),
    [add_children(Menu, Acc) || Menu <- sort_menus(Menus)];
create_route([#{<<"parentId">> := ParentId } = Row | Rows], Acc) ->
    case filter_route(Row) of
        false ->
            create_route(Rows, Acc);
        true ->
            Menus = maps:get(ParentId, Acc, []),
            create_route(Rows, Acc#{ParentId => [format_route(Row) | Menus]})
    end.

add_children(#{<<"menuId">> := Id} = Menu, Acc) ->
    case maps:get(Id, Acc, []) of
        [] ->
            Menu;
        L ->
            Children = [add_children(Child, Acc) || Child <- L],
            Menu#{
                <<"alwaysShow">> => true,
                <<"redirect">> => <<"noRedirect">>,
                <<"children">> => sort_menus(Children)
            }
    end.

sort_menus(Menus) ->
    lists:sort(fun sort_menus/2, Menus).

sort_menus(#{ <<"orderNum">> := A }, #{ <<"orderNum">> := B }) ->
    A < B;
sort_menus(_, _) -> false.


filter_route(#{ <<"menuType">> := <<"F">>}) ->
    false;
filter_route(#{<<"path">> := <<"#">>}) ->
    false;
filter_route(#{<<"path">> := <<>>}) ->
    false;
filter_route(_) ->
    true.

format_route(#{
    <<"menuId">> := Id,
    <<"menuName">> := Title,
    <<"path">> := Path,
    <<"visible">> := Visible,
    <<"isCache">> := IsCache,
    <<"component">> := Component,
    <<"parentId">> := ParentId,
    <<"isFrame">> := IsFrame,
    <<"orderNum">> := OrderNum
} = Route) ->
    #{
        <<"menuId">> => Id,
        <<"orderNum">> => OrderNum,
        <<"name">> => get_name(Path),
        <<"path">> => get_path(ParentId, Path),
        <<"hidden">> => Visible =/= <<"0">>,
        <<"component">> => get_component(Component),
        <<"meta">> => #{
            <<"title">> => Title,
            <<"icon">> => maps:get(<<"icon">>, Route, <<>>),
            <<"noCache">> => IsCache =/= <<"0">>,
            <<"link">> => get_link(IsFrame, Path)
        },
        <<"query">> => maps:get(<<"query">>, Route, <<>>)
    }.

get_component(null) ->
    <<"Layout">>;
get_component(<<>>) ->
    <<"ParentView">>;
get_component(Component) ->
    Component.

get_path(_, <<"http://", _/binary>> = Path) -> Path;
get_path(0, Path) -> <<"/", Path/binary>>;
get_path(_, Path) -> Path.

get_name(<<>>) -> <<>>;
get_name(<<P:1/bytes, Path/binary>>) ->
    P1 = list_to_binary(string:to_upper(binary_to_list(P))),
    <<P1/binary, Path/binary>>.

get_link(0, Path) -> Path;
get_link(_, _) -> null.



get_permissions(1) ->
    [<<"*:*:*">>];
get_permissions(UserId) ->
    {ok, Perms} = alinkdata_dao:query_no_count(select_menu_perms_by_user_id, #{<<"userId">> => UserId}),
    NeededPerms =
        [
            <<"system:user:router">>,
            <<"system:user:info">>,
            <<"system:profile:get">>,
            <<"system:profile:edit">>,
            <<"system:profile:update">>,
            <<"system:profile:updatepwd">>,
            <<"system:profile:resetPwd">>,
            <<"system:authRole:get">>,
            <<"system:authRole:update">>,
            <<"system:stats:*">>,
            <<"system:metrics:*">>
        ],
    lists:foldl(
        fun(#{<<"perms">> := <<>>}, Acc) ->
            Acc;
           (#{<<"perms">> := null}, Acc) ->
            Acc;
           (Perm, Acc) ->
            lists:foldl(
                fun(SubPerm, Acc2) ->
                    [SubPerm|Acc2]
            end, Acc, binary:split(maps:get(<<"perms">>, Perm), <<",">>, [global]))
    end, NeededPerms, Perms).



get_roles(1) ->
    [<<"admin">>];
get_roles(UserId) ->
    {ok, Perms} = alinkdata_dao:query_no_count(select_role_permission_by_user_id, #{<<"userId">> => UserId}),
    lists:foldl(
        fun(<<>>, Acc) ->
            Acc;
            (Perm, Acc) ->
                lists:foldl(
                    fun(SubPerm, Acc2) ->
                        [SubPerm|Acc2]
                    end, Acc, binary:split(maps:get(<<"roleKey">>, Perm), <<",">>, [global]))
        end, [], Perms).

record_login_info(IsSuccess, UserName, Log, Reason) ->
    catch case do_record_login_info(IsSuccess, UserName, Log, Reason) of
              {'EXIT', Reason} ->
                  logger:error("record loginfo crashed", [Reason]);
              _ ->
                  ok
          end.

do_record_login_info(IsSuccess, UserName, Log, Reason) ->
    IpAddr =
        case binary:split(maps:get(<<"Peer">>, Log, <<":">>), <<":">>, [global]) of
            [Ip|_] ->
                Ip;
            _ ->
                <<>>
        end,
    Status =
        case IsSuccess of
            true ->
                <<"0">>;
            false ->
                <<"1">>
        end,
    Os =
        case maps:get(<<"OS">>, Log, <<>>) of
            undefined ->
                <<>>;
            OO ->
                OO
        end,
    UserAgent =
        case maps:get(<<"UserAgent">>, Log, <<>>) of
            undefined ->
                <<>>;
            UA ->
                UA
        end,
    Args = #{
        <<"userName">> => UserName,
        <<"ipaddr">> => IpAddr,
        <<"os">> => Os,
        <<"status">> => Status,
        <<"loginTime">> => timestamp2localtime_str(erlang:system_time(second)),
        <<"msg">> => Reason,
        <<"userAgent">> => UserAgent
    },
    case alinkdata_dao:build_sql_with_dao_id(insert_logininfor, Args, false) of
        {ok, Sql} ->
            alinkdata_batch_log:insert(Sql);
        {error, Reason} ->
            logger:error("get insert_logininfor sql  failed ~p", [Reason])
    end.



record_oper_log(UserInfo, Log) ->
    IpAddr =
        case binary:split(maps:get(<<"Peer">>, Log, <<":">>), <<":">>, [global]) of
            [Ip|_] ->
                Ip;
            _ ->
                <<>>
        end,
    User = maps:get(<<"user">>, UserInfo, #{}),
    OperName = maps:get(<<"userName">>, User, <<>>),
    Os =
        case maps:get(<<"OS">>, Log, <<>>) of
            undefined ->
                <<>>;
            OO ->
                OO
        end,
    UserAgent =
        case maps:get(<<"UserAgent">>, Log, <<>>) of
            undefined ->
                <<>>;
            UA ->
                UA
        end,
    Args = #{
        <<"requestMethod">> => maps:get(<<"Method">>, Log, <<>>),
        <<"operName">> => OperName,
        <<"operUrl">> => maps:get(<<"Path">>, Log, <<>>),
        <<"operIp">> => IpAddr,
        <<"os">> => Os,
        <<"userAgent">> => UserAgent,
        <<"operTime">> => timestamp2localtime_str(erlang:system_time(second))
    },
    case alinkdata_dao:build_sql_with_dao_id(insert_oper_log, Args, false) of
        {ok, Sql} ->
            alinkdata_batch_log:insert(Sql);
        {error, Reason} ->
            logger:error("get insert_oper_log sql  failed ~p", [Reason])
    end.




timestamp2localtime_str(TimeStamp) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:gregorian_seconds_to_datetime(TimeStamp + 3600 *8 + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
    list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[Y,M,D,H,Mi,S]))).



get_wechat(UserId) ->
    case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"id">> => UserId}) of
        {ok, []} ->
            #{};
        {ok, [WechatInfo]} ->
            WechatInfo;
        {error, Reason} ->
            logger:error("request wechat UserId~p failed:~p", [UserId, Reason])
    end.



insert_default_role(UserId) ->
    case alinkdata_dao:query_no_count(select_role_list, #{<<"roleKey">> => <<"guest">>}) of
        {ok, [#{<<"roleId">> := RoleId}|_]} ->
            UserRoleArgs = #{<<"iterms">> => [#{<<"roleId">> => RoleId, <<"userId">> => UserId}]},
            ok = alinkdata_dao:query_no_count(batch_user_role, UserRoleArgs);
        {ok, []} ->
            ok;
        {error, Reason} ->
            logger:error("request guest role error:~p", [Reason])
    end.



get_admin_open_id() ->
    case alinkdata_dao:query_no_count(select_user_by_id, #{<<"userId">> => 1}) of
        {ok, [#{<<"wechat">> := WechatId}]} when WechatId > 0 ->
            case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"id">> => WechatId}) of
                {ok, [#{<<"openId">> := OpenId}]} ->
                    {ok, OpenId};
                {ok, []} ->
                    {error, not_bind};
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, _} ->
            {error, adin_account_error};
        {error, Reason} ->
            {error, Reason}
    end.


check_continue_login(UserName) ->
    Now = erlang:system_time(second),
    BeginTime = alinkdata_wechat:timestamp2localtime_str(Now - 300),
    Args = #{<<"userName">> => UserName, <<"beginDateTime">> => BeginTime, <<"status">> => <<"1">>},
    case alinkdata_dao:query_no_count(select_login_infor_list, Args) of
        {ok, Logins} ->
            length(Logins) >= 5;
        _ ->
            false
    end.

change_poassword_to_md5() ->
    case alinkdata:query_mysql_format_map(default, <<"select * from sys_user">>) of
        {ok, Users} ->
            lists:foreach(
                fun(#{<<"user_id">> := Id, <<"password">> := Password}) ->
                    NPassword = alinkutil_alg:md5(Password),
                    IdB = alinkutil_type:to_binary(Id),
                    RR = alinkdata:query_mysql_format_map(default, <<"update sys_user set password = '", NPassword, "' where user_id = ", IdB/binary>>),
                    io:format("res ~p ~p ~p", [Id, Password, RR])
            end, Users);
        _ ->
            ok
    end.