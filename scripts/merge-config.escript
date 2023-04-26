#!/usr/bin/env escript

%% This script reads up emqx.conf and split the sections
%% and dump sections to separate files.
%% Sections are grouped between CONFIG_SECTION_BGN and
%% CONFIG_SECTION_END pairs
%%
%% NOTE: this feature is so far not used in opensource
%% edition due to backward-compatibility reasons.

-mode(compile).
-define(APPS, ["emqx", "emqx_dashboard", "emqx_authz"]).

main(_) ->
    {ok, BaseConf} = file:read_file("apps/emqx_conf/etc/emqx_conf.conf"),
    Cfgs = get_all_cfgs("apps/"),
    IsEnterprise = is_enterprise(),
    Enterprise =
        case IsEnterprise of
            false -> [];
            true -> [io_lib:nl(), "include emqx-enterprise.conf", io_lib:nl()]
        end,
    Conf = [
        merge(BaseConf, Cfgs),
        io_lib:nl(),
        Enterprise
    ],
    ok = file:write_file("apps/emqx_conf/etc/emqx.conf.all", Conf),

    case IsEnterprise of
        true ->
            EnterpriseCfgs = get_all_cfgs("lib-ee"),
            EnterpriseConf = merge(<<"">>, EnterpriseCfgs),
            ok = file:write_file("apps/emqx_conf/etc/emqx-enterprise.conf.all", EnterpriseConf);
        false ->
            ok
    end,
    merge_desc_files_per_lang("en"),
    %% TODO: remove this when we have zh translation moved to dashboard package
    merge_desc_files_per_lang("zh").

is_enterprise() ->
    Profile = os:getenv("PROFILE", "emqx"),
    nomatch =/= string:find(Profile, "enterprise").

merge(BaseConf, Cfgs) ->
    Confs = [BaseConf | lists:map(fun read_conf/1, Cfgs)],
    infix(lists:filter(fun(I) -> iolist_size(I) > 0 end, Confs), [io_lib:nl(), io_lib:nl()]).

read_conf(CfgFile) ->
    case filelib:is_regular(CfgFile) of
        true ->
            {ok, Bin1} = file:read_file(CfgFile),
            string:trim(Bin1, both);
        false ->
            <<>>
    end.

infix([], _With) -> [];
infix([One], _With) -> [One];
infix([H | T], With) -> [H, With, infix(T, With)].

get_all_cfgs(Root) ->
    Apps0 = filelib:wildcard("*", Root) -- ["emqx_machine", "emqx_conf"],
    Apps1 = (Apps0 -- ?APPS) ++ lists:reverse(?APPS),
    Dirs = [filename:join([Root, App]) || App <- Apps1],
    lists:foldl(fun get_cfgs/2, [], Dirs).

get_all_cfgs(Dir, Cfgs) ->
    Fun = fun(E, Acc) ->
        Path = filename:join([Dir, E]),
        get_cfgs(Path, Acc)
    end,
    lists:foldl(Fun, Cfgs, filelib:wildcard("*", Dir)).

get_cfgs(Dir, Cfgs) ->
    case filelib:is_dir(Dir) of
        false ->
            Cfgs;
        _ ->
            Files = filelib:wildcard("*", Dir),
            case lists:member("etc", Files) of
                false ->
                    try_enter_child(Dir, Files, Cfgs);
                true ->
                    EtcDir = filename:join([Dir, "etc"]),
                    %% the conf name must start with emqx
                    %% because there are some other conf, and these conf don't start with emqx
                    Confs = filelib:wildcard("emqx*.conf", EtcDir),
                    NewCfgs = [filename:join([EtcDir, Name]) || Name <- Confs],
                    try_enter_child(Dir, Files, NewCfgs ++ Cfgs)
            end
    end.

try_enter_child(Dir, Files, Cfgs) ->
    case lists:member("src", Files) of
        false ->
            Cfgs;
        true ->
            get_all_cfgs(filename:join([Dir, "src"]), Cfgs)
    end.

%% Desc files merge is for now done locally in emqx.git repo for all languages.
%% When zh and other languages are moved to a separate repo,
%% we will only merge the en files.
%% The file for other languages will be merged in the other repo,
%% the built as a part of the dashboard package,
%% finally got pulled at build time as a part of the dashboard package.
merge_desc_files_per_lang(Lang) ->
    BaseConf = <<"">>,
    Cfgs0 = get_all_desc_files(Lang),
    Conf = do_merge_desc_files_per_lang(BaseConf, Cfgs0),
    OutputFile = case Lang of 
        "en" ->
            %% en desc will always be in the priv dir of emqx_dashboard
             "apps/emqx_dashboard/priv/desc.en.hocon";
        "zh" ->
            %% so far we inject zh desc as if it's extracted from dashboard package
            %% TODO: remove this when we have zh translation moved to dashboard package
             "apps/emqx_dashboard/priv/www/static/desc.zh.hocon"
    end,
    ok = filelib:ensure_dir(OutputFile),
    ok = file:write_file(OutputFile, Conf).

do_merge_desc_files_per_lang(BaseConf, Cfgs) ->
    lists:foldl(
      fun(CfgFile, Acc) ->
              case filelib:is_regular(CfgFile) of
                  true ->
                      {ok, Bin1} = file:read_file(CfgFile),
                      [Acc, io_lib:nl(), Bin1];
                  false -> Acc
              end
      end, BaseConf, Cfgs).

get_all_desc_files(Lang) ->
    Dir =
        case Lang of
            "en" ->
                 filename:join(["rel", "i18n"]);
            "zh" ->
                %% TODO: remove this when we have zh translation moved to dashboard package
                 filename:join(["rel", "i18n", "zh"])
        end,
    Files = filelib:wildcard("*.hocon", Dir),
    lists:map(fun(Name) -> filename:join([Dir, Name]) end, Files).
