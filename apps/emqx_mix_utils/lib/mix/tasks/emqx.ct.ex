defmodule Mix.Tasks.Emqx.Ct do
  use Mix.Task

  # todo: invoke the equivalent of `make merge-config` as a requirement...
  @requirements ["compile", "loadpaths"]

  @impl true
  def run(args) do
    Mix.debug(true)

    opts = parse_args!(args)

    Enum.each([:common_test, :eunit, :mnesia], &add_to_path_and_cache/1)

    # app_to_debug = :emqx_conf
    # Mix.Dep.Umbrella.cached()
    # |> Enum.find(& &1.app == app_to_debug)
    # |> Mix.Dep.in_dependency(fn _dep_mix_project_mod ->
    #   config = Mix.Project.config()
    #   |> IO.inspect(label: app_to_debug)
    # end)

    ensure_whole_emqx_project_is_loaded!()
    unload_emqx_applications!()
    load_common_helpers!()
    hack_test_data_dirs!(opts.suites)

    # ensure_suites_are_loaded!(opts.suites)

    {_, 0} = System.cmd("epmd", ["-daemon"])
    node_name = :"test@127.0.0.1"
    :net_kernel.start([node_name, :longnames])
    logdir = Path.join([Mix.Project.build_path(), "logs"])
    File.mkdir_p!(logdir)
    {:ok, _} = Application.ensure_all_started(:cth_readable)

    # unmangle PROFILE env because some places (`:emqx_conf.resolve_schema_module`) expect
    # the version without the `-test` suffix.
    System.fetch_env!("PROFILE")
    |> String.replace_suffix("-test", "")
    |> then(& System.put_env("PROFILE", &1))

    # {_, _, _} = ["test_server:do_init_tc_call -> return"] |> Enum.map(&to_charlist/1) |> :redbug.start()

    maybe_start_cover()

    results = :ct.run_test(
      abort_if_missing_suites: true,
      auto_compile: false,
      suite: opts |> Map.fetch!(:suites) |> Enum.map(&to_charlist/1),
      group: opts |> Map.fetch!(:group_paths) |> Enum.map(fn gp -> Enum.map(gp, &String.to_atom/1) end),
      testcase: opts |> Map.fetch!(:cases) |> Enum.map(&to_charlist/1),
      readable: 'true',
      name: node_name,
      ct_hooks: [:cth_readable_shell, :cth_readable_failonly],
      logdir: to_charlist(logdir)
    )

    symlink_to_last_run(logdir)

    case results do
      {_success, failed, {_user_skipped, auto_skipped}} when failed > 0 or auto_skipped > 0 ->
        Mix.raise("failures running tests: #{failed} failed, #{auto_skipped} auto skipped")

      {success, _failed = 0, {user_skipped, _auto_skipped = 0}} ->
        Mix.shell().info("Test suites ran successfully.  #{success} succeeded, #{user_skipped} skipped by the user")
    end
  end

  @doc """
  Here we ensure the _modules_ are loaded, not the applications.

  Specially useful for suites that use utilities such as `emqx_common_test_helpers` and
  similar.

  This needs to run before we unload the applications, otherwise we lose their `:modules`
  attribute.
  """
  def ensure_whole_emqx_project_is_loaded!() do
    apps_path =
      Mix.Project.project_file()
      |> Path.dirname()
      |> Path.join("apps")

    apps_path
    |> File.ls!()
    |> Stream.filter(fn app_name ->
      apps_path
      |> Path.join(app_name)
      |> File.dir?()
    end)
    |> Stream.map(&String.to_atom/1)
    |> Enum.flat_map(fn app ->
      case :application.get_key(app, :modules) do
        {:ok, mods} ->
          mods

        other ->
          # IO.inspect({app, other}, label: :bad_mods)
          []
      end
    end)
    |> Code.ensure_all_loaded!()
  end

  @doc """
  We do this because some callbacks rely on the side-effect of the application being
  loaded.  We leave starting them to `:emqx_cth_suite:start`.

  For example, `:emqx_dashboard:apps/0` uses that to decide which specs to load.
  """
  def unload_emqx_applications!() do
    for {app, _, _} <- Application.loaded_applications(),
        to_string(app) =~ ~r/^emqx/ do
      :ok = Application.unload(app)
    end
  end

  defp load_common_helpers!() do
    Code.ensure_all_loaded!([
      :emqx_common_test_helpers,
      :emqx_bridge_testlib,
      :emqx_bridge_v2_testlib,
      :emqx_authn_http_test_server,
    ])
  end

  defp hack_test_data_dirs!(suites) do
    project_root = Path.dirname(Mix.Project.project_file())

    suites
    |> Stream.map(fn suite_path ->
      src_data_dir =
        project_root
        |> Path.join(Path.rootname(suite_path) <> "_data")
      data_dir_exists? = File.dir?(src_data_dir)
      IO.inspect(binding())
      if data_dir_exists? do
        suite_mod =
          suite_path
          |> Path.basename(".erl")
          |> String.to_atom()
        ebin_path = get_mod_ebin_path(suite_mod)
        data_dir = Path.basename(src_data_dir)
        dest_data_path =
          [ebin_path, "..", data_dir]
          |> Path.join()
          |> Path.expand()
        File.rm(dest_data_path)
        case File.ln_s(src_data_dir, dest_data_path) do
          :ok ->
            :ok

          {:error, :eexist} ->
            :ok
        end
        IO.inspect(binding())
      end
    end)
    |> Stream.run()
  end

  defp get_mod_ebin_path(mod) do
    case :code.which(mod) do
      :cover_compiled ->
        Mix.raise("todo (see test_server_ctrl:get_data_dir/2)")

      full_path when is_list(full_path) ->
        full_path
    end
  end

  defp ensure_suites_are_loaded!(suites) do
    suites
    |> Enum.map(fn suite_path ->
      ["apps", app_name | _] = Path.split(suite_path)
      String.to_atom(app_name)
    end)
    |> Enum.uniq()
    |> Enum.flat_map(fn app ->
      {:ok, mods} = :application.get_key(app, :modules)
      mods
    end)
    |> Code.ensure_all_loaded!()
  end

  def add_to_path_and_cache(lib_name) do
    :code.lib_dir()
    |> Path.join("#{lib_name}-*")
    |> Path.wildcard()
    |> hd()
    |> Path.join("ebin")
    |> to_charlist()
    |> :code.add_path(:cache)
  end

  defp parse_args!(args) do
    {opts, _rest} = OptionParser.parse!(
      args,
      strict: [
        suites: :string,
        group_paths: :string,
        cases: :string])
    suites = get_name_list(opts, :suites)
    group_paths =
      opts
      |> get_name_list(:group_paths)
      |> Enum.map(& String.split(&1, ".", trim: true))
    cases = get_name_list(opts, :cases)

    if suites == [] do
      Mix.raise("must define at least one suite using --suites path/to/suite1.erl,path/to/suite2.erl")
    end

    %{
      suites: suites,
      group_paths: group_paths,
      cases: cases
    }
  end

  defp get_name_list(opts, key) do
    opts
    |> Keyword.get(key, "")
    |> String.split(",", trim: true)
  end

  def cover_enabled?() do
    case System.get_env("ENABLE_COVER_COMPILE") do
      "1" -> true
      "true" -> true
      _ -> false
    end
  end

  defp maybe_start_cover() do
    if cover_enabled?() do
      {:ok, _} = start_cover()
    end
  end

  defp start_cover() do
    case :cover.start() do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp symlink_to_last_run(log_dir) do
    last_dir =
      log_dir
      |> File.ls!()
      |> Stream.filter(& &1 =~ ~r/^ct_run/)
      |> Enum.sort()
      |> List.last()

    if last_dir do
      dest = Path.join(log_dir, "last")
      File.rm(dest)
      File.ln_s!(last_dir, dest)
    end
  end
end
