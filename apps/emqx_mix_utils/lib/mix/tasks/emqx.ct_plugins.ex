defmodule Mix.Tasks.Emqx.CtPlugins do
  use Mix.Task

  alias EMQXUmbrella.MixProject, as: UMP
  alias Mix.Tasks.Emqx.Ct, as: CT

  @requirements ["compile", "loadpaths"]

  @shortdoc "Run common tests for plugin suites"

  @moduledoc """
  Runs CT suites for plugin apps under `plugins/`.

  This task is dedicated to plugin suites so we can resolve precompiled suite modules
  from their ebin paths without changing the regular `mix emqx.ct` behavior for apps/.
  """

  @impl true
  def run(args) do
    CT.ensure_test_mix_env!()
    UMP.set_test_env!(true)

    opts = parse_args!(args)
    validate_plugin_suites!(opts.suites)

    Enum.each([:common_test, :eunit, :mnesia], &CT.add_to_path_and_cache/1)

    CT.ensure_whole_emqx_project_is_loaded()
    CT.unload_emqx_applications!()
    load_common_helpers!()
    hack_test_data_dirs!(opts.suites)

    {_, 0} = System.cmd("epmd", ["-daemon"])
    node_name = :"test@127.0.0.1"
    Node.start(node_name, :longnames)
    logdir = Path.join([Mix.Project.build_path(), "logs"])
    File.mkdir_p!(logdir)

    System.fetch_env!("PROFILE")
    |> String.replace_suffix("-test", "")
    |> then(&System.put_env("PROFILE", &1))

    CT.maybe_start_cover()
    if CT.cover_enabled?(), do: CT.cover_compile_files()

    EMQX.Mix.Utils.clear_screen()

    Logger.configure(level: :notice)
    :ok = enable_sasl_report_logging()
    :ok = CT.replace_elixir_formatter()

    {:ok, _} = Application.ensure_all_started(:cth_readable)

    context = %{logdir: logdir, node_name: node_name}
    results = run_suites(opts, context)

    symlink_to_last_run(logdir)

    case CT.sum_results(results) do
      {_success, failed, {_user_skipped, auto_skipped}} when failed > 0 or auto_skipped > 0 ->
        Mix.raise("failures running tests: #{failed} failed, #{auto_skipped} auto skipped")

      {success, _failed = 0, {user_skipped, _auto_skipped = 0}} ->
        CT.info(
          "Test suites ran successfully.  #{success} succeeded, #{user_skipped} skipped by the user"
        )

        if CT.cover_enabled?(), do: CT.write_coverdata(opts)
    end
  end

  defp run_suites(opts, context) do
    %{
      logdir: logdir,
      node_name: node_name
    } = context

    suite_mods = Enum.map(opts.suites, &suite_module/1)

    dirs =
      suite_mods
      |> Enum.map(&suite_ebin_dir/1)
      |> Enum.uniq()
      |> Enum.map(&to_charlist/1)

    :ct.run_test(
      abort_if_missing_suites: true,
      auto_compile: false,
      dir: dirs,
      suite: suite_mods,
      group:
        opts.group_paths
        |> Enum.map(fn gp -> Enum.map(gp, &String.to_atom/1) end),
      testcase: opts.cases |> Enum.map(&to_charlist/1),
      readable: ~c"true",
      name: node_name,
      ct_hooks: [:cth_readable_shell, :cth_readable_failonly],
      logdir: to_charlist(logdir),
      repeat: opts.repeat
    )
  end

  defp parse_args!(args) do
    {opts, rest} =
      OptionParser.parse!(
        args,
        strict: [
          cover_export_name: :string,
          suites: :string,
          group_paths: :string,
          cases: :string,
          repeat: :integer
        ]
      )

    if rest != [] do
      Mix.raise("Unknown options:\n  #{inspect(rest, pretty: true)}")
    end

    suites = get_name_list(opts, :suites)

    if suites == [] do
      Mix.raise(
        "must define at least one suite using --suites plugins/app/test/suite1.erl,plugins/app/test/suite2.erl"
      )
    end

    %{
      cover_export_name: Keyword.get(opts, :cover_export_name, "ct"),
      suites: suites,
      group_paths:
        opts
        |> get_name_list(:group_paths)
        |> Enum.map(&String.split(&1, ".", trim: true)),
      cases: get_name_list(opts, :cases),
      repeat: Keyword.get(opts, :repeat, 1)
    }
  end

  defp get_name_list(opts, key) do
    opts
    |> Keyword.get(key, "")
    |> String.split(",", trim: true)
  end

  defp validate_plugin_suites!(suites) do
    non_plugin = Enum.reject(suites, &String.starts_with?(&1, "plugins/"))

    if non_plugin != [] do
      Mix.raise("emqx.ct_plugins only accepts suites under plugins/: #{Enum.join(non_plugin, ", ")}")
    end
  end

  defp suite_module(suite_path) do
    suite_path
    |> Path.basename(".erl")
    |> String.to_atom()
  end

  defp suite_ebin_dir(suite_mod) do
    case :code.which(suite_mod) do
      full_path when is_list(full_path) ->
        full_path
        |> List.to_string()
        |> Path.dirname()

      _ ->
        Mix.raise("suite #{suite_mod} is not compiled")
    end
  end

  defp symlink_to_last_run(logdir) do
    last_dir =
      logdir
      |> File.ls!()
      |> Stream.filter(&(&1 =~ ~r/^ct_run/))
      |> Enum.sort()
      |> List.last()

    if last_dir do
      dest = Path.join(logdir, "last")
      File.rm(dest)
      File.ln_s!(last_dir, dest)
    end
  end

  defp load_common_helpers!() do
    Code.ensure_all_loaded!([
      :emqx_common_test_helpers,
      :emqx_bridge_v2_testlib,
      :emqx_utils_http_test_server
    ])
  end

  defp hack_test_data_dirs!(suites) do
    project_root = Path.dirname(Mix.Project.project_file())

    suites
    |> Stream.map(fn suite_path ->
      src_data_dir =
        project_root
        |> Path.join(Path.rootname(suite_path) <> "_data")

      if File.dir?(src_data_dir) do
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
          :ok -> :ok
          {:error, :eexist} -> :ok
        end
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

  defp enable_sasl_report_logging do
    filters =
      :logger.get_primary_config()
      |> Map.fetch!(:filters)
      |> Enum.map(fn
        {name = :logger_translator, {mod, config}} ->
          {name, {mod, %{config | sasl: true, translators: []}}}

        filter ->
          filter
      end)

    :logger.set_primary_config(:filters, filters)
  end
end
