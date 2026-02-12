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

    opts = CT.parse_args!(args, allow_spec: false)
    validate_plugin_suites!(opts.suites)

    Enum.each([:common_test, :eunit, :mnesia], &CT.add_to_path_and_cache/1)

    CT.ensure_whole_emqx_project_is_loaded()
    CT.unload_emqx_applications!()
    CT.load_common_helpers!()
    CT.hack_test_data_dirs!(opts.suites)

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
    :ok = CT.enable_sasl_report_logging()
    :ok = CT.replace_elixir_formatter()

    {:ok, _} = Application.ensure_all_started(:cth_readable)

    context = %{logdir: logdir, node_name: node_name}
    results = run_suites(opts, context)

    CT.symlink_to_last_run(logdir)

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

  defp validate_plugin_suites!(suites) do
    non_plugin = Enum.reject(suites, &String.starts_with?(&1, "plugins/"))

    if non_plugin != [] do
      Mix.raise(
        "emqx.ct_plugins only accepts suites under plugins/: #{Enum.join(non_plugin, ", ")}"
      )
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
end
