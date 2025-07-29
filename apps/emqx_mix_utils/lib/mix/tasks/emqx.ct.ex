defmodule Mix.Tasks.Emqx.Ct do
  use Mix.Task

  # todo: invoke the equivalent of `make merge-config` as a requirement...
  @requirements ["compile", "loadpaths"]

  @moduledoc """
  Runs CT suites.

  ## Options

    * `--suites` - specify the comma-separated list of suites to run.

    * `--cases` - specify the comma-separated list of test cases to run in a suite.

    * `--group-paths` - group paths to run the tests.  Group sequences are dot-separated,
      and different group paths are comma-separated.
      Ex: `--group-paths a.b.c,x.y.z` will run the group path `[a, b, c]`, then `[x, y, z]`.

    * `--spec` - specify the path to a CT test spec file.  This option cannot be combined
      with the above.

  ## Examples

      $ mix ct --suites apps/emqx/test/emqx_SUITE.erl,apps/emqx/test/emqx_alarm_SUITE.erl

      $ mix ct --suites apps/emqx_bridge_s3tables/test/emqx_bridge_s3tables_SUITE.erl \\
               --group-paths batched.not_partitioned.avro,not_batched.not_partitioned.parquet \\
               --cases t_rule_action

      $ mix ct --spec apps/emqx_durable_storage/test/otx.spec
  """

  @impl true
  def run(args) do
    opts = parse_args!(args)

    Enum.each([:common_test, :eunit, :mnesia], &add_to_path_and_cache/1)

    # app_to_debug = :emqx_conf
    # Mix.Dep.Umbrella.cached()
    # |> Enum.find(& &1.app == app_to_debug)
    # |> Mix.Dep.in_dependency(fn _dep_mix_project_mod ->
    #   config = Mix.Project.config()
    #   |> IO.inspect(label: app_to_debug)
    # end)

    ensure_whole_emqx_project_is_loaded()
    unload_emqx_applications!()
    load_common_helpers!()
    hack_test_data_dirs!(opts.suites)

    # ensure_suites_are_loaded(opts.suites)

    {_, 0} = System.cmd("epmd", ["-daemon"])
    node_name = :"test@127.0.0.1"
    Node.start(node_name, :longnames)
    logdir = Path.join([Mix.Project.build_path(), "logs"])
    File.mkdir_p!(logdir)

    # unmangle PROFILE env because some places (`:emqx_conf.resolve_schema_module`) expect
    # the version without the `-test` suffix.
    System.fetch_env!("PROFILE")
    |> String.replace_suffix("-test", "")
    |> then(&System.put_env("PROFILE", &1))

    # {_, _, _} = ["test_server:do_init_tc_call -> return"] |> Enum.map(&to_charlist/1) |> :redbug.start()

    maybe_start_cover()
    if cover_enabled?(), do: cover_compile_files()

    EMQX.Mix.Utils.clear_screen()

    Logger.configure(level: :notice)
    :ok = enable_sasl_report_logging()
    :ok = replace_elixir_formatter()

    {:ok, _} = Application.ensure_all_started(:cth_readable)

    context = %{logdir: logdir, node_name: node_name}

    results =
      if !opts[:spec] do
        run_suites(opts, context)
      else
        run_test_spec(opts, context)
      end

    symlink_to_last_run(logdir)

    case results do
      {_success, failed, {_user_skipped, auto_skipped}} when failed > 0 or auto_skipped > 0 ->
        Mix.raise("failures running tests: #{failed} failed, #{auto_skipped} auto skipped")

      {success, _failed = 0, {user_skipped, _auto_skipped = 0}} ->
        info(
          "Test suites ran successfully.  #{success} succeeded, #{user_skipped} skipped by the user"
        )

        if cover_enabled?(), do: write_coverdata(opts)
    end
  end

  def run_suites(opts, context) do
    %{
      logdir: logdir,
      node_name: node_name
    } = context

    :ct.run_test(
      abort_if_missing_suites: true,
      auto_compile: false,
      suite: opts |> Map.fetch!(:suites) |> Enum.map(&to_charlist/1),
      group:
        opts
        |> Map.fetch!(:group_paths)
        |> Enum.map(fn gp -> Enum.map(gp, &String.to_atom/1) end),
      testcase: opts |> Map.fetch!(:cases) |> Enum.map(&to_charlist/1),
      readable: ~c"true",
      name: node_name,
      ct_hooks: [:cth_readable_shell, :cth_readable_failonly],
      logdir: to_charlist(logdir)
    )
  end

  def run_test_spec(opts, context) do
    %{
      logdir: logdir,
      node_name: node_name
    } = context

    spec = opts[:spec]

    :ct.run_test(
      abort_if_missing_suites: true,
      auto_compile: false,
      spec: to_charlist(spec),
      readable: ~c"true",
      name: node_name,
      ct_hooks: [:cth_readable_shell, :cth_readable_failonly],
      logdir: to_charlist(logdir)
    )
  end

  def replace_elixir_formatter() do
    erl_formatter = {:logger_formatter, %{single_line: false, legacy_header: true}}
    :logger.update_handler_config(:default, %{formatter: erl_formatter})
  end

  @doc """
  Here we ensure the _modules_ are loaded, not the applications.

  Specially useful for suites that use utilities such as `emqx_common_test_helpers` and
  similar.

  This needs to run before we unload the applications, otherwise we lose their `:modules`
  attribute.
  """
  def ensure_whole_emqx_project_is_loaded() do
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

        _other ->
          # IO.inspect({app, other}, label: :bad_mods)
          []
      end
    end)
    |> Code.ensure_all_loaded()
  end

  @doc """
  We do this because some callbacks rely on the side-effect of the application being
  loaded.  We leave starting them to `:emqx_cth_suite:start`.

  For example, `:emqx_dashboard:apps/0` uses that to decide which specs to load.
  """
  def unload_emqx_applications!() do
    for {app, _, _} <- Application.loaded_applications(),
        to_string(app) =~ ~r/^emqx/ do
      case Application.unload(app) do
        :ok -> :ok
        {:error, {:not_loaded, _}} -> :ok
      end
    end
  end

  defp load_common_helpers!() do
    Code.ensure_all_loaded!([
      :emqx_common_test_helpers,
      :emqx_bridge_testlib,
      :emqx_bridge_v2_testlib,
      :emqx_utils_http_test_server
    ])
  end

  # Links `test/*_data` directories inside the build dir, so that CT picks them up.
  defp hack_test_data_dirs!(suites) do
    project_root = Path.dirname(Mix.Project.project_file())

    suites
    |> Stream.map(fn suite_path ->
      src_data_dir =
        project_root
        |> Path.join(Path.rootname(suite_path) <> "_data")

      data_dir_exists? = File.dir?(src_data_dir)

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

  # defp ensure_suites_are_loaded(suites) do
  #   suites
  #   |> Enum.map(fn suite_path ->
  #     ["apps", app_name | _] = Path.split(suite_path)
  #     String.to_atom(app_name)
  #   end)
  #   |> Enum.uniq()
  #   |> Enum.flat_map(fn app ->
  #     {:ok, mods} = :application.get_key(app, :modules)
  #     mods
  #   end)
  #   |> Code.ensure_all_loaded()
  # end

  def add_to_path_and_cache(lib_name) do
    :code.lib_dir()
    |> Path.join("#{lib_name}-*")
    |> Path.wildcard()
    |> hd()
    |> Path.join("ebin")
    |> to_charlist()
    |> :code.add_path(:cache)
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

  defp parse_args!(args) do
    {opts, _rest} =
      OptionParser.parse!(
        args,
        strict: [
          cover_export_name: :string,
          suites: :string,
          group_paths: :string,
          cases: :string,
          spec: :string
        ]
      )

    suites = get_name_list(opts, :suites)

    group_paths =
      opts
      |> get_name_list(:group_paths)
      |> Enum.map(&String.split(&1, ".", trim: true))

    cases = get_name_list(opts, :cases)

    spec = opts[:spec]

    if Enum.any?(suites ++ group_paths ++ cases) && !!spec do
      Mix.raise(
        "must define either `--spec` or `--suites`/`--group-paths`/`--cases`, but not both"
      )
    end

    if !spec && suites == [] do
      Mix.raise(
        "must define at least one suite using --suites path/to/suite1.erl,path/to/suite2.erl"
      )
    end

    %{
      cover_export_name: Keyword.get(opts, :cover_export_name, "ct"),
      suites: suites,
      group_paths: group_paths,
      cases: cases,
      spec: spec
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

  def maybe_start_cover() do
    if cover_enabled?() do
      {:ok, _} = start_cover()
    end
  end

  def start_cover() do
    case :cover.start() do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
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

  def cover_compile_files() do
    info("Cover compiling project modules...")
    real_modules = real_modules()

    Mix.Dep.Umbrella.loaded()
    |> Stream.flat_map(fn umbrella_app ->
      beams_dir = Path.join(umbrella_app.opts[:build], "ebin")

      beams_dir
      |> Path.join("*.beam")
      |> Path.wildcard()
    end)
    |> Stream.filter(fn beam_path ->
      mod_bin = Path.basename(beam_path, ".beam")
      mod_bin in real_modules
    end)
    |> Stream.map(fn beam_path ->
      mod_bin = Path.basename(beam_path, ".beam")
      mod = String.to_atom(mod_bin)
      {mod, beam_path}
    end)
    |> Enum.map(&do_cover_compile_file_async/1)
    |> Task.await_many(:infinity)

    info("Cover compiled project modules.")
  end

  defp do_cover_compile_file_async({mod, beam_path}) do
    Task.async(fn ->
      try do
        case :cover.is_compiled(mod) do
          {:file, _} ->
            debug("Module already cover-compiled: #{mod}")
            :ok

          false ->
            debug("cover-compiling: #{beam_path}")

            beam_path
            |> to_charlist()
            |> :cover.compile_beam()
            |> case do
              {:ok, _} ->
                :ok

              {:error, reason} ->
                warn("Cover compilation failed: #{inspect(reason, pretty: true)}")
            end
        end
      catch
        kind, reason ->
          warn("Cover compilation failed: #{inspect({kind, reason}, pretty: true)}")
      end
    end)
  end

  # Set of "real" module names in the project as strings, i.e., excluding test modules
  defp real_modules() do
    Mix.Dep.Umbrella.loaded()
    |> Stream.flat_map(fn dep ->
      dep.opts[:path]
      |> Path.join("{src,gen_src}/**/*.erl")
      |> Path.wildcard()
    end)
    |> MapSet.new(&Path.basename(&1, ".erl"))
  end

  def write_coverdata(opts) do
    with [_ | _] = _modules <- :cover.modules() do
      cover_dir = Path.join([Mix.Project.build_path(), "cover"])
      File.mkdir_p!(cover_dir)
      cover_export_name = opts.cover_export_name

      export_file = Path.join(cover_dir, "#{cover_export_name}.coverdata")

      case :cover.export(export_file) do
        :ok ->
          info("Cover data written to #{export_file}")
          :ok = :cover.reset()
          :ok

        {:error, reason} ->
          warn("Cover data export failed:\n  #{inspect(reason, pretty: true)}")
      end
    else
      [] ->
        warn("No cover-compiled modules found")
        :ok
    end
  end

  def debug(iodata) do
    if Mix.debug?() do
      Mix.shell().info(IO.ANSI.format([:cyan, iodata]))
    end
  end

  def info(iodata) do
    Mix.shell().info(IO.ANSI.format(iodata))
  end

  def warn(iodata) do
    Mix.shell().info(IO.ANSI.format([:yellow, iodata]))
  end
end
