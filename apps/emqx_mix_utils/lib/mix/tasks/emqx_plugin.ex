defmodule Mix.Tasks.Emqx.Plugin do
  use Mix.Task

  @shortdoc "Build EMQX plugin package (.tar.gz)"

  @moduledoc """
  Build an EMQX plugin package using Mix.

  Example:

      mix emqx.plugin --app apps/emqx_username_quota
  """

  @requirements ["loadpaths"]

  @metadata_vsn "0.2.0"

  @impl true
  # Entrypoint: validate plugin app path, read release metadata, and package tarball.
  def run(args) do
    opts = parse_args!(args)
    plugin_dir = Path.expand(opts[:app])

    validate_plugin_dir!(plugin_dir)

    plugin_name = Path.basename(plugin_dir)

    build_profile =
      System.get_env("PROFILE")
      |> case do
        nil -> "emqx-enterprise"
        v -> String.replace_suffix(v, "-test", "")
      end

    System.put_env("PROFILE", build_profile)

    plugin_vsn = plugin_mix_version!(plugin_dir, plugin_name)
    ensure_prebuilt_plugin_exists!(plugin_name)

    info = collect_info(plugin_dir, plugin_name, plugin_vsn)
    make_tar(plugin_dir, info)

    Mix.shell().info("Built plugin package: #{info.name}-#{info.rel_vsn}.tar.gz")
  end

  # Parse and validate CLI arguments accepted by `mix emqx.plugin`.
  defp parse_args!(args) do
    {opts, rest} =
      OptionParser.parse!(args,
        strict: [app: :string]
      )

    if rest != [] do
      Mix.raise("Unknown arguments: #{Enum.join(rest, " ")}")
    end

    app = Keyword.get(opts, :app)

    if is_nil(app) or app == "" do
      Mix.raise("Missing required --app argument, e.g. --app apps/emqx_username_quota")
    end

    %{app: app}
  end

  # Ensure the target plugin path exists and has expected source layout.
  defp validate_plugin_dir!(plugin_dir) do
    cond do
      not File.dir?(plugin_dir) ->
        Mix.raise("Plugin dir not found: #{plugin_dir}")

      not File.exists?(Path.join(plugin_dir, "src")) ->
        Mix.raise("Plugin src/ not found: #{plugin_dir}")

      true ->
        :ok
    end
  end

  # Packaging is build-output-only; this asserts plugin ebin has already been compiled.
  defp ensure_prebuilt_plugin_exists!(plugin_name) do
    app_file = Path.join([plugin_build_lib_dir(), plugin_name, "ebin", "#{plugin_name}.app"])

    if not File.regular?(app_file) do
      Mix.raise("""
      Missing prebuilt plugin artifact: #{app_file}
      This task only packages existing build outputs from _build/$PROFILE/lib.
      Build first, e.g.:
        PROFILE=#{System.get_env("PROFILE", "emqx-enterprise")} mix compile
      """)
    end
  end

  # Build release.json payload using plugin config plus build/runtime metadata.
  defp collect_info(plugin_dir, plugin_name, plugin_vsn) do
    emqx_plugin = plugin_package_config!(plugin_dir, plugin_name)
    release_name = emqx_plugin[:name] || plugin_name
    release_vsn = emqx_plugin[:rel_vsn] || plugin_vsn
    release_apps = emqx_plugin[:rel_apps] || [String.to_atom(plugin_name)]
    metadata = emqx_plugin[:metadata] || []

    rel_apps =
      release_apps
      |> expand_release_apps!()
      |> Enum.map(&resolve_app_vsn!/1)

    plugrel_map = info_map(metadata)

    merged =
      Map.merge(plugrel_map, %{
        name: to_bin(release_name),
        rel_vsn: to_bin(release_vsn),
        rel_apps: rel_apps,
        git_ref: git_ref(),
        git_commit_or_build_date: build_date(),
        metadata_vsn: @metadata_vsn,
        built_on_otp_release: to_bin(:erlang.system_info(:otp_release)),
        with_config_schema: File.regular?(Path.join(plugin_dir, "priv/config_schema.avsc")),
        hidden: Map.get(plugrel_map, :hidden, false)
      })

    validate_optional_files!(plugin_dir)

    %{
      plugin_dir: plugin_dir,
      plugin_name: plugin_name,
      name: merged[:name],
      rel_vsn: merged[:rel_vsn],
      rel_apps: merged[:rel_apps],
      release_json: merged
    }
  end

  # Validate optional config artifacts when they are present in plugin priv/.
  defp validate_optional_files!(plugin_dir) do
    avsc = Path.join(plugin_dir, "priv/config_schema.avsc")
    hocon = Path.join(plugin_dir, "priv/config.hocon")
    i18n = Path.join(plugin_dir, "priv/config_i18n.json")

    if File.regular?(avsc) do
      validate_avsc!(avsc, hocon)
    end

    if File.regular?(i18n) do
      _ = json_decode!(File.read!(i18n))
    end
  end

  # Validate AVSC schema against config.hocon by decoding HOCON through avro_json_decoder.
  defp validate_avsc!(avsc_file, hocon_file) do
    name = "TryDecodeDefaultAvro"
    {:ok, hocon_map} = hocon_load!(String.to_charlist(hocon_file))

    json = json_encode!(hocon_map)

    store0 = avro_schema_store_new!([:map])
    {:ok, avsc_bin} = :file.read_file(String.to_charlist(avsc_file))
    store = avro_schema_store_import_schema_json!(name, avsc_bin, store0)

    opts =
      avro_make_decoder_options!(
        map_type: :map,
        record_type: :map,
        encoding: :avro_json
      )

    _ = avro_json_decode_value!(json, name, store, opts)
    :ok
  end

  # Resolve an app entry in rel_apps to "<app>-<vsn>" from compiled .app file.
  defp resolve_app_vsn!(app) do
    app_name = Atom.to_string(app)
    props = compiled_app_props!(app_name)
    {:vsn, vsn} = List.keyfind(props, :vsn, 0)
    to_bin([app_name, "-", to_string(vsn)])
  end

  # Expand rel_apps with transitive packageable dependencies from app metadata.
  defp expand_release_apps!(release_apps) do
    do_expand_release_apps!(release_apps, MapSet.new(), [])
  end

  defp do_expand_release_apps!([], _seen, acc), do: Enum.reverse(acc)

  defp do_expand_release_apps!([app | rest], seen, acc) do
    if MapSet.member?(seen, app) do
      do_expand_release_apps!(rest, seen, acc)
    else
      app_name = Atom.to_string(app)
      deps = packageable_app_deps(app_name)
      do_expand_release_apps!(rest ++ deps, MapSet.put(seen, app), [app | acc])
    end
  end

  defp packageable_app_deps(app_name) do
    props = compiled_app_props!(app_name)

    deps =
      Keyword.get(props, :applications, []) ++
        Keyword.get(props, :included_applications, [])

    Enum.filter(deps, &packageable_app?/1)
  end

  defp packageable_app?(app) do
    app_name = Atom.to_string(app)
    File.dir?(Path.join([plugin_build_lib_dir(), app_name, "ebin"]))
  end

  # Load application properties from compiled .app file under _build/$PROFILE/lib.
  defp compiled_app_props!(app_name) do
    app_file = Path.join([plugin_build_lib_dir(), app_name, "ebin", "#{app_name}.app"])

    case :file.consult(String.to_charlist(app_file)) do
      {:ok, [{:application, _name, props}]} ->
        props

      {:error, reason} ->
        Mix.raise("Failed to read app file #{app_file}: #{inspect(reason)}")

      other ->
        Mix.raise("Invalid app file format #{app_file}: #{inspect(other)}")
    end
  end

  # Assemble package directory content, generate tar.gz and sha256 checksum in _build/plugins.
  defp make_tar(plugin_dir, info) do
    name_vsn = "#{info.name}-#{info.rel_vsn}"
    out_dir = plugin_pkg_out_dir()
    package_dir = Path.join(out_dir, name_vsn)

    File.rm_rf!(package_dir)
    File.mkdir_p!(package_dir)

    readme = Path.join(plugin_dir, "README.md")

    if File.regular?(readme) do
      File.cp!(readme, Path.join(package_dir, "README.md"))
    end

    release_json = json_encode!(info.release_json)

    File.write!(Path.join(package_dir, "release.json"), release_json)

    Enum.each(info.rel_apps, fn app_vsn_bin ->
      app_vsn = to_string(app_vsn_bin)
      app_name = app_name_from_vsn(app_vsn)
      dst = Path.join(package_dir, app_vsn)
      copy_payload!(plugin_dir, info.plugin_name, app_name, dst)
    end)

    tar_file = Path.join(out_dir, "#{name_vsn}.tar.gz")
    sha_file = Path.join(out_dir, "#{name_vsn}.sha256")

    create_tar!(out_dir, name_vsn, tar_file)

    sha =
      tar_file |> File.read!() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)

    File.write!(sha_file, sha)
  end

  # Copy packaged payload for one release app using compiled ebin and include/priv resources.
  defp copy_payload!(plugin_dir, plugin_name, app_name, dst) do
    File.rm_rf!(dst)
    File.mkdir_p!(dst)

    src = Path.join(plugin_build_lib_dir(), app_name)
    ebin = Path.join(src, "ebin")

    if not File.dir?(ebin) do
      Mix.raise("Missing compiled ebin dir for app #{app_name}: #{ebin}")
    end

    File.cp_r!(ebin, Path.join(dst, "ebin"))

    include_src =
      if app_name == plugin_name do
        Path.join(plugin_dir, "include")
      else
        Path.join(src, "include")
      end

    priv_src =
      if app_name == plugin_name do
        Path.join(plugin_dir, "priv")
      else
        Path.join(src, "priv")
      end

    if File.dir?(include_src), do: File.cp_r!(include_src, Path.join(dst, "include"))
    if File.dir?(priv_src), do: File.cp_r!(priv_src, Path.join(dst, "priv"))
  end

  # Create compressed tarball from the staged package directory entries.
  defp create_tar!(cwd, root_name, tar_file) do
    File.rm_rf!(tar_file)

    entries =
      cwd
      |> Path.join("#{root_name}/*")
      |> Path.wildcard()
      |> Enum.sort()
      |> Enum.map(fn full_path ->
        {String.to_charlist(Path.relative_to(full_path, cwd)), String.to_charlist(full_path)}
      end)

    case :erl_tar.create(String.to_charlist(tar_file), entries, [:compressed]) do
      :ok -> :ok
      {:error, reason} -> Mix.raise("Failed to create tarball: #{inspect(reason)}")
    end
  end

  # Root build output path where precompiled app artifacts are expected.
  defp plugin_build_lib_dir() do
    profile = System.get_env("PROFILE", "emqx-enterprise")
    Path.join([project_root(), "_build", profile, "lib"])
  end

  # Root output path for plugin release artifacts.
  defp plugin_pkg_out_dir() do
    Path.join([project_root(), "_build", "plugins"])
  end

  # Read plugin release version from plugin mix project config.
  defp plugin_mix_version!(plugin_dir, plugin_name) do
    app = String.to_atom(plugin_name)

    config =
      Mix.Project.in_project(app, plugin_dir, fn _module ->
        Mix.Project.config()
      end)

    case config[:version] do
      version when is_binary(version) and version != "" ->
        version

      other ->
        Mix.raise("Invalid plugin mix.exs version: #{inspect(other)}")
    end
  end

  # Extract app name from "<app>-<vsn>" release app entry.
  defp app_name_from_vsn(name_vsn) do
    name_vsn
    |> String.split("-")
    |> List.first()
  end

  # Build date used in release metadata, preferring latest commit date.
  defp build_date() do
    case cmd_output(["log", "-1", "--pretty=format:%cd", "--date=format:%Y-%m-%d"]) do
      nil -> Date.utc_today() |> Date.to_iso8601() |> to_bin()
      date -> to_bin(date)
    end
  end

  # Git commit hash used in release metadata.
  defp git_ref() do
    case cmd_output(["rev-parse", "HEAD"]) do
      nil -> "unknown"
      ref -> to_bin(ref)
    end
  end

  # Run a git command and return trimmed output on success.
  defp cmd_output(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> nil
    end
  end

  # Load plugin package configuration from plugin mix project.
  defp plugin_package_config!(plugin_dir, plugin_name) do
    app = String.to_atom(plugin_name)

    config =
      Mix.Project.in_project(app, plugin_dir, fn _module ->
        Mix.Project.config()
      end)

    case config[:emqx_plugin] do
      nil ->
        Mix.raise("Missing :emqx_plugin config in #{Path.join(plugin_dir, "mix.exs")} project()")

      cfg when is_list(cfg) ->
        cfg

      other ->
        Mix.raise("Invalid :emqx_plugin config: expected keyword list, got #{inspect(other)}")
    end
  end

  # Convert plugin metadata keyword list into a map with normalized field encodings.
  defp info_map(info_list) do
    info_list
    |> Enum.map(fn {k, v} -> {k, info_field(k, v)} end)
    |> Map.new()
  end

  # Field-level metadata encoding rules aligned with release.json conventions.
  defp info_field(:compatibility, values), do: info_map(values)
  defp info_field(:builder, values), do: info_map(values)
  defp info_field(:authors, values), do: to_bin(values)
  defp info_field(:functionality, values), do: to_bin(values)
  defp info_field(:hidden, value) when is_boolean(value), do: value
  defp info_field(_key, value), do: to_bin(value)

  # Convert common Erlang/Elixir value types to binary for JSON serialization.
  defp to_bin(v) when is_binary(v), do: v
  defp to_bin(v) when is_atom(v), do: Atom.to_string(v)
  defp to_bin(v) when is_list(v), do: IO.iodata_to_binary(v)
  defp to_bin(v), do: to_string(v)

  # Use Erlang/OTP native JSON encoder for release payload generation.
  defp json_encode!(input), do: :json.encode(input) |> IO.iodata_to_binary()

  # Use Erlang/OTP native JSON decoder for JSON schema/i18n validation.
  defp json_decode!(input), do: :json.decode(input)

  # These Erlang modules may be absent from emqx_mix_utils compile path.
  # Use apply/3 to avoid undefined-module warnings under --warnings-as-errors.
  defp hocon_load!(path), do: apply(:hocon, :load, [path])

  defp avro_schema_store_new!(opts),
    do: apply(:avro_schema_store, :new, [opts])

  defp avro_schema_store_import_schema_json!(name, avsc_bin, store),
    do: apply(:avro_schema_store, :import_schema_json, [name, avsc_bin, store])

  defp avro_make_decoder_options!(opts), do: apply(:avro, :make_decoder_options, [opts])

  defp avro_json_decode_value!(json, name, store, opts),
    do: apply(:avro_json_decoder, :decode_value, [json, name, store, opts])

  # Resolve monorepo root from the current Mix project file.
  defp project_root() do
    Mix.Project.project_file() |> Path.dirname()
  end
end
