defmodule Mix.Tasks.Emqx.Plugin do
  use Mix.Task

  @shortdoc "Build EMQX plugin package (.tar.gz)"

  @moduledoc """
  Build an EMQX plugin package using Mix.

  Example:

      mix emqx.plugin
  """

  @requirements ["loadpaths", "compile"]

  @metadata_vsn "0.2.0"

  @impl true
  def run(_args) do
    plugin_dir = "."
    plugin_name = Mix.Project.config()[:app] |> to_string()
    plugin_vsn = Mix.Project.config()[:version]

    build_profile =
      System.get_env("PROFILE")
      |> case do
        nil -> "emqx-enterprise"
        v -> String.replace_suffix(v, "-test", "")
      end

    System.put_env("PROFILE", build_profile)

    info = collect_info(plugin_dir, plugin_name, plugin_vsn)
    make_tar(plugin_dir, info)

    Mix.shell().info("Built plugin package: #{info.name}-#{info.rel_vsn}.tar.gz")
  end

  defp collect_info(plugin_dir, plugin_name, plugin_vsn) do
    emqx_plugin = plugin_package_config!()
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

  defp resolve_app_vsn!(app) do
    app_name = Atom.to_string(app)
    props = compiled_app_props!(app_name)
    {:vsn, vsn} = List.keyfind(props, :vsn, 0)
    to_bin([app_name, "-", to_string(vsn)])
  end

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

  defp plugin_build_lib_dir() do
    profile = System.get_env("PROFILE", "emqx-enterprise")

    build_path =
      Mix.Project.config()
      |> Keyword.fetch!(:build_path)
      |> Path.expand()

    Path.join([build_path, profile, "lib"])
  end

  defp plugin_pkg_out_dir() do
    build_path =
      Mix.Project.config()
      |> Keyword.fetch!(:build_path)
      |> Path.expand()

    Path.join([build_path, "plugins"])
  end

  defp app_name_from_vsn(name_vsn) do
    name_vsn
    |> String.split("-")
    |> List.first()
  end

  defp build_date() do
    case cmd_output(["log", "-1", "--pretty=format:%cd", "--date=format:%Y-%m-%d"]) do
      nil -> Date.utc_today() |> Date.to_iso8601() |> to_bin()
      date -> to_bin(date)
    end
  end

  defp git_ref() do
    case cmd_output(["rev-parse", "HEAD"]) do
      nil -> "unknown"
      ref -> to_bin(ref)
    end
  end

  defp cmd_output(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> nil
    end
  end

  defp plugin_package_config!() do
    case Mix.Project.config()[:emqx_plugin] do
      nil ->
        Mix.raise("Missing :emqx_plugin config in the plugins' `mix.exs` project()")

      cfg when is_list(cfg) ->
        cfg

      other ->
        Mix.raise("Invalid :emqx_plugin config: expected keyword list, got #{inspect(other)}")
    end
  end

  defp info_map(info_list) do
    info_list
    |> Enum.map(fn {k, v} -> {k, info_field(k, v)} end)
    |> Map.new()
  end

  defp info_field(:compatibility, values), do: info_map(values)
  defp info_field(:builder, values), do: info_map(values)
  defp info_field(:authors, values), do: to_bin(values)
  defp info_field(:functionality, values), do: to_bin(values)
  defp info_field(:hidden, value) when is_boolean(value), do: value
  defp info_field(_key, value), do: to_bin(value)

  defp to_bin(v) when is_binary(v), do: v
  defp to_bin(v) when is_atom(v), do: Atom.to_string(v)
  defp to_bin(v) when is_list(v), do: IO.iodata_to_binary(v)
  defp to_bin(v), do: to_string(v)

  defp json_encode!(input), do: :json.encode(input) |> IO.iodata_to_binary()
  defp json_decode!(input), do: :json.decode(input)

  defp hocon_load!(path), do: apply(:hocon, :load, [path])

  defp avro_schema_store_new!(opts),
    do: apply(:avro_schema_store, :new, [opts])

  defp avro_schema_store_import_schema_json!(name, avsc_bin, store),
    do: apply(:avro_schema_store, :import_schema_json, [name, avsc_bin, store])

  defp avro_make_decoder_options!(opts), do: apply(:avro, :make_decoder_options, [opts])

  defp avro_json_decode_value!(json, name, store, opts),
    do: apply(:avro_json_decoder, :decode_value, [json, name, store, opts])
end
