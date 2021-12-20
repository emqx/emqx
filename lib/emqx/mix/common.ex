defmodule EMQX.Mix.Common do
  @kernel_apps [:kernel, :stdlib, :sasl, :elixir]

  def project(app, overrides \\ []) when is_atom(app) and app != nil do
    %{
      vsn: version,
      description: description
    } =
      app
      |> erl_app_props!()
      |> Map.take([:vsn, :description])
      |> Map.new(fn {k, v} -> {k, to_string(v)} end)

    Keyword.merge(
      [
        app: app,
        version: version,
        description: description,
        build_path: "../../_build",
        config_path: "../../config/config.exs",
        deps_path: "../../deps",
        lockfile: "../../mix.lock",
        elixir: "~> 1.13"
      ],
      overrides
    )
  end

  def application(app, overrides \\ []) when app != nil do
    {deps, overrides} = Keyword.pop(overrides, :deps, [])
    # get only the dependency names
    deps = Enum.map(deps, &elem(&1, 0))

    app
    |> erl_app_props!()
    |> Map.take([:registered, :mod, :applications])
    |> Map.update!(:applications,
    fn apps ->
      deps ++ apps -- @kernel_apps
    end)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Keyword.new()
    |> Keyword.merge(overrides)
  end

  def erl_apps(app) when app != nil do
    from_erl!(app, :applications)
  end

  def erl_app_props!(app) do
    path = Path.join("src", "#{app}.app.src")
    {:ok, [{:application, ^app, props}]} = :file.consult(path)
    Map.new(props)
  end

  def from_erl!(app, key) when app != nil do
    app
    |> erl_app_props!()
    |> Map.fetch!(key)
  end

  def from_rebar_deps!() do
    path = "rebar.config"
    {:ok, props} = :file.consult(path)

    props
    |> Keyword.fetch!(:deps)
    |> Enum.map(&rebar_to_mix_dep/1)
  end

  def rebar_to_mix_dep({name, {:git, url, {:tag, tag}}}),
    do: {name, git: to_string(url), tag: to_string(tag)}

  def rebar_to_mix_dep({name, {:git, url, {:ref, ref}}}),
    do: {name, git: to_string(url), ref: to_string(ref)}

  def rebar_to_mix_dep({name, {:git, url, {:branch, branch}}}),
    do: {name, git: to_string(url), branch: to_string(branch)}

  def rebar_to_mix_dep({name, vsn}) when is_list(vsn),
    do: {name, to_string(vsn)}

  def compile_protos(mix_filepath) do
    app_path = Path.dirname(mix_filepath)

    config = [
      :use_packages,
      :maps,
      :strings_as_binaries,
      rename: {:msg_name, :snake_case},
      rename: {:msg_fqname, :base_name},
      i: '.',
      report_errors: false,
      o: app_path |> Path.join("src") |> to_charlist(),
      module_name_prefix: 'emqx_',
      module_name_suffix: '_pb'
    ]

    app_path
    |> Path.join("priv/protos/*.proto")
    |> Path.wildcard()
    |> Enum.map(&to_charlist/1)
    |> Enum.each(&:gpb_compile.file(&1, config))

    :ok
  end
end
