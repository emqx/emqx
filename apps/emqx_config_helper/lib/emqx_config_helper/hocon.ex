defmodule EmqxConfigHelper.Hocon do
  def hocon(_application, options) do
    {:ok, config} =
      options
      |> Keyword.fetch!(:config_file)
      |> List.wrap()
      |> Enum.map(&to_charlist/1)
      |> :hocon.files(%{format: :richmap})

    options
    |> Keyword.fetch!(:schema_module)
    |> :hocon_schema.check(config, %{atom_key: true, return_plain: true})
    |> IO.inspect
    |> Enum.each(fn {application, envs} ->
      Config.config(application, Enum.to_list(envs))
    end)
  end
end

defmodule EmqxConfigHelper.Cuttlefish do
  def cuttlefish(application, options) do
    config =
      options
      |> Keyword.fetch!(:config_file)
      |> :cuttlefish_conf.file()

    application
    |> :code.priv_dir()
    |> Path.join("#{application}.schema")
    |> List.wrap()
    |> :cuttlefish_schema.files()
    |> :cuttlefish_generator.map(config)
    |> Enum.each(fn {application, envs} -> Config.config(application, envs) end)
  end
end
