defmodule Mix.Tasks.Compile.Asn1 do
  use Mix.Task.Compiler

  @recursive true
  @manifest_vsn 2
  @manifest "compile.asn1"
  # TODO: use manifest to track generated files?

  @stale? {__MODULE__, :stale?}

  @impl true
  def manifests(), do: [manifest()]
  defp manifest(), do: Path.join(Mix.Project.manifest_path(), @manifest)

  @impl true
  def run(_args) do
    add_to_path_and_cache(:asn1)

    Mix.Project.get!()
    config = Mix.Project.config()
    app_root = File.cwd!()

    asn1_srcs = config[:asn1_srcs] || []
    manifest_data = read_manifest(manifest())
    manifest_modified_time = Mix.Utils.last_modified(manifest())
    Enum.each(asn1_srcs, &compile(&1, app_root, manifest_modified_time, manifest_data))

    if Process.get(@stale?, false) do
      manifest_data =
        Enum.reduce(asn1_srcs, manifest_data, fn asn1_src, acc ->
          %{src: src_path, compile_opts: compile_opts} = asn1_src
          put_in(acc, [:compile_opts, src_path], compile_opts)
        end)

      write_manifest(manifest(), manifest_data)
    end

    {:noop, []}
  after
    Process.delete(@stale?)
  end

  defp compile(src, app_root, manifest_modified_time, manifest_data) do
    %{
      src: src_path,
      compile_opts: compile_opts
    } = src

    src_path =
      app_root
      |> Path.join(src_path)
      |> Path.expand()

    if stale?(src, manifest_modified_time, app_root, manifest_data) do
      Process.put(@stale?, true)
      debug("compiling asn1 file: #{src_path}")
      :ok = :asn1ct.compile(to_charlist(src_path), compile_opts)
    else
      debug("file is up to date, not compiling: #{src_path}")
    end
  end

  defp stale?(src, manifest_modified_time, app_root, manifest_data) do
    %{
      src: src_path,
      compile_opts: compile_opts
    } = src

    out_files = out_files(src, app_root)
    previous_compile_opts = get_in(manifest_data, [:compile_opts, src_path]) || :undefined

    with true <- previous_compile_opts == compile_opts,
         true <- Enum.all?([src_path | out_files], &File.exists?/1),
         false <- Mix.Utils.stale?([src_path], [manifest_modified_time]) do
      false
    else
      _ -> true
    end
  end

  defp out_files(%{src: src_path, compile_opts: compile_opts}, app_root) do
    outdir = Keyword.fetch!(compile_opts, :outdir)
    name = Path.basename(src_path, ".asn")
    # note: not all asn1 sources output header files; hence no hrl
    Enum.map(["asn1db", "erl"], fn ext ->
      Path.join([app_root, outdir, "#{name}.#{ext}"])
    end)
  end

  defp read_manifest(file) do
    try do
      file |> File.read!() |> :erlang.binary_to_term()
    rescue
      _ -> empty_manifest()
    else
      {@manifest_vsn, data} when is_map(data) -> data
      _ -> empty_manifest()
    end
  end

  defp empty_manifest() do
    %{compile_opts: %{}}
  end

  defp write_manifest(file, data) do
    debug("writing manifest #{file}")
    File.mkdir_p!(Path.dirname(file))
    File.write!(file, :erlang.term_to_binary({@manifest_vsn, data}))
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

  defp debug(iodata) do
    if Mix.debug?() do
      Mix.shell().info(IO.ANSI.format([:cyan, iodata]))
    end
  end
end
