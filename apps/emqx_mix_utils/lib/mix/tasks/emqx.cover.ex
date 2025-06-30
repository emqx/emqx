defmodule Mix.Tasks.Emqx.Cover do
  use Mix.Task

  alias Mix.Tasks.Emqx.Ct, as: ECt

  @requirements ["compile", "loadpaths"]

  @impl true
  def run(_args) do
    cover_dir = Path.join([Mix.Project.build_path(), "cover"])
    File.mkdir_p!(cover_dir)

    Enum.each([:common_test, :eunit, :mnesia, :tools], &ECt.add_to_path_and_cache/1)
    {:ok, cover_pid} = ECt.start_cover()
    redirect_cover_output(cover_pid, cover_dir)

    coverdata_files =
      cover_dir
      |> List.wrap()
      |> Mix.Utils.extract_files("*.coverdata")

    :ok = :cover.reset()

    ECt.info("Loading coverdata...")

    Enum.each(coverdata_files, fn coverdata_file ->
      case :cover.import(to_charlist(coverdata_file)) do
        :ok ->
          :ok

        {:error, {:cant_open_file, f, _reason}} ->
          ECt.warn("Can't import cover data from #{f}")
      end
    end)

    outdir = Path.join([cover_dir, "aggregate"])
    File.mkdir_p!(outdir)

    ECt.info("Analyzing coverdata...")

    results =
      :cover.imported_modules()
      |> Enum.flat_map(fn mod ->
        # {:ok, answer} = :cover.analyze(mod, :coverage, :line)
        outfile = Path.join([outdir, "#{mod}.html"]) |> to_charlist()
        coverage = compute_coverage(mod)
        ECt.debug("Analyzing coverage of #{mod}")

        case :cover.analyze_to_file(mod, outfile, [:html]) do
          {:ok, _file} ->
            mod_report_path = Path.relative_to(outfile, cover_dir)
            [%{mod: mod, mod_report_path: mod_report_path, cover_percentage: coverage}]

          {:error, reason} ->
            ECt.warn(
              "Couldn't write annotated file for module #{mod}: #{inspect(reason, pretty: true)}"
            )

            []
        end
      end)
      |> Enum.sort_by(& &1.mod)

    # require IEx; IEx.pry()

    aggregate_outfile = Path.join(cover_dir, "index.html")
    ECt.info("Saving analysis index to #{aggregate_outfile}")

    __DIR__
    |> Path.join("cover_index.html.eex")
    |> EEx.eval_file([lines: results], [])
    |> then(&File.write!(aggregate_outfile, &1))

    ECt.info("Cover analysis done")

    :ok
  end

  defp compute_coverage(mod) do
    with {:ok, result} <- :cover.analyse(mod, :coverage, :line) do
      coverage =
        Enum.reduce(result, {0, 0}, fn
          {{_, 0}, _}, acc ->
            # line 0 is a line added by eunit and never executed so ignore it
            acc

          {_, {covered, not_covered}}, {acc_cov, acc_not_cov} ->
            {acc_cov + covered, acc_not_cov + not_covered}
        end)

      case coverage do
        {_, 0} -> 100
        {cov, not_cov} -> trunc(cov / (cov + not_cov) * 100)
      end
    else
      err ->
        ECt.warn("Couldn't analyze coverage of #{mod}: #{inspect(err, pretty: true)}")
        0
    end
  end

  defp redirect_cover_output(cover_pid, cover_dir) do
    log_out = Path.join(cover_dir, "cover.log")
    ECt.debug("Writing :cover log output to #{log_out}")
    File.rm(log_out)
    fd = File.open!(log_out, [:append])
    Process.group_leader(cover_pid, fd)
  end
end
