defmodule DuckFeeder.TempFileReaperTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.TempFileReaper

  setup do
    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_temp_reaper_#{System.unique_integer([:positive])}"
      )

    :ok = File.mkdir_p(tmp_dir)

    on_exit(fn ->
      _ = File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "reaps stale files by prefix/suffix while preserving unrelated files", %{tmp_dir: tmp_dir} do
    now = System.os_time(:second)

    stale_file = Path.join(tmp_dir, "duck_feeder_stale.jsonl")
    fresh_file = Path.join(tmp_dir, "duck_feeder_fresh.jsonl")
    unrelated_file = Path.join(tmp_dir, "other_app.tmp")

    File.write!(stale_file, "stale")
    File.write!(fresh_file, "fresh")
    File.write!(unrelated_file, "other")

    :ok = File.touch(stale_file, now - 120)
    :ok = File.touch(fresh_file, now)

    assert {:ok, summary} =
             TempFileReaper.reap(
               tmp_dir: tmp_dir,
               prefix: "duck_feeder_",
               suffixes: [".jsonl"],
               stale_after_seconds: 30,
               now_posix_seconds: now
             )

    assert summary.checked >= 2
    assert summary.deleted == 1
    assert summary.errors == []

    refute File.exists?(stale_file)
    assert File.exists?(fresh_file)
    assert File.exists?(unrelated_file)
  end

  test "maybe_reap honors min interval between runs", %{tmp_dir: tmp_dir} do
    stale_file_1 = Path.join(tmp_dir, "duck_feeder_interval_1.parquet")
    stale_file_2 = Path.join(tmp_dir, "duck_feeder_interval_2.parquet")

    File.write!(stale_file_1, "stale")
    :ok = File.touch(stale_file_1, 10)

    assert :ok =
             TempFileReaper.maybe_reap(
               %{adapter_opts: %{tmp_dir: tmp_dir}},
               suffixes: [".parquet"],
               stale_after_seconds: 0,
               min_interval_ms: 60_000,
               now_mono_ms: 1_000,
               now_posix_seconds: 2_000
             )

    refute File.exists?(stale_file_1)

    File.write!(stale_file_2, "stale")
    :ok = File.touch(stale_file_2, 10)

    assert :ok =
             TempFileReaper.maybe_reap(
               %{adapter_opts: %{tmp_dir: tmp_dir}},
               suffixes: [".parquet"],
               stale_after_seconds: 0,
               min_interval_ms: 60_000,
               now_mono_ms: 1_100,
               now_posix_seconds: 3_000
             )

    assert File.exists?(stale_file_2)
  end
end
