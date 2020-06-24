from click.testing import CliRunner
import packtivity.cli


def test_maincli(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.runcli,
        [
            "tests/testspecs/localtouchfile.yml",
            "-p",
            'outputfile="{workdir}/hello.txt"',
            "-w",
            str(tmpdir),
        ],
    )
    assert result.exit_code == 0
    assert tmpdir.join("hello.txt").check()


def test_maincli_fail(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.runcli,
        [
            "tests/testspecs/localtouchfail.yml",
            "-p",
            'outputfile="{workdir}/hello.txt"',
            "-w",
            str(tmpdir),
        ],
    )
    assert result.exit_code != 0


def test_maincli_async(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.runcli,
        [
            "tests/testspecs/localtouchfile.yml",
            "-p",
            'outputfile="{workdir}/hello.txt"',
            "-w",
            str(tmpdir.join("workdir")),
            "-b",
            "foregroundasync",
            "-x",
            str(tmpdir.join("proxy.json")),
            "--async",
        ],
    )
    assert result.exit_code == 0
    assert tmpdir.join("proxy.json").check()

    result = runner.invoke(packtivity.cli.checkproxy, [str(tmpdir.join("proxy.json"))])
    assert result.exit_code == 0


def test_maincli(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.runcli,
        [
            "tests/testspecs/localtouchfile.yml",
            "-p",
            'outputfile="{workdir}/hello.txt"',
            "-w",
            str(tmpdir),
        ],
    )
    assert result.exit_code == 0
    assert tmpdir.join("hello.txt").check()


def test_maincli_fail(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.runcli,
        [
            "tests/testspecs/localtouchfail.yml",
            "-p",
            'outputfile="{workdir}/hello.txt"',
            "-w",
            str(tmpdir),
        ],
    )
    assert result.exit_code != 0


def test_validatecli_valid(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.validatecli, ["tests/testspecs/noop-test.yml"]
    )
    assert result.exit_code == 0


def test_validatecli_invalid(tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        packtivity.cli.validatecli, ["tests/testspecs/noop-test-invalid.yml"]
    )
    assert result.exit_code == 1
