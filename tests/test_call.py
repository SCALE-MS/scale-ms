"""Test our ability to call a packaged function call through our CLI handler.

Example:
    python -m scalems.call package.json
"""
import logging
import os
import urllib.parse
from subprocess import CompletedProcess
from subprocess import run as subprocess_run
import sys
import tempfile

import scalems.call

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


sample_call_input = dict(
    func=subprocess_run,
    kwargs={"args": ["/bin/echo", "hello", "world"], "capture_output": True},
)


def test_call_builtin_func():
    """Test the API for scalems.call"""
    call = scalems.call.serialize_call(**sample_call_input)
    result = scalems.call.main(scalems.call.deserialize_call(call))
    assert result.exception is None
    complete_process: CompletedProcess = result.return_value
    # Note that we are not treating OS text encoding generally here. However,
    # this is a toy example in which we are just using `subprocess.run` as an
    # arbitrary Python callable for testing. Let's not generalize unless it
    # becomes and issue for devs in other locales.
    assert "hello world" in complete_process.stdout.decode(encoding="utf8")

    def local_echo(*text: str) -> tuple[str]:
        if isinstance(text, str):
            text = (text,)
        if not isinstance(text, tuple) or not all(isinstance(s, str) for s in text):
            raise ValueError("Function call requires one or more positional string arguments.")
        return tuple(text)

    try:
        local_echo(1)
    except ValueError as e:
        reference_error = repr(e)
    else:
        reference_error = None
    assert reference_error is not None

    sample_text_tuple = ("hi", "there")
    call = scalems.call.serialize_call(func=local_echo, args=sample_text_tuple)
    result = scalems.call.main(scalems.call.deserialize_call(call))
    assert result.exception is None
    output = result.return_value
    assert output == sample_text_tuple

    call = scalems.call.serialize_call(func=local_echo, args=(1,))
    result = scalems.call.main(scalems.call.deserialize_call(call))
    assert result.return_value is None
    assert result.exception == reference_error


def test_call_cli(tmp_path):
    """Run the command line in a subprocess to confirm reasonable behavior."""
    call = scalems.call.serialize_call(**sample_call_input)
    outfile = os.path.join(tmp_path, "scalems_out.json")

    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", dir=tmp_path) as tmp_file:
        tmp_file.write(call)
        tmp_file.flush()
        call_path = tmp_file.name
        process = subprocess_run(
            args=(sys.executable, "-m", "scalems.call", call_path, outfile),
            capture_output=True,
            cwd=tmp_path,
        )
    assert process.returncode == 0
    assert os.path.exists(outfile)
    with open(outfile, "r") as fh:
        result: scalems.call.CallResult = scalems.call.deserialize_result(fh.read())
    completed_process: CompletedProcess = result.return_value
    assert "hello world" in completed_process.stdout.decode(encoding="utf8")
    assert tmp_path.samefile(urllib.parse.urlparse(result.directory).path)
