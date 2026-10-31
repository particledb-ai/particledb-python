"""Check memory measurement independently of the SDK and database."""

import json
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

PROBE = Path(__file__).with_name('peak_memory.py').read_text()


def execute(code):
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True,
                            check=True, timeout=30)
    return json.loads(result.stdout)


def test_peak_includes_released_memory():
    result = execute(PROBE + textwrap.dedent('''
        import gc, json
        data = bytearray(96 * 1024 * 1024)
        for offset in range(0, len(data), 4096):
            data[offset] = 1
        del data
        gc.collect()
        print(json.dumps({'peak': peak_rss_mib()}))
    '''))
    assert result['peak'] >= 96


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Linux exec resource accounting')
def test_exec_does_not_charge_the_previous_process_image():
    worker = PROBE + textwrap.dedent('''
        import json, resource
        print(json.dumps({'peak': peak_rss_mib(),
                          'historical': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024}))
    ''')
    launcher = textwrap.dedent('''
        import os, sys
        data = bytearray(192 * 1024 * 1024)
        for offset in range(0, len(data), 4096):
            data[offset] = 1
        os.execv(sys.executable, [sys.executable, '-c', sys.argv[1]])
    ''')
    result = subprocess.run([sys.executable, '-c', launcher, worker], capture_output=True,
                            text=True, check=True, timeout=30)
    measured = json.loads(result.stdout)
    assert measured['historical'] >= 192
    assert measured['peak'] < 64
