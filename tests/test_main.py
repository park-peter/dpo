"""Tests for module entrypoint (`python -m dpo`)."""

import runpy


def test_module_entrypoint_invokes_cli_main(monkeypatch):
    """Executing `dpo.__main__` should call dpo.cli.main."""
    called = {"value": False}

    def _fake_main():
        called["value"] = True

    monkeypatch.setattr("dpo.cli.main", _fake_main)
    runpy.run_module("dpo.__main__", run_name="__main__")

    assert called["value"] is True
