"""Render a complete fixed-size PTY capture without retaining cleared menu text."""

import pyte


def terminal_screen(client):
    with client.lock:
        if client.total != len(client.window):
            raise RuntimeError('terminal history was truncated; cannot reconstruct approval screen')
        raw = bytes(client.window)
    screen = pyte.Screen(80, 24)  # wake_probe.PTY_SIZE; no terminal resize is sent.
    pyte.ByteStream(screen).feed(raw)
    return '\n'.join(screen.display)
