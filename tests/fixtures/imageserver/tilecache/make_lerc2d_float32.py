"""Build a synthetic float32 LERC2D cache tile, 256x256, partially masked."""

from __future__ import annotations

from pathlib import Path

import lerc
import numpy as np

SIZE = 256
data = np.full((SIZE, SIZE), 741.4, dtype=np.float32)
mask = np.zeros((SIZE, SIZE), dtype=np.uint8)
# A ragged footprint, so the mask cannot be inferred from the values.
rows, cols = np.mgrid[0:SIZE, 0:SIZE]
valid = (rows + cols) % 7 != 0
valid &= rows < 200
mask[valid] = 1
data[~valid] = 0.0

code, n, blob = lerc.encode(data, 1, True, mask, 0.0, len(data.tobytes()) + 1024)
print("encode code", code, "bytes", n)
out = Path(__file__).with_name("lerc2d_float32.bin")
out.write_bytes(bytes(blob[:n]))
print("valid pixels", int(valid.sum()), "->", out)

res = lerc.decode(out.read_bytes())
print(
    "decode",
    res[0],
    res[1].dtype,
    res[1].shape,
    int((res[2] > 0).sum()) if res[2] is not None else None,
)
