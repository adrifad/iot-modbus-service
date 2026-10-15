"""Analog correction and engineering scaling, kept separate from raw acquisition."""

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class Calibration:
    gain: float = 1.0
    offset_ma: float = 0.0
    enabled: bool = True


@dataclass(frozen=True)
class Scaling:
    input_min_ma: float
    input_max_ma: float
    engineering_min: float
    engineering_max: float
    unit: str = ""
    enabled: bool = False


def calibrate(raw_ma: float, config: Calibration) -> float:
    if not math.isfinite(raw_ma) or not math.isfinite(config.gain) or not math.isfinite(config.offset_ma):
        raise ValueError("calibration values must be finite")
    if config.gain <= 0:
        raise ValueError("gain must be greater than zero")
    return raw_ma * config.gain + config.offset_ma if config.enabled else raw_ma


def two_point(raw1: float, reference1: float, raw2: float, reference2: float) -> Calibration:
    values = (raw1, reference1, raw2, reference2)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("calibration points must be finite")
    delta = raw2 - raw1
    if abs(delta) < 0.1:
        raise ValueError("raw calibration points must be separated by at least 0.1 mA")
    gain = (reference2 - reference1) / delta
    if not 0.1 <= gain <= 10:
        raise ValueError("calculated gain is outside the supported range 0.1–10")
    return Calibration(gain=gain, offset_ma=reference1 - gain * raw1)


def scale(value_ma: float, config: Scaling) -> tuple[float | None, str]:
    if not config.enabled:
        return None, "DISABLED"
    values = (value_ma, config.input_min_ma, config.input_max_ma, config.engineering_min, config.engineering_max)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("scaling values must be finite")
    span = config.input_max_ma - config.input_min_ma
    if span <= 0:
        raise ValueError("input maximum must be greater than input minimum")
    result = config.engineering_min + ((value_ma - config.input_min_ma) / span) * (config.engineering_max - config.engineering_min)
    return result, "OUT_OF_RANGE" if value_ma < config.input_min_ma or value_ma > config.input_max_ma else "GOOD"
