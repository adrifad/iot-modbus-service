import unittest

from backend.calibration import Calibration, Scaling, calibrate, scale, two_point


class CalibrationTests(unittest.TestCase):
    def test_identity_and_offset_calibration(self):
        self.assertEqual(calibrate(11.85, Calibration()), 11.85)
        self.assertAlmostEqual(calibrate(11.85, Calibration(offset_ma=0.15)), 12.0)
        self.assertAlmostEqual(calibrate(11.85, Calibration(offset_ma=-0.15)), 11.7)

    def test_gain_and_disable(self):
        self.assertAlmostEqual(calibrate(10.0, Calibration(gain=1.02, offset_ma=-0.2)), 10.0)
        self.assertEqual(calibrate(10.0, Calibration(gain=2, offset_ma=1, enabled=False)), 10.0)

    def test_two_point_calibration(self):
        result = two_point(4.1, 4.0, 19.9, 20.0)
        self.assertAlmostEqual(result.gain, 16 / 15.8)
        self.assertAlmostEqual(calibrate(4.1, result), 4.0)
        self.assertAlmostEqual(calibrate(19.9, result), 20.0)

    def test_rejects_unstable_two_point_calibration(self):
        with self.assertRaisesRegex(ValueError, "separated"):
            two_point(4, 4, 4.01, 20)

    def test_engineering_scaling_and_out_of_range_quality(self):
        config = Scaling(4, 20, 0, 100, "bar", True)
        self.assertEqual(scale(12, config), (50, "GOOD"))
        value, quality = scale(22, config)
        self.assertEqual(value, 112.5)
        self.assertEqual(quality, "OUT_OF_RANGE")

    def test_scaling_does_not_silently_clamp_and_rejects_zero_span(self):
        value, status = scale(25, Scaling(0, 20, 0, 10, "m", True))
        self.assertEqual(value, 12.5)
        self.assertEqual(status, "OUT_OF_RANGE")
        with self.assertRaisesRegex(ValueError, "greater"):
            scale(3, Scaling(4, 4, 0, 100, "bar", True))


if __name__ == "__main__":
    unittest.main()
