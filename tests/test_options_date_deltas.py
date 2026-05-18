import unittest

from yahoors.modules.options import Options


class OptionsDateDeltaTests(unittest.TestCase):
    def test_calc_date_deltas_returns_dict_limited_to_max_dte(self):
        options = Options.__new__(Options)

        result = options._calc_date_deltas(
            dates=["2026-01-03", "2026-01-07", "2026-01-12"],
            max_dte=7,
            ref_date="2026-01-01",
        )

        self.assertEqual(
            result,
            {
                "2026-01-03": 2,
                "2026-01-07": 6,
            },
        )


if __name__ == "__main__":
    unittest.main()
