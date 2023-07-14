from pathlib import Path

from unittest import TestCase

from pymetard import fetch_stations, Station


class TestStation(TestCase):
    def test_fetch_stations(self):
        station_file_path = Path(__file__).parent / "fixtures" / "fake_stations.txt"
        stations = fetch_stations(station_file_path)

        self.assertIsInstance(stations, dict)
        self.assertEqual(len(stations), 4)

        self.assertIn('PCSA', stations)
        self.assertIsInstance(stations['PCSA'], Station)
        self.assertEqual(stations['PCSA'].state_code, 'PC')
        self.assertEqual(stations['PCSA'].name, "STATION1")
        self.assertEqual(stations['PCSA'].code4, 'PCSA')
        self.assertEqual(round(stations['PCSA'].longitude, 2), -176.65)
        self.assertEqual(round(stations['PCSA'].latitude, 2), 51.88)
        self.assertEqual(stations['PCSA'].elevation, 3)

        self.assertIn('APSC', stations)
        self.assertIsInstance(stations['APSC'], Station)
        self.assertEqual(stations['APSC'].state_code, "  ")
        self.assertEqual(stations['APSC'].name, "STATION3")
        self.assertEqual(stations['APSC'].code4, 'APSC')
        self.assertEqual(round(stations['APSC'].longitude, 2), 89.83)
        self.assertEqual(round(stations['APSC'].latitude, 2), 13.57)
        self.assertEqual(stations['APSC'].elevation, 14)

        self.assertIn('AISD', stations)
        self.assertIsInstance(stations['AISD'], Station)
        self.assertEqual(stations['AISD'].state_code, "  ")
        self.assertEqual(stations['AISD'].name, 'STATION4')
        self.assertEqual(stations['AISD'].code4, 'AISD')
        self.assertEqual(round(stations['AISD'].longitude, 2), 152.32)
        # self.assertEqual(round(stations['AISD'].latitude, 2), -24.9)
        self.assertEqual(stations['AISD'].elevation, 30)

        self.assertIn('NWSE', stations)
        self.assertIsInstance(stations['NWSE'], Station)
        self.assertEqual(stations['NWSE'].state_code, "  ")
        self.assertEqual(stations['NWSE'].name, 'STATION5')
        self.assertEqual(stations['NWSE'].code4, 'NWSE')
        self.assertEqual(round(stations['NWSE'].longitude, 2), -58.42)
        self.assertEqual(round(stations['NWSE'].latitude, 2), -34.57)
        self.assertEqual(stations['NWSE'].elevation, 3462)
