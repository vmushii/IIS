from csv import DictReader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from domain.parking import Parking
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename

    def read(self, *args):
        accelerometer_reader_custom = DictReader(args[0], delimiter=",")
        gps_reader_custom = DictReader(args[1], delimiter=",")
        parking_reader_custom = DictReader(args[2], delimiter=",")
        accelerometer_data_custom = [
            Accelerometer(
                int(row["x"]),
                int(row["y"]),
                int(row["z"]),
            )
            for row in accelerometer_reader_custom
        ]
        gps_data_custom = [
            Gps(
                float(row["latitude"]),
                float(row["longitude"]),
            )
            for row in gps_reader_custom
        ]
        parking_data_custom = [
            Parking(
                int(row["empty_count"]),
                Gps(
                    float(row["latitude"]),
                    float(row["longitude"]),
                ),
            )
            for row in parking_reader_custom
        ]

        aggregated_data = []

        max_length = max(
            len(accelerometer_data_custom),
            len(gps_data_custom),
            len(parking_data_custom),
        )

        for i in range(max_length):
            accelerometer = (
                accelerometer_data_custom[i]
                if i < len(accelerometer_data_custom)
                else None
            )
            if i < len(gps_data_custom):
                gps = gps_data_custom[i]
            else:
                gps = Gps(float(0), float(0))

            if i < len(parking_data_custom):
                parking = parking_data_custom[i]
            else:
                parking = Parking(0, Gps(float(0), float(0)))

            aggregated_data.append(
                AggregatedData(
                    accelerometer, gps, parking, datetime.now(), config.USER_ID
                )
            )

        return aggregated_data

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        accelometerData = open(self.accelerometer_filename, "r")
        gpsData = open(self.gps_filename, "r")
        parkingData = open(self.parking_filename, "r")
        return accelometerData, gpsData, parkingData

    def stopReading(self, accelerometer, gps, parking):
        """Метод повинен викликатись для закінчення читання даних"""
        accelerometer.close()
        gps.close()
        parking.close()
