import paho.mqtt.client as mqtt
import time
import logging
from datetime import datetime
import sys

class PeakPowerMeter():

    def __init__(self):
        log_file = "/var/log/peak_usage.log"
        logging.basicConfig(filename=log_file, level=logging.DEBUG)
        logging.info(f'{datetime.now().strftime("%H:%M:%S")} Starting peak usage meter')
        self.reset_time = None
        self.first_time = None
        self.latest_update = None
        self.previous_update = None
        self.latest_consumption = None
        self.last_update = None
        self.first_consumption = None
        self.last_consumption = None
        self.interval = 15
        self.highest_usage = 0
        self.mqtt_host = "192.168.0.183"
        self.mqtt_port = 1883
        self.mqtt_read_topic = "homeassistant/sensor/grid/usage"
        self.client = mqtt.Client()
        self.client.on_message = self.on_message

    def connect(self):
        connecting = True
        while(connecting):
            try:
                logging.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Connecting to MQTT')
                self.client.connect(self.mqtt_host, self.mqtt_port, 60)
                self.client.subscribe(self.mqtt_read_topic, 1)
                self.client.subscribe("energy/peak_usage", 0)
                logging.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Connected to MQTT')
                self.client.loop_start()
                self.last_update = datetime.now()
                while True:
                    time.sleep(15)
                    if self.needs_update():
                        self.update_usage()
            except NameError:
                logging.warning(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Failed to connect to MQTT')
                time.sleep(10)

    def needs_update(self):
        now = datetime.now()
        return (now - self.last_update).total_seconds() >= self.interval


    def update_interval(self, usage):
        if usage > 0:
            self.interval = min(5 * 60, 36000 / usage)
        else:
            self.interval = 5 * 60
        logging.debug(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Set update interval to {self.interval:.1f}s')

    def publish_current_peak(self, usage):
        self.client.publish("energy/current_peak_usage", usage, 2, False)
        logging.debug(
            f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Peak usage {usage:.0f}W')
        if (usage > self.highest_usage) and usage > 2500:
            self.client.publish("energy/peak_usage_warning", True, 2, False)
            logging.debug(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Peak usage warning.')
        else:
            logging.debug(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Peak usage no warning.')
            self.client.publish("energy/peak_usage_warning", False, 2, False)

    def publish_rollover_if_needed(self, now, dt, usage):
        if (now.minute % 15 < self.first_time.minute % 15) or (now - self.first_time).total_seconds() >= 15 * 60:
            logging.debug(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 15 minutes have passed. Publishing peak.')
            # New time block
            if self.highest_usage < usage:
                logging.info(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Peak usage increased from = {self.highest_usage:.0f}W to {usage:.0f}W.')
                self.highest_usage = usage
            self.client.publish("energy/peak_usage", self.highest_usage, 2, True)
            logging.debug(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Published peak usage.')
            self.first_time = now
            self.first_consumption = self.latest_consumption
            if now.day == 1:
                logging.info(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Peak usage reset because of new month.')
                self.highest_usage = 0

    def update_usage(self):
        now = datetime.now()
        self.last_update = now
        if self.first_time != None:
            dt = (now - self.first_time).total_seconds()
            if self.previous_update < self.first_time:
                logging.debug(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Last update was too long ago. Using full period for calculation.')
                dt = (now - self.previous_update).total_seconds()
            du = self.latest_consumption - self.first_consumption
            usage = du * 1000 / dt * 3600
            self.update_interval(usage)

            self.publish_current_peak(usage)
            self.publish_rollover_if_needed(now, dt, usage)
        else:
            self.update_interval(0)
            self.publish_current_peak(0)

    def register_consumption(self, consumption: float, now):
        self.latest_consumption = consumption
        self.previous_update = self.latest_update
        self.latest_update = now

    def on_message(self, client, userdata, message):
        logging.debug(
            f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Received message payload = {float(message.payload)}, timestamp = {message.timestamp} on topic {message.topic}')
        if message.topic == "energy/peak_usage":
            self.highest_usage = float(message.payload)
            logging.info(
                f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Setting peak usage from earlier to {self.highest_usage:.0f}W.')
        elif message.topic == "homeassistant/sensor/grid/usage":
            now = datetime.now()
            self.register_consumption(float(message.payload), now)
            if self.first_time != None:
                dt = (now - self.first_time).total_seconds()
                current_consumption = float(message.payload)
                logging.debug(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Consumption {current_consumption}kWh, dt = {dt}s')
                # usage kWh 1kWh /3600s -> 1kW
                du = current_consumption - self.first_consumption
                usage = du * 1000 / dt * 3600
                self.update_interval(usage)
            else:
                self.first_time = now
                self.first_consumption = float(message.payload)
                self.reset_time = now


def main(argv):
    meter = PeakPowerMeter()
    meter.connect()

if __name__ == '__main__':
    main(sys.argv[1:])





