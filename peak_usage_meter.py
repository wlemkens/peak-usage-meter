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
        self.first_time = None
        self.last_time = None
        self.first_consumption = None
        self.last_consumption = None
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
                logging.info(f'{datetime.now().strftime("%H:%M:%S")} Connecting to MQTT')
                self.client.connect(self.mqtt_host, self.mqtt_port, 60)
                self.client.subscribe(self.mqtt_read_topic, 1)
                self.client.subscribe("energy/peak_usage", 0)
                logging.info(f'{datetime.now().strftime("%H:%M:%S")} Connected to MQTT')
                self.client.loop_forever()
            except:
                logging.warning(f'{datetime.now().strftime("%H:%M:%S")} Failed to connect to MQTT')
                time.sleep(10)

    def on_message(self, client, userdata, message):
        logging.debug(
            f'{datetime.now().strftime("%H:%M:%S")} Received message payload = {float(message.payload)}, timestamp = {message.timestamp}')
        if message.topic == "energy/peak_usage":
            self.highest_usage = float(message.payload)
            logging.info(
                f'{datetime.now().strftime("%H:%M:%S")} Setting peak usage from earlier to {self.highest_usage}W.')
        elif message.topic == "homeassistant/sensor/grid/usage":
            now = datetime.now()
            if self.first_time != None:
                dt = (now - self.first_time).seconds
                current_consumption = float(message.payload)
                # usage kWh 1kWh /3600s -> 1kW
                du = current_consumption - self.first_consumption
                usage = du * 1000 / dt * 3600
                self.client.publish("energy/current_peak_usage", usage, 2, False)
                logging.debug(
                    f'{datetime.now().strftime("%H:%M:%S")} Peak usage {usage}W')
                if (usage > self.highest_usage) and usage > 2500:
                    self.client.publish("energy/peak_usage_warning", True, 2, False)
                    logging.debug(
                        f'{datetime.now().strftime("%H:%M:%S")} Peak usage warning.')
                else:
                    logging.debug(
                        f'{datetime.now().strftime("%H:%M:%S")} Peak usage no warning.')
                    self.client.publish("energy/peak_usage_warning", False, 2, False)
                if (now.minute % 15 < self.first_time.minute % 15) or dt >= 15*60:
                    logging.debug(
                        f'{datetime.now().strftime("%H:%M:%S")} 15 minutes have passed.')
                    # New time block
                    self.first_time = now
                    self.first_consumption = current_consumption
                    if self.highest_usage < usage:
                        logging.info(
                            f'{datetime.now().strftime("%H:%M:%S")} Peak usage increased from = {self.highest_usage}W to {usage}W.')
                        self.highest_usage = usage
                    self.client.publish("energy/peak_usage", self.highest_usage, 2, True)
                    logging.debug(
                        f'{datetime.now().strftime("%H:%M:%S")} Published peak usage.')
                    if now.day == 1:
                        logging.info(
                            f'{datetime.now().strftime("%H:%M:%S")} Peak usage reset because of new month.')
                        self.highest_usage = 0
            else:
                self.first_time = now
                self.first_consumption = float(message.payload)

def main(argv):
    meter = PeakPowerMeter()
    meter.connect()

if __name__ == '__main__':
    main(sys.argv[1:])





