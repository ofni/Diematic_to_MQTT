#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import datetime
import json
import logging.config
import signal
import time
import threading

from mqtt_interface import MqttInterface

from modbus_interface import DiematicModbusInterface, DDREGISTER


class Boiler:

    def __init__(self, conf):
        self.mqtt_client = MqttInterface(conf)

        self.mqtt_client.add_callback('+/mode/set', self.set_mode)
        self.mqtt_client.add_callback('date/set', self.set_date)
        self.mqtt_client.add_callback('temp/reset', self.reset_temp)

        self.boiler = DiematicModbusInterface(self.mqtt_client)

    def set_mode(self, client, userdata, message):

        mode = message.payload.decode()
        print('setting mode', mode)

        if message.topic == 'home/heater2/boiler/circuit_B/mode/set':
            register = DDREGISTER.MODE_B
        elif message.topic == 'home/heater2/boiler/circuit_C/mode/set':
            register = DDREGISTER.MODE_C

        if mode == 'AUTO':
            self.boiler.write_register(register, 8)
        elif mode == 'TEMP JOUR':
            self.boiler.write_register(register, 36)
        elif mode == 'TEMP NUIT':
            self.boiler.write_register(register, 34)
        elif mode == 'PERM JOUR':
            self.boiler.write_register(register, 4)
        elif mode == 'PERM NUIT':
            self.boiler.write_register(register, 2)
        elif mode == 'ANTIGEL':
            self.boiler.write_register(register, 1)

    def set_date(self, client, userdata, message):
        if message.payload.decode() == 'now':
            date = datetime.datetime.now()
            print('datetime requested:' + date.isoformat())

            # Request hour/minute/weekday registers change
            self.boiler.write_register(DDREGISTER.HEURE, date.hour)
            self.boiler.write_register(DDREGISTER.MINUTE, date.minute)
            self.boiler.write_register(DDREGISTER.JOUR_SEMAINE, date.isoweekday())

            # Request day/month/year registers change
            self.boiler.write_register(DDREGISTER.MOIS, date.month)
            self.boiler.write_register(DDREGISTER.JOUR, date.day)
            self.boiler.write_register(DDREGISTER.ANNEE, date.year % 100)

    def reset_temp(self, client, userdata, message):

        message = json.loads(message.payload.decode('utf-8'))

        cons_temps = [
            message.get('jour', DDREGISTER.DEFAULT_CONS_JOUR),
            message.get('nuit', DDREGISTER.DEFAULT_CONS_NUIT),
            message.get('antigel', DDREGISTER.DEFAULT_CONS_ANTIGEL)
            ]

        temps = list(map(lambda x: min(max(round(2*x)*5, DDREGISTER.TEMP_MIN_INT*10), DDREGISTER.TEMP_MAX_INT*10), cons_temps))

        print('setting temp', temps)

        self.boiler.write_register(DDREGISTER.CONS_JOUR_B, temps[0])
        self.boiler.write_register(DDREGISTER.CONS_NUIT_B, temps[1])
        self.boiler.write_register(DDREGISTER.CONS_ANTIGEL_B, temps[2])

        self.boiler.write_register(DDREGISTER.CONS_JOUR_C, temps[0])
        self.boiler.write_register(DDREGISTER.CONS_NUIT_C, temps[1])
        self.boiler.write_register(DDREGISTER.CONS_ANTIGEL_C, temps[2])

    def loop_start(self):
        self.mqtt_client.loop_start()
        self.boiler.loop_start()

    def loop_stop(self):
        logger.critical('stopping mqtt')
        self.mqtt_client.loop_stop()
        logger.critical('stopping modbus')
        self.boiler.loop_stop()


def sigterm_exit(signum, frame):
    logger.critical('Stop requested by SIGTERM, raising KeyboardInterrupt')
    raise KeyboardInterrupt


if __name__ == '__main__':

    # Initialisation Logger
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger(__name__)

    # Sigterm trapping
    signal.signal(signal.SIGTERM, sigterm_exit)
    try:
        # Initialisation config
        config = configparser.ConfigParser()
        config.read('Diematic32MQTT.conf')

        # Modbus settings
        modbusAddress = config.get('Modbus', 'ip')
        modbusPort = config.get('Modbus', 'port')
        modbusRegulatorAddress = int(config.get('Modbus', 'regulatorAddress'), 0)
        logger.critical('Modbus interface address: '+modbusAddress+' : '+modbusPort)
        logger.critical('Modbus regulator address: ' + hex(modbusRegulatorAddress))

        # Init Boiler
        boiler = Boiler(config)
        boiler.loop_start()

        run = True
        while run:
            # Check every 10s that all threads are living
            time.sleep(10)

            # in normal mode, 6 in debug with pycharm
            if threading.active_count() != 3 and threading.active_count() != 6:
                logger.critical('At least one process has been killed, stop launched')
                run = False

        boiler.loop_stop()
        logger.critical('Stopped')

    except KeyboardInterrupt:
        boiler.loop_stop()
        logger.critical('Stopped by KeyboardInterrupt')
    except BaseException as exc:
        logger.exception(exc)
