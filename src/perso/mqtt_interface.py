#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging.config

import paho.mqtt.client as mqtt


class MqttInterface:

    def __init__(self, conf):

        logging.config.fileConfig('logging.conf')
        self.logger = logging.getLogger(__name__)

        host = conf.get('MQTT', 'brokerHost')
        port = conf.get('MQTT', 'brokerPort')
        user = conf.get('MQTT', 'brokerUser')
        pwd = conf.get('MQTT', 'brokerPassword')

        mqttClientId = conf.get('MQTT', 'clientId')

        self.mqttTopicPrefix = conf.get('MQTT', 'topicPrefix') + '/' + mqttClientId

        self.logger.critical(f'Root topic: {self.mqttTopicPrefix}')

        self.client = mqtt.Client()
        self.logger.critical(f'Broker: {host}:{port}')

        if user and pwd:
            self.client.username_pw_set(user, pwd)

        self.client.will_set(self.mqttTopicPrefix + '/boiler/status', "Offline", 1, True)
        self.client.connect_async(host, int(port))
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def loop_start(self):
        # launch MQTT client
        self.client.loop_start()

    def loop_stop(self):
        self.client.publish('home/heater2/boiler/boiler/status', "Offline", 2, True)
        self.client.loop_stop()

    def add_callback(self, topic, func):
        self.client.message_callback_add(f'{self.mqttTopicPrefix}/{topic}', func)

    def send(self, registers):
        if registers:
            for register in registers:
                self.client.publish(
                        f"{self.mqttTopicPrefix}/{register['system']}/{register['name']}", str(register['value']), 1, True
                )
        else:
            print("nothing to publish")

    def on_connect(self, client, userdata, flags, rc):
        print('Connected to MQTT broker')
        self.logger.critical('Connected to MQTT broker')
        client.subscribe(f'{self.mqttTopicPrefix}/+/mode/set', 2)
        client.subscribe(f'{self.mqttTopicPrefix}/date/set', 2)
        client.subscribe(f'{self.mqttTopicPrefix}/temp/reset', 2)

    def on_disconnect(self, client, userdata, rc):
        print('Disconnected from MQTT broker')
        self.logger.critical('Disconnected from MQTT broker')