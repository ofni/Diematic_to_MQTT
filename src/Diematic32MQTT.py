﻿#!/usr/bin/env python
# -*- coding: utf-8 -*-

import signal,threading
import configparser
import logging, logging.config
import DDModbus,Diematic3Panel,Hassio
import paho.mqtt.client as mqtt
import json
import time, datetime

class MessageBuffer:
    def __init__(self,mqtt):
        #logger
        self.logger = logging.getLogger(__name__)

        self.buffer=dict()
        self.mqtt=mqtt

    #clear buffer
    def clear(self):
        self.buffer=dict()

    #update or create a message in the buffer
    def update(self,topic,value):
        #if the topic is not in buffer
        if ((topic not in self.buffer) or (self.buffer[topic]['value']!=value)):
            self.buffer[topic]={'value':value,'update':True}

    #publish buffer content to MQTT broker
    def send(self):
        #for each topic
        for topic in self.buffer:
            if self.buffer[topic]['update']:
                #send message without trailing / on topic
                if (topic!=''):
                    #print('Publish :' + mqttTopicPrefix + '/' + topic + ' ' + self.buffer[topic]['value'])
                    self.mqtt.publish(mqttTopicPrefix+'/'+topic,self.buffer[topic]['value'],1,True)
                    self.logger.info('Publish :'+mqttTopicPrefix+'/'+topic+' '+self.buffer[topic]['value'])
                else:
                    self.mqtt.publish(mqttTopicPrefix,self.buffer[topic]['value'],1,True)
                    self.logger.info('Publish :'+mqttTopicPrefix+' '+self.buffer[topic]['value'])
                    #print('Publish :' + mqttTopicPrefix + ' ' + self.buffer[topic]['value'])
                #set the flag to False
                self.buffer[topic]['update']=False


def diematic3Publish(self):
    def floatValue(parameter):
        return (f"{parameter:.1f}" if parameter is not None else '')
    def intValue(parameter):
        return (f"{parameter:d}" if parameter is not None else '')

    #boiler
    buffer.update('status','Online' if self.availability else 'Offline')
    buffer.update('date',self.datetime.isoformat() if self.datetime is not None else '')
    buffer.update('type',intValue(self.type))
    buffer.update('ctrl',intValue(self.release))
    buffer.update('ext/temp',floatValue(self.extTemp))
    buffer.update('temp',floatValue(self.temp))
    buffer.update('targetTemp',floatValue(self.targetTemp))
    buffer.update('returnTemp',floatValue(self.returnTemp))
    buffer.update('waterPressure',floatValue(self.waterPressure))
    buffer.update('power',intValue(self.burnerPower))
    buffer.update('smokeTemp',floatValue(self.smokeTemp))
    buffer.update('ionizationCurrent',floatValue(self.ionizationCurrent))
    buffer.update('fanSpeed',intValue(self.fanSpeed))
    buffer.update('burnerStatus',intValue(self.burnerStatus))
    buffer.update('pumpPower',intValue(self.pumpPower))
    buffer.update('alarm',json.dumps(self.alarm) if self.alarm is not None else '')

    #hotwater
    buffer.update('hotWater/pump',intValue(self.hotWaterPump))
    buffer.update('hotWater/temp',floatValue(self.hotWaterTemp))
    buffer.update('hotWater/mode', self.hot_water_mode if self.hot_water_mode is not None else '')
    buffer.update('hotWater/dayTemp', floatValue(self.hot_water_day_target_temp))
    buffer.update('hotWater/nightTemp', floatValue(self.hot_water_night_target_temp))

    #area C
    buffer.update('zoneC/temp',floatValue(self.zoneCTemp))
    buffer.update('zoneC/mode', self.zone_c_mode if self.zone_c_mode is not None else '')
    buffer.update('zoneC/pump',intValue(self.zoneCPump))
    buffer.update('zoneC/dayTemp', floatValue(self.zone_c_day_target_temp))
    buffer.update('zoneC/nightTemp', floatValue(self.zone_c_night_target_temp))
    buffer.update('zoneC/antiiceTemp', floatValue(self.zone_c_antiice_target_temp))

    #area B
    buffer.update('zoneB/temp',floatValue(self.zoneBTemp))
    buffer.update('zoneB/mode', self.zone_b_mode if self.zone_b_mode is not None else '')
    buffer.update('zoneB/pump',intValue(self.zoneBPump))
    buffer.update('zoneB/dayTemp', floatValue(self.zone_b_day_target_temp))
    buffer.update('zoneB/nightTemp', floatValue(self.zone_b_night_target_temp))
    buffer.update('zoneB/antiiceTemp', floatValue(self.zone_b_antiice_target_temp))

    #send MQTT messages
    buffer.send()

def haSendDiscoveryMessages(client, userdata, message):
    if (message.payload.decode()=='online'):
        logger.info('Sending HA discovery messages')

        #boiler
        hassio.addSensor('heater_datetime',"Horloge Chaudière",None,'date',"{{ as_timestamp(value) |timestamp_custom ('%d/%m/%Y %H:%M') }}",None)
        hassio.addSwitch('heater_datetime_set',"Synchro Horloge",'unknown','date/set','--','Now')
        hassio.addSensor('type',"Type",None,'type',None,None)
        hassio.addSensor('ctrl',"Controleur",None,'ctrl',None,None)
        hassio.addSensor('ext_temp',"Température Extérieure",'temperature','ext/temp',None,"°C")
        hassio.addSensor('boiler_temp',"Température Chaudière",'temperature','temp',None,"°C")
        hassio.addSensor('target_temp',"Température Cible",'temperature','targetTemp',None,"°C")
        hassio.addSensor('return_temp',"Température Retour",'temperature','returnTemp',None,"°C")
        hassio.addSensor('water_pressure',"Pression d'eau",'pressure','waterPressure',None,"bar")
        hassio.addSensor('power',"Puissance",'power_factor','power',None,"%")
        hassio.addSensor('smoke_temp',"Température Fumées",'temperature','smokeTemp',None,"°C")
        hassio.addSensor('ionization_current',"Courant Ionisation",'current','ionizationCurrent',None,"µA")
        hassio.addSensor('fan_speed',"Vitesse Ventilateur",None,'fanSpeed',None,"RPM")
        hassio.addBinarySensor('burner_status',"Etat Bruleur",None,'burnerStatus',"1","0")
        hassio.addSensor('pump_power',"Puissance Pompe",'power_factor','pumpPower',None,"%")
        hassio.addSensor('alarm',"Etat",None,'alarm',"{{ value_json.txt}}",None)
        hassio.addSensor('alarm_id',"N° Erreur",None,'alarm',"{{ value_json.id}}",None)

        #hot water
        hassio.addBinarySensor('hot_water_pump',"Pompe ECS",None,'hotWater/pump',"1","0")
        hassio.addSensor('hot_water_temp',"Température ECS",'temperature','hotWater/temp',None,"°C")
        hassio.addSelect('hot_water_mode',"Mode ECS",'hotWater/mode','hotWater/mode/set',['AUTO','TEMP','PERM'])
        hassio.addSensor('hot_water_mode',"Mode ECS",None,'hotWater/mode',None,None)
        hassio.addNumber('hot_water_temp_day',"Température ECS Jour",'hotWater/dayTemp','hotWater/dayTemp/set',10,80,5,"°C")
        hassio.addNumber('hot_water_temp_night',"Température ECS Nuit",'hotWater/nightTemp','hotWater/nightTemp/set',10,80,5,"°C")

        #area A
        hassio.addSensor('zone_C_temp',"Température zone C",'temperature','zoneC/temp',None,"°C")
        hassio.addSelect('zone_C_mode',"Mode zone C",'zoneC/mode','zoneC/mode/set',['AUTO','TEMP JOUR','PERM JOUR','TEMP NUIT','PERM NUIT','ANTIGEL'])
        hassio.addSensor('zone_C_mode',"Mode zone C",None,'zoneC/mode',None,None)
        hassio.addBinarySensor('zone_C_pump',"Pompe zone C",None,'zoneC/pump',"1","0")
        hassio.addNumber('zone_C_temp_day',"Température Jour zone C",'zoneC/dayTemp','zoneC/dayTemp/set',5,30,0.5,"°C")
        hassio.addNumber('zone_C_temp_night',"Température Nuit zone C",'zoneC/nightTemp','zoneC/nightTemp/set',5,30,0.5,"°C")
        hassio.addNumber('zone_C_temp_antiice',"Température Antigel zone C",'zoneC/antiiceTemp','zoneC/antiiceTemp/set',5,20,0.5,"°C")

        #area B
        hassio.addSensor('zone_B_temp',"Température Zone B",'temperature','zoneB/temp',None,"°C")
        hassio.addSelect('zone_B_mode',"Mode Zone B",'zoneB/mode','zoneB/mode/set',['AUTO','TEMP JOUR','PERM JOUR','TEMP NUIT','PERM NUIT','ANTIGEL'])
        hassio.addSensor('zone_B_mode',"Mode Zone B",None,'zoneB/mode',None,None)
        hassio.addBinarySensor('zone_B_pump',"Pompe Zone B",None,'zoneB/pump',"1","0")
        hassio.addNumber('zone_B_temp_day',"Température Jour Zone B",'zoneB/dayTemp','zoneB/dayTemp/set',5,30,0.5,"°C")
        hassio.addNumber('zone_B_temp_night',"Température Nuit Zone B",'zoneB/nightTemp','zoneB/nightTemp/set',5,30,0.5,"°C")
        hassio.addNumber('zone_B_temp_antiice',"Température Antigel Zone B",'zoneB/antiiceTemp','zoneB/antiiceTemp/set',5,20,0.5,"°C")




def on_connect(client, userdata, flags, rc):		
    logger.critical('Connected to MQTT broker')
    print('Connected to MQTT broker')
    #subscribe to control messages with Q0s of 2
    client.subscribe(mqttTopicPrefix+'/+/+/set',2)
    client.subscribe(mqttTopicPrefix+'/date/set',2)
    if hassioDiscoveryEnable:
        client.subscribe(hassioDiscoveryPrefix+'/status',2)
    #clear buffer and inform client that status is still Offline
    buffer.clear()
    buffer.update('status','Offline')
    buffer.send()



def on_disconnect(client, userdata, rc):
    logger.critical('Diconnected from MQTT broker')

def modeSet(client, userdata, message):
    #print('mode set -> MQTT msg received :' + message.topic + ' ' + str(message.payload))
    #table for topic to attribute bind
    table={'/hotWater/mode/set':'hotWaterMode',
        '/zoneC/mode/set':'zone_c_mode',
        '/zoneB/mode/set':'zone_b_mode'}

    #remove root of the topic
    shortTopic=message.topic[len(mqttTopicPrefix):]

    #if topic exist
    if shortTopic in table:
        #process it
        #print('set attr',table[shortTopic],message.payload.decode() )
        setattr(panel,table[shortTopic],message.payload.decode())
        logger.info(shortTopic+' : '+str(message.payload))
    else:
        logger.warning('Unknown topic : '+shortTopic)

def tempSet(client, userdata, message):	
    #table for topic to attribute bind
    table={'/hotWater/dayTemp/set':'hotWaterDayTargetTemp',
        '/hotWater/nightTemp/set':'hotWaterNightTargetTemp',
        '/zoneC/dayTemp/set':'zone_c_day_target_temp',
        '/zoneC/nightTemp/set':'zone_c_night_target_temp',
        '/zoneC/antiiceTemp/set':'zone_c_antiice_target_temp',
        '/zoneB/dayTemp/set':'zone_b_day_target_temp',
        '/zoneB/nightTemp/set':'zone_b_night_target_temp',
        '/zoneB/antiiceTemp/set':'zone_b_antiice_target_temp'}

    #remove root of the topic
    shortTopic=message.topic[len(mqttTopicPrefix):]

    try:
        value=float(message.payload)
    except (ValueError,OverflowError):
        logger.warning('Value error :'+str(message.payload))
        return

    #if topic exist
    if shortTopic in table:
        #process it
        setattr(panel,table[shortTopic],value)
        logger.info(shortTopic+' : '+str(value))
    else:
        logger.warning('Unknown topic : '+shortTopic)

def dateSet(client, userdata, message):
    #table for topic to attribute bind
    table={'/date/set':'datetime'}

    #remove root of the topic
    shortTopic=message.topic[len(mqttTopicPrefix):]

    #if topic exist
    if shortTopic in table:
        #process it
        logger.info(shortTopic+' : '+str(message.payload))
        if (message.payload.decode()=='Now'):
            setattr(panel,table[shortTopic],datetime.datetime.now().astimezone())
    else:
        logger.warning('Unknown topic : '+shortTopic)

def paramSet(client, userdata, message):
    try:
        logger.debug('MQTT msg received :'+message.topic+' '+str(message.payload))
        #print('param set -> MQTT msg received :' + message.topic + ' ' + str(message.payload))
        if (message.topic[-8:]=='Temp/set'):
            tempSet(client, userdata, message)
        elif (message.topic[-8:]=='mode/set'):
            modeSet(client, userdata, message)
        elif (message.topic[-8:]=='date/set'):
            dateSet(client, userdata, message)
    except BaseException as exc:
        logger.exception(exc)

def sigterm_exit(signum, frame):
        logger.critical('Stop requested by SIGTERM, raising KeyboardInterrupt')
        raise KeyboardInterrupt


if __name__ == '__main__':

    # Initialisation Logger
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger(__name__)

    #Sigterm trapping
    signal.signal(signal.SIGTERM, sigterm_exit)
    try:
        #Initialisation config
        config = configparser.ConfigParser()
        config.read('Diematic32MQTT.conf')

        #Modbus settings
        modbusAddress=config.get('Modbus','ip')
        modbusPort=config.get('Modbus','port')
        modbusRegulatorAddress=int(config.get('Modbus','regulatorAddress'),0)
        logger.critical('Modbus interface address: '+modbusAddress+' : '+modbusPort)
        logger.critical('Modbus regulator address: '+ hex(modbusRegulatorAddress))

        #boiler time timezone
        boilerTimezone=config.get('Boiler','timezone')

        #MQTT settings
        mqttBrokerHost=config.get('MQTT','brokerHost')
        mqttBrokerPort=config.get('MQTT','brokerPort')
        mqttBrokerUser=config.get('MQTT','brokerUser')
        mqttBrokerPassword=config.get('MQTT','brokerPassword')

        mqttClientId=config.get('MQTT','clientId')
        mqttTopicPrefix=config.get('MQTT','topicPrefix')+'/'+mqttClientId

        logger.critical('Broker: '+mqttBrokerHost+' : '+mqttBrokerPort)
        logger.critical('Topic Root: '+mqttTopicPrefix)

        #Home Assistant discovery settings
        hassioDiscoveryEnable=config.getboolean('Home Assistant','MQTT_DiscoveryEnable')
        hassioDiscoveryPrefix=config.get('Home Assistant','discovery_prefix')

        logger.critical('Hassio Discovery Enable: '+ str(hassioDiscoveryEnable))
        logger.critical('Hassio Discovery Prefix: '+ hassioDiscoveryPrefix)


        #init panel
        period=int(config.get('Boiler','period'),0)
        Diematic3Panel.Diematic3Panel.updateCallback=diematic3Publish
        panel=Diematic3Panel.Diematic3Panel(modbusAddress,int(modbusPort),modbusRegulatorAddress,boilerTimezone)
        #set refresh period, with a minimum of 10s
        panel.refreshPeriod=max(period,10)


        #init mqtt brooker
        client = mqtt.Client()
        if mqttBrokerPassword and mqttBrokerUser:
            client.username_pw_set(mqttBrokerUser, mqttBrokerPassword)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        #last will
        client.will_set(mqttTopicPrefix+'/status',"Offline",1,True)
        client.connect_async(mqttBrokerHost, int(mqttBrokerPort))
        client.message_callback_add(mqttTopicPrefix+'/+/+/set',paramSet)
        client.message_callback_add(mqttTopicPrefix+'/date/set',paramSet)
        if hassioDiscoveryEnable:
            client.message_callback_add(hassioDiscoveryPrefix+'/status',haSendDiscoveryMessages)

        #create HomeAssistant discovery instance

        hassio=Hassio.Hassio(client,mqttTopicPrefix,mqttClientId,hassioDiscoveryPrefix)
        hassio.availabilityInfo('status','Online','Offline')

        #create mqtt message buffer
        buffer=MessageBuffer(client)

        #launch MQTT client
        client.loop_start()

        #start modbus thread
        panel.loop_start()
        run=True
        while run:
            #check every 10s that all threads are living
            time.sleep(5)
            if (threading.active_count()!=3):
                logger.critical('At least one process has been killed, stop launched')
                run=False
        #stop modbus thread
        panel.loop_stop()
        #disconnect mqtt server
        client.loop_stop()
        logger.critical('Stopped')
    except KeyboardInterrupt:
        #stop modbus thread
        panel.loop_stop()

        #disconnect mqtt server
        client.loop_stop()
        logger.critical('Stopped by KeyboardInterrupt')
    except BaseException as exc:
        logger.exception(exc)
