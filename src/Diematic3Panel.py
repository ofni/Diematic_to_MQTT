﻿#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading,queue
import logging, logging.config
import DDModbus
import time,datetime,pytz
from enum import IntEnum

#Target Temp min/max for hotwater
TEMP_MIN_ECS = 10
TEMP_MAX_ECS = 80

#Target Temp min/max for hotwater
TEMP_MIN_INT = 5
TEMP_MAX_INT = 30

#definition for state machine used for modBus data exchange
class DDModBusStatus(IntEnum):
    INIT = 0
    SLAVE = 1
    MASTER = 2

#definition of Diematic Register used to read/write functional attributes values

class DDREGISTER(IntEnum):
    CTRL = 3
    HEURE = 4
    MINUTE = 5
    JOUR_SEMAINE = 6
    TEMP_EXT = 7
    NB_JOUR_ANTIGEL = 13
    CONS_JOUR_A = 14
    CONS_NUIT_A = 15
    CONS_ANTIGEL_A = 16
    MODE_A = 17
    TEMP_AMB_A = 18
    TCALC_A = 21
    CONS_JOUR_B = 23
    CONS_NUIT_B = 24
    CONS_ANTIGEL_B = 25
    MODE_B = 26
    TEMP_AMB_B = 27
    TCALC_B = 32
    CONS_ECS = 59
    TEMP_ECS = 62
    TEMP_CHAUD = 75
    CONS_ECS_NUIT = 96
    JOUR = 108
    MOIS = 109
    ANNEE = 110
    BASE_ECS = 427
    OPTIONS_B_C = 428
    IONIZATION_CURRENT = 451
    RETURN_TEMP = 453
    SMOKE_TEMP = 454
    FAN_SPEED = 455
    PRESSION_EAU = 456
    BOILER_TYPE = 457
    PUMP_POWER = 463
    ALARME = 465

#This class allow to read/write parameters to Diematic regulator with the helo of a RS485/TCP IP converter
#refresh of attributes From regulator is done roughly every minute
#update request to the regulator are done within 10 s and trigger a whole read refresh
class Diematic3Panel:
    updateCallback = None
    modBusInterface = None
    availability = False
    _datetime = None
    type = None
    release = None
    extTemp = None
    temp = None
    targetTemp = None
    returnTemp = None
    waterPressure = None
    burnerPower = None
    smokeTemp = None
    fanSpeed = None
    ionizationCurrent = None
    burnerStatus = None
    pumpPower = None
    alarm = None
    hotWaterPump = None
    hotWaterTemp = None
    _hotWaterMode = None
    _hotWaterDayTargetTemp = None
    _hotWaterNightTargetTemp = None
    zoneATemp = None
    _zoneAMode = None
    zoneAPump = None
    _zoneADayTargetTemp = None
    _zoneANightTargetTemp = None
    _zoneAAntiiceTargetTemp = None
    zoneBTemp = None
    _zoneBMode = None
    zoneBPump = None
    _zoneBDayTargetTemp = None
    _zoneBNightTargetTemp = None
    _zoneBAntiiceTargetTemp = None
    run = None
    slaveTime = None
    loopThread = None
    _zoneBNightTempTarget = None
    _zoneBAntiiceTempTarget = None
    masterTime = None
    masterSlaveSynchro = None
    lastSynchroTimestamp = None

    def __init__(self, ip, port, regulator_address, boiler_timezone):
        #parameter refresh period

        REFRESH_PERIOD = 60

        #logger
        self.logger = logging.getLogger(__name__)

        #RS485 converter connexion parameter saving
        self.ip = ip
        self.port = port

        #regulator modbus address
        self.regulatorAddress = regulator_address

        #timezone
        self.boilerTimezone = boiler_timezone

        #state machine initialisation
        self.busStatus = DDModBusStatus.INIT

        #queue for generic register write request
        self.regUpdateRequest = queue.Queue()

        #queues for specific Mode register request
        self.zoneAModeUpdateRequest = queue.Queue()
        self.zoneBModeUpdateRequest = queue.Queue()
        self.hotWaterModeUpdateRequest = queue.Queue()

        #dictionnary used to save registers data read from the regulator
        self.registers = dict()

        #init values of functional attributes
        self.init_regulator()

        #period
        self.refreshPeriod = REFRESH_PERIOD

        #init refreshRequest flag
        self.refreshRequest = False

    def init_connection(self):
        #RS485 converter connexion init
        self.modBusInterface = DDModbus.DDModbus(self.ip,self.port)
        self.logger.warning('Init Link with Regulator')
        self.modBusInterface.clean()

    def init_attributes(self):
        #regulator attributes
        self.availability = False
        self._datetime = None
        self.type = None
        self.release = None
        self.extTemp = None
        self.temp = None
        self.targetTemp = None
        self.returnTemp = None
        self.waterPressure = None
        self.burnerPower = None
        self.smokeTemp = None
        self.fanSpeed = None
        self.ionizationCurrent = None
        self.burnerStatus = None
        self.pumpPower = None
        self.alarm = None
        self.hotWaterPump = None
        self.hotWaterTemp = None
        self._hotWaterMode = None
        self._hotWaterDayTargetTemp = None
        self._hotWaterNightTargetTemp = None
        self.zoneATemp = None
        self._zoneAMode = None
        self.zoneAPump = None
        self._zoneADayTargetTemp = None
        self._zoneANightTargetTemp = None
        self._zoneAAntiiceTargetTemp = None
        self.zoneBTemp = None
        self._zoneBMode = None
        self.zoneBPump = None
        self._zoneBDayTargetTemp = None
        self._zoneBNightTargetTemp = None
        self._zoneBAntiiceTargetTemp = None
        self.slaveTime = None
        self.loopThread = None

    def init_regulator(self):
        #RS485 converter connexion init
        self.init_connection()
        #Attributes init
        self.init_attributes()



#this setter/getter are used to read or change values of the regulator
    @property
    def hot_water_night_target_temp(self):
            return self._hotWaterNightTargetTemp

    @hot_water_night_target_temp.setter
    def hot_water_night_target_temp(self, x):
            #register structure creation, only 5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_ECS_NUIT.value,[min(max(round(x/5)*50,TEMP_MIN_ECS*10),TEMP_MAX_ECS*10)])
            self.regUpdateRequest.put(reg)

    @property
    def hot_water_day_target_temp(self):
            return self._hotWaterDayTargetTemp

    @hot_water_day_target_temp.setter
    def hot_water_day_target_temp(self, x):
            #register structure creation, only 5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_ECS.value,[min(max(round(x/5)*50,TEMP_MIN_ECS*10),TEMP_MAX_ECS*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_a_antiice_target_temp(self):
            return self._zoneAAntiiceTargetTemp

    @zone_a_antiice_target_temp.setter
    def zone_a_antiice_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_ANTIGEL_A.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_a_night_target_temp(self):
            return self._zoneANightTargetTemp

    @zone_a_night_target_temp.setter
    def zone_a_night_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_NUIT_A.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_a_day_target_temp(self):
            return self._zoneADayTargetTemp

    @zone_a_day_target_temp.setter
    def zone_a_day_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_JOUR_A.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_b_antiice_target_temp(self):
            return self._zoneBAntiiceTargetTemp

    @zone_b_antiice_target_temp.setter
    def zone_b_antiice_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_ANTIGEL_B.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_b_night_target_temp(self):
            return self._zoneBNightTargetTemp

    @zone_b_night_target_temp.setter
    def zone_b_night_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_NUIT_B.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_b_day_target_temp(self):
            return self._zoneBDayTargetTemp

    @zone_b_day_target_temp.setter
    def zone_b_day_target_temp(self, x):
            #register structure creation, only 0.5 multiple are usable, temp is in tenth of degree
            reg = DDModbus.RegisterSet(DDREGISTER.CONS_JOUR_B.value,[min(max(round(2*x)*5,TEMP_MIN_INT*10),TEMP_MAX_INT*10)])
            self.regUpdateRequest.put(reg)

    @property
    def zone_a_mode(self):
            return self._zoneAMode

    @zone_a_mode.setter
    def zone_a_mode(self, x):

        #request mode A register change depending mode requested
        self.logger.debug('zone A mode requested:'+str(x))
        if x =='AUTO':
            self.zoneAModeUpdateRequest.put(8)
        elif x =='TEMP JOUR':
            self.zoneAModeUpdateRequest.put(36)
        elif x =='TEMP NUIT':
            self.zoneAModeUpdateRequest.put(34)
        elif x =='PERM JOUR':
            self.zoneAModeUpdateRequest.put(4)
        elif x =='PERM NUIT':
            self.zoneAModeUpdateRequest.put(2)
        elif x =='ANTIGEL':
            self.zoneAModeUpdateRequest.put(1)
    @property
    def zone_b_mode(self):
            return self._zoneBMode

    @zone_b_mode.setter
    def zone_b_mode(self, x):

        #request mode B register change depending mode requested
        self.logger.debug('zone B mode requested:'+str(x))
        if x =='AUTO':
            self.zoneBModeUpdateRequest.put(8)
        elif x =='TEMP JOUR':
            self.zoneBModeUpdateRequest.put(36)
        elif x =='TEMP NUIT':
            self.zoneBModeUpdateRequest.put(34)
        elif x =='PERM JOUR':
            self.zoneBModeUpdateRequest.put(4)
        elif x =='PERM NUIT':
            self.zoneBModeUpdateRequest.put(2)
        elif x =='ANTIGEL':
            self.zoneBModeUpdateRequest.put(1)

    @property
    def hot_water_mode(self):
            return self._hotWaterMode

    @hot_water_mode.setter
    def hot_water_mode(self, x):

        #request hotwater mode register change depending mode requested
        self.logger.debug('hot water mode requested:'+str(x))
        if x == 'AUTO':
            self.hotWaterModeUpdateRequest.put(0)
        elif x == 'TEMP':
            self.hotWaterModeUpdateRequest.put(0x50)
        elif x == 'PERM':
            self.hotWaterModeUpdateRequest.put(0x10)

    @property
    def datetime(self):
            return self._datetime

    @datetime.setter
    def datetime(self,x):
        #switch time to boiler timezone
        x = x.astimezone(pytz.timezone(self.boilerTimezone))

        #request hour/minute/weekday registers change
        self.logger.debug('datetime requested:'+x.isoformat())
        reg = DDModbus.RegisterSet(DDREGISTER.HEURE.value,[x.hour,x.minute,x.isoweekday()])
        self.regUpdateRequest.put(reg)

        #request day/month/year registers change
        reg = DDModbus.RegisterSet(DDREGISTER.JOUR.value,[x.day,x.month,(x.year % 100)])
        self.regUpdateRequest.put(reg)

#this property is used to get register values from the regulator
    def refresh_registers(self):
        #update registers 1->63
        reg = self.modBusInterface.master_read_analog(self.regulatorAddress, 1, 63)
        if reg is not None:
            self.registers.update(reg)
        else:
            return False
        #update registers 64->127
        reg = self.modBusInterface.master_read_analog(self.regulatorAddress, 64, 64)
        if reg is not None:
            self.registers.update(reg)
        else:
            return False

        #update registers 128->191
        #reg = self.modBusInterface.masterReadAnalog(self.regulatorAddress,128,64)
        #if (reg is not None):
        #	self.registers.update(reg)
        #else:
        #	return(False)

        #update registers 191->255
        #reg = self.modBusInterface.masterReadAnalog(self.regulatorAddress,192,64)
        #if (reg is not None):
        #	self.registers.update(reg)
        #else:
        #	return(False)

        #update registers 384->447
        reg = self.modBusInterface.master_read_analog(self.regulatorAddress, 384, 64)
        if reg is not None:
            self.registers.update(reg)
        else:
            return False
        #update registers 448->470
        reg = self.modBusInterface.master_read_analog(self.regulatorAddress, 448, 23)
        if reg is not None:
            self.registers.update(reg)
        else:
            return False

        #display register table on standard output
        #regLine = ""
        #for index in range(256):
        #	try:
        #		regLine += '{:04X}'.format(self.registers[index])+' '
        #	except KeyError:
        #		regLine += '---- '

        #	if (index % 16) == 15:
        #		regLine = '{:01X}'.format(index >>4)+'0: '+regLine
        #		print(regLine)
        #		regLine = ''

        #print('==========================================')
        return True

#decoding property to decode Modbus encoded float values	
    def float10(self,reg):
        if reg == 0xFFFF:
            return None
        if reg >= 0x8000:
            reg =- (reg & 0x7FFF)
        return reg*0.1

#this property is used to refresh class functional attributes with data extracted from the regulator
    def refresh_attributes(self):
        FAN_SPEED_MAX = 5900

        #boiler
        self.availability = True
        self._datetime = datetime.datetime(self.registers[DDREGISTER.ANNEE]+2000,self.registers[DDREGISTER.MOIS],self.registers[DDREGISTER.JOUR],self.registers[DDREGISTER.HEURE],self.registers[DDREGISTER.MINUTE],0,0,pytz.timezone(self.boilerTimezone))
        self.type = self.registers[DDREGISTER.BOILER_TYPE]
        self.release = self.registers[DDREGISTER.CTRL]
        self.extTemp = self.float10(self.registers[DDREGISTER.TEMP_EXT])
        self.temp = self.float10(self.registers[DDREGISTER.TEMP_CHAUD])
        self.targetTemp = self.float10(self.registers[DDREGISTER.TCALC_A])
        self.returnTemp = self.float10(self.registers[DDREGISTER.RETURN_TEMP])
        self.waterPressure = self.float10(self.registers[DDREGISTER.PRESSION_EAU])
        self.smokeTemp = self.float10(self.registers[DDREGISTER.SMOKE_TEMP])
        self.ionizationCurrent = self.float10(self.registers[DDREGISTER.IONIZATION_CURRENT])
        self.fanSpeed = self.registers[DDREGISTER.FAN_SPEED]
        self.burnerStatus = (self.registers[DDREGISTER.BASE_ECS] & 0x08) >>3
        #burner power calculation with fans peed and ionization current
        self.burnerPower = round((self.registers[DDREGISTER.FAN_SPEED] / FAN_SPEED_MAX)*100) if (self.ionizationCurrent>0) else 0
        self.alarm = {'id':self.registers[DDREGISTER.ALARME], 'txt':None}
        if self.alarm['id'] == 0:
            self.alarm['txt'] = 'OK'
        elif self.alarm['id'] == 10:
            self.alarm['txt'] = 'Défaut Sonde Retour'
        elif self.alarm['id'] == 21:
            self.alarm['txt'] = 'Pression d\'eau basse'
        elif self.alarm['id'] == 26:
            self.alarm['txt'] = 'Défaut Allumage'
        elif self.alarm['id'] == 27:
            self.alarm['txt'] = 'Flamme Parasite'
        elif self.alarm['id'] == 28:
            self.alarm['txt'] = 'STB Chaudière'
        elif self.alarm['id'] == 30:
            self.alarm['txt'] = 'Rearm. Coffret'
        elif self.alarm['id'] == 31:
            self.alarm['txt'] = 'Défaut Sonde Fumée'
        else:
            self.alarm['txt']  =  'Défaut inconnu'

        #hotwater
        self.hotWaterPump = (self.registers[DDREGISTER.BASE_ECS] & 0x20) >>5
        self.hotWaterTemp = self.float10(self.registers[DDREGISTER.TEMP_ECS])
        if (self.registers[DDREGISTER.MODE_A] & 0x50) == 0:
            self._hotWaterMode = 'AUTO'
        elif (self.registers[DDREGISTER.MODE_A] & 0x50) == 0x50:
            self._hotWaterMode = 'TEMP'
        elif (self.registers[DDREGISTER.MODE_A] & 0x50) == 0x10:
            self._hotWaterMode = 'PERM'
        else:
            self._hotWaterMode = None
        self._hotWaterDayTargetTemp = self.float10(self.registers[DDREGISTER.CONS_ECS])
        self._hotWaterNightTargetTemp = self.float10(self.registers[DDREGISTER.CONS_ECS_NUIT])

        #Area A
        self.zoneATemp = self.float10(self.registers[DDREGISTER.TEMP_AMB_A])
        if self.zoneATemp is not None:
            mode_a = self.registers[DDREGISTER.MODE_A] & 0x2F

            if mode_a == 8:
                self._zoneAMode = 'AUTO'
            elif mode_a == 36:
                self._zoneAMode = 'TEMP JOUR'
            elif mode_a == 34:
                self._zoneAMode = 'TEMP NUIT'
            elif mode_a == 4:
                self._zoneAMode = 'PERM JOUR'
            elif mode_a == 2:
                self._zoneAMode = 'PERM NUIT'
            elif mode_a == 1:
                self._zoneAMode = 'ANTIGEL'
            self.zoneAPump = (self.registers[DDREGISTER.BASE_ECS] & 0x10) >>4
            self.pumpPower = self.registers[DDREGISTER.PUMP_POWER] if (self.zoneAPump == 1) else 0
            self._zoneADayTargetTemp = self.float10(self.registers[DDREGISTER.CONS_JOUR_A])
            self._zoneANightTargetTemp = self.float10(self.registers[DDREGISTER.CONS_NUIT_A])
            self._zoneAAntiiceTargetTemp = self.float10(self.registers[DDREGISTER.CONS_ANTIGEL_A])

        else:
            self._zoneAMode = None
            self.zoneAPump = None
            self._zoneADayTargetTemp = None
            self._zoneANightTargetTemp = None
            self._zoneAAntiiceTargetTemp = None


        #Area B
        self.zoneBTemp = self.float10(self.registers[DDREGISTER.TEMP_AMB_B])
        if self.zoneBTemp is not None:
            mode_b = self.registers[DDREGISTER.MODE_B]& 0x2F
            if mode_b == 8:
                self._zoneBMode = 'AUTO'
            elif mode_b == 36:
                self._zoneBMode ='TEMP JOUR'
            elif mode_b == 34:
                self._zoneBMode ='TEMP NUIT'
            elif mode_b == 4:
                self._zoneBMode ='PERM JOUR'
            elif mode_b == 2:
                self._zoneBMode ='PERM NUIT'
            elif mode_b == 1:
                self._zoneBMode ='ANTIGEL'

            self.zoneBPump =(self.registers[DDREGISTER.OPTIONS_B_C] & 0x10) >>4
            self._zoneBDayTargetTemp = self.float10(self.registers[DDREGISTER.CONS_JOUR_B])
            self._zoneBNightTempTarget = self.float10(self.registers[DDREGISTER.CONS_NUIT_B])
            self._zoneBAntiiceTempTarget = self.float10(self.registers[DDREGISTER.CONS_ANTIGEL_B])

        else:
            self._zoneBMode = None
            self.zoneBPump = None
            self._zoneBDayTargetTemp = None
            self._zoneBNightTempTarget = None
            self._zoneBAntiiceTempTarget  =  None

        self.updateCallback()


#this property is used by the Modbus loop to set register dedicated to Mode A and hotwater mode (in case of no usage of B area)		
    def mode_a_update(self):
        #if mode A register update request is pending
        if not(self.zoneAModeUpdateRequest.empty()) or (not(self.hotWaterModeUpdateRequest.empty()) and (self.zone_a_mode is None)):
            #get current mode
            current_mode = self.modBusInterface.master_read_analog(self.regulatorAddress, DDREGISTER.MODE_A.value, 1)
            #in case of success
            if current_mode:
                mode = current_mode[DDREGISTER.MODE_A]
                self.logger.info('Mode A current value :'+str(mode))

                #update mode with mode requests
                if not(self.zoneAModeUpdateRequest.empty()):
                    mode = (mode & 0x50) | self.zoneAModeUpdateRequest.get()

                if not(self.hotWaterModeUpdateRequest.empty()) and (self.zone_b_mode is None):
                    mode = (mode & 0x2F) | self.hotWaterModeUpdateRequest.get()

                self.logger.info('Mode A next value :'+str(mode))
                #specific case for antiice request
                #following write procedure is an empirical solution to have remote control refresh while updating mode
                if mode == 1:
                    #set antiice day number to 1
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [1])
                    time.sleep(0.5)
                    #set antiice day number to 0
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [0])
                    #set mode A number to requested value
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_A.value, [mode])

                #general case
                #following write procedure is an empirical solution to have remote control refresh while updating mode
                else:
                    #set mode A
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_A.value, [mode])
                    #set antiice day number to 1
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [1])
                    #set mode A again
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_A.value, [mode])
                    time.sleep(0.5)
                    #set mode A again
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_A.value, [mode])
                    #set antiice day number to 0
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [0])

                #request register refresh
                self.refreshRequest = True

#this property is used by the Modbus loop to set register dedicated to Mode B and hotwater mode (in case of usage of B area)					
    def mode_b_update(self):
        #if mode B register update request is pending
        if not(self.zoneBModeUpdateRequest.empty()) or (not(self.hotWaterModeUpdateRequest.empty()) and self.zone_b_mode):
            #get current mode
            current_mode = self.modBusInterface.master_read_analog(self.regulatorAddress, DDREGISTER.MODE_B.value, 1)
            #in case of success
            if current_mode:
                mode = current_mode[DDREGISTER.MODE_B]
                self.logger.info('Mode B current value :'+str(mode))

                #update mode with mode requests
                if not(self.zoneBModeUpdateRequest.empty()):
                    mode = (mode & 0x50) | self.zoneBModeUpdateRequest.get()

                if not(self.hotWaterModeUpdateRequest.empty()) and self.zone_b_mode:
                    mode = (mode & 0x2F) | self.hotWaterModeUpdateRequest.get()

                self.logger.info('Mode B next value :'+str(mode))
                #specific case for antiice request
                #following write procedure is an empirical solution to have remote control refresh while updating mode
                if mode == 1:
                    #set antiice day number to 1
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [1])
                    time.sleep(0.5)
                    #set antiice day number to 0
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [0])
                    #set mode B number to requested value
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_B.value, [mode])

                #general case
                #following write procedure is an empirical solution to have remote control refresh while updating mode
                else:
                    #set mode B
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_B.value, [mode])
                    #set antiice day number to 1
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [1])
                    #set mode B again
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_B.value, [mode])
                    time.sleep(0.5)
                    #set mode B again
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.MODE_B.value, [mode])
                    #set antiice day number to 0
                    self.modBusInterface.master_write_analog(self.regulatorAddress, DDREGISTER.NB_JOUR_ANTIGEL.value, [0])

                #request refresh
                self.refreshRequest = True

#modbus loop, shall run in a specific thread. Allow exchanging register values with the Diematic regulator
    def loop(self):
        #parameter validity duration in seconds after expiration of period
        VALIDITY_TIME = 30
        try:
            self.masterSlaveSynchro = False
            self.run = True
            #reset timeout
            self.lastSynchroTimestamp = time.time()
            while self.run:
                #wait for a frame received
                frame = self.modBusInterface.slave_rx()

                #depending current bus mode
                if self.busStatus != DDModBusStatus.SLAVE:
                    if frame:
                        #switch mode to slave
                        self.busStatus = DDModBusStatus.SLAVE
                        self.slaveTime = time.time()
                        self.logger.debug('Bus status switched to SLAVE')

                elif self.busStatus == DDModBusStatus.SLAVE:
                    slave_mode_duration = time.time()-self.slaveTime
                    #if no frame have been received and slave happen during at least 5s
                    if (not frame) and (slave_mode_duration>5):
                        #switch mode to MASTER
                        self.masterTime = time.time()
                        self.busStatus = DDModBusStatus.MASTER
                        self.logger.debug('Bus status switched to MASTER after '+str(slave_mode_duration))

                        #if the state wasn't still synchronised
                        if not self.masterSlaveSynchro:
                            self.logger.info('ModBus Master Slave Synchro OK')
                            self.masterSlaveSynchro = True

                        #mode A register update if needed
                        self.mode_a_update()

                        #mode B register update if needed
                        self.mode_b_update()

                        #while general register update request are pending and Master mode is started since less than 2s
                        while not(self.regUpdateRequest.empty()) and ((time.time()-self.masterTime) < 2):
                            reg_set = self.regUpdateRequest.get(False)
                            self.logger.debug('Write Request :'+str(reg_set.address)+':'+str(reg_set.data))
                            #write to Analog registers
                            if not self.modBusInterface.master_write_analog(self.regulatorAddress, reg_set.address, reg_set.data):
                                #And cancel Master Slave Synchro Flag in case of error
                                self.logger.warning('ModBus Master Slave Synchro Error')
                                self.masterSlaveSynchro = False
                            self.refreshRequest = True


                        #update registers, todo condition for refresh launch
                        if ((time.time()-self.lastSynchroTimestamp) > (self.refreshPeriod-5)) or self.refreshRequest:
                            if self.refresh_registers():
                                self.lastSynchroTimestamp = time.time()

                                #refresh regulator attribute
                                self.refresh_attributes()

                                #clear Flag
                                self.refreshRequest = False
                            else:
                                #Cancel Master Slave Synchro Flag in case of error
                                self.logger.warning('ModBus Master Slave Synchro Error')
                                self.masterSlaveSynchro = False

                if (time.time()-self.lastSynchroTimestamp) > self.refreshPeriod + VALIDITY_TIME:
                    #log
                    self.logger.warning('Synchro timeout')
                    #init regulator register
                    self.init_attributes()
                    #publish values
                    self.updateCallback()
                    #reinit connection
                    self.init_connection()
                    self.refreshRequest = True
                    #reset timeout
                    self.lastSynchroTimestamp = time.time()


            self.logger.critical('Modbus Thread stopped')
        except BaseException as exc:
            self.logger.exception(exc)

#property used to launch Modbus loop			
    def loop_start(self):
            #launch loop
            self.loopThread = threading.Thread(target=self.loop)
            self.loopThread.start()

#property used to stop Modbus loop	
    def loop_stop(self):
        self.run = False
        self.loopThread.join()
        #reinit Regulator
        self.init_attributes()
        self.updateCallback()
