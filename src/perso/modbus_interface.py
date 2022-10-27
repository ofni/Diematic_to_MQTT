#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from enum import IntEnum
from itertools import groupby
import logging.config
import logging
from operator import itemgetter
import queue
import threading
import time

from pymodbus.client.sync import ModbusSerialClient as ModbusClient


class DDREGISTER(IntEnum):

    # not real registers, it more a configuration
    DEFAULT_CONS_JOUR = 20
    DEFAULT_CONS_NUIT = DEFAULT_CONS_JOUR - 3
    DEFAULT_CONS_ANTIGEL = 13

    TEMP_MIN_INT = 5
    TEMP_MAX_INT = 30

    TEMP_MAX_ECS = 80
    TEMP_MIN_ECS = 10

    FAN_SPEED_MAX = 5900

    # TEMPO
    TYPE_CIRCUIT = 303
    PUISS_INST = 471
    PUISS_INST_MOY = 472

    # Diematic registers
    CTRL = 3
    HEURE = 4
    MINUTE = 5
    JOUR_SEMAINE = 6
    TEMP_EXT = 7
    TEMP_ETE_HIVER = 8
    ADAPTATION = 12
    NB_JOUR_ANTIGEL = 13

    CONS_JOUR_B = 23
    CONS_NUIT_B = 24
    CONS_ANTIGEL_B = 25
    MODE_B = 26
    TEMP_AMB_B = 27
    TCALC_B = 32

    CONS_JOUR_C = 35
    CONS_NUIT_C = 36
    CONS_ANTIGEL_C = 37
    MODE_C = 38
    TEMP_AMB_C = 39
    TCALC_C = 44

    CONS_ECS = 59
    TEMP_ECS = 62
    PERMUTATION = 63
    TEMP_CHAUD = 75
    BASE_ECS = 89  # 427
    OPTIONS_B_C = 90  # 428
    CONS_ECS_NUIT = 96
    JOUR = 108
    MOIS = 109
    ANNEE = 110
    FAN_SPEED = 307  # 455
    BOILER_TYPE = 308  # 457
    IONIZATION_CURRENT = 451
    RETURN_TEMP = 453
    SMOKE_TEMP = 454
    PRESSION_EAU = 456
    PUMP_POWER = 463
    ALARME = 465


class DienematicRegisters:

    registers = {
        DDREGISTER.PUISS_INST: {"name": "PUISS_INST", "value": None, "type": "integer", "system": "boiler"},
#        DDREGISTER.CTRL:            {"name": "CTRL", "value": None, "type": "decimal", "system": "boiler"},
        DDREGISTER.HEURE:           {"name": "HEURE", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.MINUTE:          {"name": "MINUTE", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.JOUR_SEMAINE:    {"name": "JOUR_SEMAINE", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.TEMP_EXT:        {"name": "TEMP_EXT", "value": None, "type": "decimal", "system": "boiler"},
#        DDREGISTER.TEMP_ETE_HIVER: {"name": "TEMP_E_H", "value": None, "type": "decimal", "system": "boiler"},
        DDREGISTER.NB_JOUR_ANTIGEL: {"name": "NB_JOUR_ANTIGEL", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.CONS_JOUR_B:     {"name": "CONS_JOUR_B", "value": None, "type": "decimal", "system": "circuit_B"},
        DDREGISTER.CONS_NUIT_B:     {"name": "CONS_NUIT_B", "value": None, "type": "decimal", "system": "circuit_B"},
        DDREGISTER.CONS_ANTIGEL_B:  {"name": "CONS_ANTIGEL_B", "value": None, "type": "decimal", "system": "circuit_B"},
        DDREGISTER.MODE_B:          {"name": "MODE_B_bits", "value": None, "type": "bits", "system": "circuit_B"},
        DDREGISTER.TEMP_AMB_B:      {"name": "TEMP_AMB_B", "value": None, "type": "decimal", "system": "circuit_B"},
        DDREGISTER.TCALC_B:         {"name": "TCALC_B", "value": None, "type": "decimal", "system": "circuit_B"},
        DDREGISTER.CONS_JOUR_C:     {"name": "CONS_JOUR_C", "value": None, "type": "decimal", "system": "circuit_C"},
        DDREGISTER.CONS_NUIT_C:     {"name": "CONS_NUIT_C", "value": None, "type": "decimal", "system": "circuit_C"},
        DDREGISTER.CONS_ANTIGEL_C:  {"name": "CONS_ANTIGEL_C", "value": None, "type": "decimal", "system": "circuit_C"},
        DDREGISTER.MODE_C:          {"name": "MODE_C_bits", "value": None, "type": "bits", "system": "circuit_C"},
        DDREGISTER.TEMP_AMB_C:      {"name": "TEMP_AMB_C", "value": None, "type": "decimal", "system": "circuit_C"},
        DDREGISTER.TCALC_C:         {"name": "TCALC_C", "value": None, "type": "decimal", "system": "circuit_C"},
        DDREGISTER.CONS_ECS:        {"name": "CONS_ECS_JOUR", "value": None, "type": "decimal", "system": "ECS"},
        DDREGISTER.TEMP_ECS:        {"name": "TEMP_ECS", "value": None, "type": "decimal", "system": "ECS"},
        DDREGISTER.BASE_ECS:        {"name": "BASE_ECS", "value": None, "type": "bits", "system": "ECS"},
        DDREGISTER.OPTIONS_B_C:     {"name": "OPTIONS_B_C", "value": None, "type": "bits", "system": "ECS"},
        DDREGISTER.CONS_ECS_NUIT:   {"name": "CONS_ECS_NUIT", "value": None, "type": "decimal", "system": "ECS"},
        DDREGISTER.JOUR:            {"name": "JOUR", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.MOIS:            {"name": "MOIS", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.ANNEE:           {"name": "ANNEE", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.FAN_SPEED:       {"name": "FAN_SPEED", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.BOILER_TYPE:     {"name": "BOILER_type", "value": None, "type": "integer", "system": "boiler"},
        DDREGISTER.IONIZATION_CURRENT: {"name": "IONIZATION_CURRENT", "value": None, "type": "decimal", "system": "boiler"},
#        DDREGISTER.RETURN_TEMP:     {"name": "RETURN_TEMP", "value": None, "type": "decimal", "system": "boiler"},
#        DDREGISTER.SMOKE_TEMP:      {"name": "SMOKE_TEMP", "value": None, "type": "decimal", "system": "boiler"},
        DDREGISTER.PRESSION_EAU:    {"name": "PRESSION_EAU", "value": None, "type": "decimal", "system": "boiler"},
#        DDREGISTER.PUMP_POWER:      {"name": "PUMP_POWER", "value": None, "type": "decimal", "system": "boiler"},
        DDREGISTER.ALARME:          {"name": "ALARME_raw", "value": None, "type": "integer", "system": "boiler"},
    }
    ranges = []

    def __init__(self):
        self.sort_registers()
        self.find_range()

    def find_range(self):
        data = [x for x in self.registers.keys()]
        for k, g in groupby(enumerate(data), lambda ix: ix[0] - ix[1]):
            self.ranges.append(list(map(itemgetter(1), g)))

    def sort_registers(self):
        self.registers = { x[0]:x[1] for x in sorted(self.registers.items(), key=lambda x: x[0])}

    def decode(self, register, value):
        register_type = self.registers[register]["type"]

        if register_type == "integer":
            return self._decode_integer(value)
        elif register_type == "decimal":
            return self._decode_decimal(value)
        elif register_type == "bits":
            return self._decode_bit(value)

    def _decode_decimal(self, value, decimals=1):
        if value == 65535:
            return None
        else:
            output = value & 0x7FFF
        if value >> 15 == 1:
            output = -output
        return float(output) / 10 ** decimals

    def _decode_integer(self, value):
        return value

    def _decode_bit(self, value):
        return list("{0:016b}".format(value))

    def decode_mode(self, reg):


        value = int('0b' + "".join(self.registers[reg]['value']), 2)

        if value == 8:
            mode = 'AUTO'
        elif value == 36:
            mode = 'TEMP JOUR'
        elif value == 34:
            mode = 'TEMP NUIT'
        elif value == 4:
            mode = 'PERM JOUR'
        elif value == 2:
            mode = 'PERM NUIT'
        elif value == 1:
            mode = 'ANTIGEL'

        return mode

    def decode_alarm(self):
        alarm = self.registers[DDREGISTER.ALARME]['value']
        if alarm == 0:
            return 'All OK'
        elif alarm == 10:
            return 'Défaut Sonde Retour'
        elif alarm == 21:
            return 'Pression d\'eau basse'
        elif alarm == 26:
            return 'Défaut Allumage'
        elif alarm == 27:
            return 'Flamme Parasite'
        elif alarm == 28:
            return 'STB Chaudière'
        elif alarm == 30:
            return 'Rearm. Coffret'
        elif alarm == 31:
            return 'Défaut Sonde Fumée'
        else:
            return 'Défaut inconnu'

    def set_value(self, register, value):
        self.registers[register]["value"] = self.decode(register, value)

    def dump_registers(self):
        """
        function to print registers values
        """
        for reg in self.get_registers():
            print(f'{reg["name"]} -> value: {reg["value"]}')

    def dump_raw_register(self, reg):
        """
        function to print registers values
        """
        reg = self.registers.get(reg, {"name": "NA", "value": "NA"})
        print(f'{reg["name"]} -> value: {reg["value"]}')

    def reset_values(self):
        """
        reset all register values to none
        and refill queue with all registers to get
        """
        for reg in self.registers.values():
            reg["value"] = None

    def get_registers(self):
        registers = list(self.registers.values())

        boiler_datetime = datetime(self.registers[110]['value'] + 2000, self.registers[109]['value'], self.registers[108]['value'], self.registers[4]['value'], self.registers[5]['value'], 0, 0)

        registers.append({"name": "status", "value": "Online", "type": "string", "system": "boiler"})
        registers.append({"name": "MODE_B", "value": self.decode_mode(DDREGISTER.MODE_B), "type": "decimal", "system": "circuit_B"})
        registers.append({"name": "MODE_C", "value": self.decode_mode(DDREGISTER.MODE_C), "type": "decimal", "system": "circuit_C"})
        registers.append({"name": "DATE", "value": boiler_datetime, "type": "string", "system": "boiler"})
        registers.append({"name": "ALARME", "value": self.decode_alarm(), "type": "integer", "system": "boiler"})

        #burnerStatus = (self.registers[DDREGISTER.BASE_ECS]['value'] & 0x08) >>3
        # #burner power calculation with fans peed and ionization current
        FAN_SPEED_MAX = 5900
        #burnerPower = round((self.registers[307]['value'] / FAN_SPEED_MAX)*100) if (self.registers[451]['value']>0) else 0

        return registers


class DiematicModbusInterface:

    register_to_read = queue.Queue()
    register_to_write = queue.Queue()

    modBusInterface = None
    run_loop = True
    busStatus = 'INIT'
    registers = DienematicRegisters()

    def __init__(self, publish_function, port='/dev/ttyUSB0', unit=0x0A):

        logging.config.fileConfig('logging.conf')
        self.logger = logging.getLogger(__name__)

        self.logger.critical(f'Modbus interface address: {port}')
        self.logger.critical(f'Modbus regulator address: {hex(unit)}')

        self.loopThread = None

        self.modBusInterface = ModbusClient(method='rtu', port=port, baudrate=9600)

        for range in self.registers.ranges:
            self.register_to_read.put(range)

        self.publisher = publish_function

        self.unit = unit

    def write_register(self, register, value):
        self.register_to_write.put({"register": register, "value": value})

    def reset_queue(self):
        for range in self.registers.ranges:
            self.register_to_read.put(range)

    def loop_start(self):
        # launch loop
        self.loopThread = threading.Thread(target=self.loop, name='modbus_interface')
        self.loopThread.start()

    def loop_stop(self):
        self.run_loop = False
        self.loopThread.join()

    def loop(self):

        while not (connected := self.modBusInterface.connect()):
            self.logger.critical('modbus interface not ready')
            connected = True

        while self.run_loop:
            #print('new loop !')
            # wait for a frame received
            self.modBusInterface.socket.timeout = 0.5
            frame = self.modBusInterface.recv(256)

            # depending current bus mode
            if self.busStatus != 'SLAVE' and frame:
                # switch mode to slave
                self.busStatus = "SLAVE"
                slave_time = time.time()
                #print('Bus status switched to SLAVE')

            elif self.busStatus == "SLAVE":
                slave_mode_duration = time.time() - slave_time
                if (not frame) and (slave_mode_duration > 5):
                    master_time = time.time()
                    self.busStatus = "MASTER"
                    #print(f'Bus status switched to MASTER after {str(slave_mode_duration)}')

                    current_time = time.time()
                    #print('empty', self.register_to_write.empty())
                    if not self.register_to_write.empty():
                        while (not self.register_to_write.empty()) and (current_time - master_time < 5):
                            current_time = time.time()

                            try:

                                reg = self.register_to_write.get(False)
                                res = self.modBusInterface.write_register(address=reg['register'], value=reg['value'], unit=0x0A)
                                print(res)
                                if res.isError():
                                    print("error while writing reg: {reg}")
                                    self.register_to_read.put(reg)
                            except Exception as e:
                                print(e)
                                break
                            #print(f"writing register {reg['register']} with value: {reg['value']}")

                    else:
                        while (not self.register_to_read.empty()) and (current_time - master_time < 5):
                            current_time = time.time()
                            #print('loop reading registers', current_time - master_time)

                            range = self.register_to_read.get()
                            range_min = range[0]
                            range_max = range[-1]
                            nb_range = range_max - range_min + 1

                            #print(f'  - getting registers: {range}')
                            res = self.modBusInterface.read_holding_registers(address=range_min, count=nb_range, unit=self.unit)
                            if res.isError():
                                #print('    * error: ', range, res)
                                self.register_to_read.put(range)
                            else:
                                #print('    * read OK')
                                for idx, reg in enumerate(range):
                                    self.registers.set_value(reg, res.registers[idx])

                        if self.register_to_read.empty():
                            #print('all registers read', current_time - master_time)
                            # self.registers.dump_registers()
                            self.registers.dump_raw_register([DDREGISTER.MODE_C, DDREGISTER.MODE_B])
                            self.publisher.send(self.registers.get_registers())
                            self.reset_queue()
