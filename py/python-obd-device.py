#!/usr/bin/python

import obd
from obd import OBDStatus

from paho.mqtt import client as mqtt_client
import json
import time

class mqtt_handle:
    def __init__(self, broker, port, topic, client_id, user, password):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client_id = client_id
        self.user = user
        self.password = password

    def connect(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connect OK!")
                client.subscribe("$SYS/#")
            else:
                print(f"Connect fail! RC: {rc}")
            
        self.client = mqtt_client.Client(self.client_id)
        self.client.username_pw_set(self.user, self.password)
        self.client.on_connect = on_connect
        try:
            self.client.loop_start()
            self.client.connect(self.broker, self.port, 60)
        except:
            print("Rede tem problemas!")

    def publish(self, data):
        result = self.client.publish(self.topic, data)
        status = result[0]
        if status == 0:
            print(f"Msg enviada ao topico `{self.topic}`")
        else:
            print(f"Falha ao enviar msg no topico `{self.topic}`")
            
class dados:
    def __init__(self):
        self.timestamp = 0
        self.RPM = 0
        self.VELOCIDADE = 0.0
        self.PEDAL_ACELERADOR = 0.0
        self.TEMPERATURA_INTAKE = 0
        self.TEMPERATURA_ARREFECIMENTO = 0
        self.MIL = False
        self.FREEZE_FRAME = None
        
    def toJson(self):
        out = {
            "timestamp": self.timestamp,
            "RPM" : self.RPM,
            "SPD" : self.VELOCIDADE,
            "PDL" : self.PEDAL_ACELERADOR,
            "INT" : self.TEMPERATURA_INTAKE,
            "COL" : self.TEMPERATURA_ARREFECIMENTO,
            "TRB" : self.MIL,
            "ERR" : self.FREEZE_FRAME
        }
        return json.dumps(out, indent = 4)
        
class freeze_frame:
    def __init__(self):
        self.DTC = None
        self.LISTA_DTC = None
        self.RPM = 0
        self.VELOCIDADE = 0.0
        self.PEDAL_ACELERADOR = 0.0
        self.TEMPERATURA_INTAKE = 0.0
        self.TEMPERATURA_ARREFECIMENTO = 0.0
    
# Para criar o cliente mqtt:
# hostname, porta, topico, client_id, usuario, senha
cliente_mqtt = mqtt_handle("XXXXXXXXXXX", 1883, "XXXX/XXXXX", "XXXXXXXXXXX", "XXXXXXXXXXX", "XXXXXXXXXXX")
cliente_mqtt.connect()

#porta OBD
porta = "XXXXXXXXXXX"
con = obd.OBD(porta)
while True:
    print(con.status())
    if con.status() == OBDStatus.CAR_CONNECTED:
        data = dados()
        print("Coletando dados...")
        
        try:
            rpm = con.query(obd.commands.RPM)
            speed = con.query(obd.commands.SPEED)
            pedal = con.query(obd.commands.THROTTLE_POS)
            trouble = con.query(obd.commands.STATUS)
            coolant = con.query(obd.commands.COOLANT_TEMP)
            intake = con.query(obd.commands.INTAKE_TEMP)
        except:
            print("FAIL RECV DATA")
            con.close()
            con = obd.OBD(porta)
            continue
        
        if not rpm.is_null():
            data.RPM = rpm.value.magnitude
        else:
            con.close()
            con = obd.OBD(porta)
            continue
        
        if not speed.is_null():
            data.VELOCIDADE = speed.value.magnitude

        if not pedal.is_null():
            data.PEDAL_ACELERADOR = pedal.value.magnitude

        if not trouble.is_null():
            data.MIL = trouble.value.MIL

        if not coolant.is_null():
            data.TEMPERATURA_ARREFECIMENTO = coolant.value.magnitude

        if not intake.is_null():
            data.TEMPERATURA_INTAKE = intake.value.magnitude

        
        if data.MIL == True:
            frame = freeze_frame()
            try:
                errors = con.query(obd.commands.GET_DTC)
                if not errors.is_null():
                    frame.LISTA_DTC = errors.value
                    errors = con.query(obd.commands.FREEZE_DTC)
                    frame.DTC = errors.value
                    errors = con.query(obd.commands.DTC_RPM)
                    frame.RPM = errors.value.magnitude
                    errors = con.query(obd.commands.DTC_SPEED)
                    frame.VELOCIDADE = errors.value.magnitude
                    errors = con.query(obd.commands.DTC_THROTTLE_POS)
                    frame.PEDAL_ACELERADOR = errors.value.magnitude
                    errors = con.query(obd.commands.DTC_INTAKE_TEMP)
                    frame.TEMPERATURA_INTAKE = errors.value.magnitude
                    errors = con.query(obd.commands.DTC_COOLANT_TEMP)
                    frame.TEMPERATURA_ARREFECIMENTO = errors.value.magnitude
                    data.FREEZE_FRAME = frame
            except:
                print("FAIL RECV DATA")
                con.close()
                con = obd.OBD(porta)
                continue

        data.timestamp = int(time.time())
        
        try:
            cliente_mqtt.publish(data.toJson())
        except:
            print("FAIL SEND")
    else:
            print("FAIL CONNECT CAR")
            con.close()
            con = obd.OBD(porta)
    time.sleep(2)
con.close()


