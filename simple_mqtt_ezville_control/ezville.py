import paho.mqtt.client as mqtt
import json
import time
import asyncio
import threading
import telnetlib
import socket

from threading import Thread
from queue import Queue

# DEVICE 별 패킷 정보
RS485_DEVICE = {
    "light": {
        "state":    { "id": "0E", "cmd": "81" },

        "power":    { "id": "0E", "cmd": "41", "ack": "C1" }
    },
    "thermostat": {
        "state":    { "id": "36", "cmd": "81" },

        "away":    { "id": "36", "cmd": "45", "ack": "00" },
        "target":   { "id": "36", "cmd": "44", "ack": "C4" }
    },
    "plug": {
        "state":    { "id": "50", "cmd": "81" },

        "power":    { "id": "50", "cmd": "43", "ack": "C3" }
    },
    "gasvalve": {
        "state":    { "id": "12", "cmd": "81" },

        "power":    { "id": "12", "cmd": "41", "ack": "C1" } # 잠그기만 가능
    },
    "batch": {
        "state":    { "id": "33", "cmd": "81" },

        "press":    { "id": "33", "cmd": "41", "ack": "C1" }
    }
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    "ids": ["ezville_wallpad",],
    "name": "ezville_wallpad",
    "mf": "EzVille",
    "mdl": "EzVille Wallpad",
    "sw": "ktdo79/addons/ezville_wallpad",
}

# MQTT Discovery를 위한 Payload 정보
DISCOVERY_PAYLOAD = {
    "light": [ {
        "_intg": "light",
        "~": "ezville/light_{:0>2d}_{:0>2d}",
        "name": "ezville_light_{:0>2d}_{:0>2d}",
        "opt": True,
        "stat_t": "~/power/state",
        "cmd_t": "~/power/command"
    } ],
    "thermostat": [ {
        "_intg": "climate",
        "~": "ezville/thermostat_{:0>2d}_{:0>2d}",
        "name": "ezville_thermostat_{:0>2d}_{:0>2d}",
        "mode_stat_t": "~/power/state",
        "temp_stat_t": "~/setTemp/state",
        "temp_cmd_t": "~/setTemp/command",
        "curr_temp_t": "~/curTemp/state",
        "away_mode_stat_t": "~/away/state",
        "away_mode_cmd_t": "~/away/command",
        "modes": [ "off", "heat" ],
        "min_temp": "5",
        "max_temp": 40
    } ],
    "plug": [ {
        "_intg": "switch",
        "~": "ezville/plug_{:0>2d}_{:0>2d}",
        "name": "ezville_plug_{:0>2d}_{:0>2d}",
        "stat_t": "~/power/state",
        "cmd_t": "~/power/command",
        "icon": "mdi:leaf"
    },
    {
        "_intg": "binary_sensor",
        "~": "ezville/plug_{:0>2d}_{:0>2d}",
        "name": "ezville_plug-automode_{:0>2d}_{:0>2d}",
        "stat_t": "~/auto/state",
        "icon": "mdi:leaf"
    },
    {
        "_intg": "sensor",
        "~": "ezville/plug_{:0>2d}_{:0>2d}",
        "name": "ezville_plug_{:0>2d}_{:0>2d}_powermeter",
        "stat_t": "~/current/state",
        "unit_of_meas": "W"
    } ],
    "gasvalve": [ {
        "_intg": "switch",
        "~": "ezville/gasvalve_{:0>2d}_{:0>2d}",
        "name": "ezville_gasvalve_{:0>2d}_{:0>2d}",
        "stat_t": "~/power/state",
        "cmd_t": "~/power/command",
        "icon": "mdi:valve"
    } ],
    "batch": [ {
        "_intg": "button",
        "~": "ezville/batch_{:0>2d}_{:0>2d}",
        "name": "ezville_batch-elevator-up_{:0>2d}_{:0>2d}",
        "cmd_t": "~/elevator-up/command",
        "icon": "mdi:elevator-up"
    },
    {
        "_intg": "button",
        "~": "ezville/batch_{:0>2d}_{:0>2d}",
        "name": "ezville_batch-elevator-down_{:0>2d}_{:0>2d}",
        "cmd_t": "~/elevator-down/command",
        "icon": "mdi:elevator-down"
    },
    {
        "_intg": "binary_sensor",
        "~": "ezville/batch_{:0>2d}_{:0>2d}",
        "name": "ezville_batch-groupcontrol_{:0>2d}_{:0>2d}",
        "stat_t": "~/group/state",
        "icon": "mdi:lightbulb-group"
    },
    {
        "_intg": "binary_sensor",
        "~": "ezville/batch_{:0>2d}_{:0>2d}",
        "name": "ezville_batch-outing_{:0>2d}_{:0>2d}",
        "stat_t": "~/outing/state",
        "icon": "mdi:home-circle"
    } ]
}

# STATE 확인용 Dictionary
STATE_HEADER = {
    prop['state']['id']: (device, prop['state']['cmd'])
    for device, prop in RS485_DEVICE.items()
    if 'state' in prop
}

ACK_HEADER = {
    prop[cmd]['id']: (device, prop[cmd]['ack'])
    for device, prop in RS485_DEVICE.items()
        for cmd, code in prop.items()
            if 'ack' in code
}

# LOG 메시지
def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    print('[{}] {}'.format(date, string))
    return

# CHECKSUM 및 ADD를 마지막 4 BYTE에 추가
def checksum(input_hex):
    try:
        input_hex = input_hex[:-4]
        
        # 문자열 bytearray로 변환
        packet = bytes.fromhex(input_hex)
        
        # checksum 생성
        checksum = 0
        for b in packet:
            checksum ^= b
        
        # add 생성
        add = (sum(packet) + checksum) & 0xFF 
        
        # checksum add 합쳐서 return
        return input_hex + format(checksum, '02X') + format(add, '02X')
    except:
        return None

    
config_dir = '/data'

HA_TOPIC = 'ezville'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
ELFIN_TOPIC = 'ew11'
ELFIN_SEND_TOPIC = ELFIN_TOPIC + '/send'


def ezville_loop(config):
    
    # config 
    debug = config['DEBUG']
    comm_mode = config['mode']
    mqtt_log = config['mqtt_log']
    elfin_log = config['elfin_log']
    
    # SOCKET 정보
    SOC_ADDRESS = config['elfin_server']
    SOC_PORT = config['elfin_port']
    
    # EW11 혹은 HA 전달 메시지 저장소
    MSG_QUEUE = Queue()
    # EW11에 보낼 Command 및 예상 Acknowledge 패킷 
    CMD_QUEUE = []
    
    # 기존 STATE 저장용 공간
    HOMESTATE = {}
    
    # 이전에 전달된 패킷인지 판단을 위한 캐쉬
    MSG_CACHE = {}
    
    # MQTT Discovery Que 및 모드 조절
    DISCOVERY_LIST = []
    DISCOVERY_MODE = True
    DISCOVERY_DURATION = 20
    
    # EW11 전달 패킷 중 처리 후 남은 짜투리 패킷 저장
    RESIDUE = ""
    
    # 강제 주기적 업데이트 설정 - 매 300초 마다 3초간 HA 업데이트 실시
    FORCE_UPDATE = False
    FORCE_PERIOD = 300
    FORCE_DURATION = 3
    
    # 현관스위치 요구 상태
    ELEVUP = ''
    ELEVDOWN = ''
    GROUPON = ''
    OUTING = ''
    
    # Command를 EW11로 보내는 방식 설정 (동시 명령 횟수, 명령 간격 및 재시도 횟수)
    CMD_COUNT = config['command_send_count']
    CMD_INTERVAL = config['command_interval']
    CMD_RETRY_COUNT = config['command_retry_count']
    
    # State 업데이트 루프와 Command 루프의 Delay Time 설정
    STATE_LOOP_DELAY = config['state_loop_delay']
    COMMAND_LOOP_DELAY = config['command_loop_delay']

    def on_connect(client, userdata, flags, rc):
        nonlocal comm_mode
        if rc == 0:
            log("Connected to MQTT broker..")
            if comm_mode == 'socket':
                client.subscribe(HA_TOPIC + '/#', 0)
            elif comm_mode == 'mixed':
                client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0)])
            else:
                client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0), (ELFIN_TOPIC + '/send', 1)])
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])
         
        
    def on_message(client, userdata, msg):
        nonlocal MSG_QUEUE
        MSG_QUEUE.put(msg)
            
    # MQTT message를 분류하여 처리
    async def process_message():
        # MSG_QUEUE의 message를 하나씩 pop
        nonlocal MSG_QUEUE
        stop = False
        while not stop:
            if MSG_QUEUE.empty():
                stop = True
            else:
                msg = MSG_QUEUE.get()
                topics = msg.topic.split('/')

                if topics[0] == HA_TOPIC and topics[-1] == 'command':
                    await HA_process(topics, msg.payload.decode('utf-8'))
                elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
                    await EW11_process(msg.payload.hex().upper())

                    
    # HA에서 전달된 메시지 처리        
    async def HA_process(topics, value):
        nonlocal CMD_QUEUE
        nonlocal ELEVUP, ELEVDOWN, GROUPON, OUTING
        device_info = topics[1].split('_')
        device = device_info[0]
        
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        if device in RS485_DEVICE:
            key = topics[1] + topics[2]
            idx = int(device_info[1])
            sid = int(device_info[2])
            cur_state = HOMESTATE.get(key)
            value = 'ON' if value == 'heat' else value.upper()
            if cur_state:
                if value == cur_state:
                    if debug:
                        log('[DEBUG] {} is already set: {}'.format(key, value))
                else:
                    if device == 'thermostat':                        
                        if topics[2] == 'away':
                            away = '01' if value == 'ON' else '00'
                            
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01' + away + '0000')
                            recvcmd = 'NULL'
                            
                            if sendcmd:
                                CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                if debug:
                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                                    
                        elif topics[2] == 'setTemp':
                            curTemp = HOMESTATE.get(topics[1] + 'curTemp')
                            setTemp = HOMESTATE.get(topics[1] + 'setTemp')
                            
                            value = int(float(value))
                            if value == int(setTemp):
                                if debug:
                                    log('[DEBUG] {} is already set: {}'.format(topics[1], value))
                            else:
                                setTemp = value
                                sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(setTemp) + '0000')
                                recvcmd = ['F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']]

                                if sendcmd:
                                    CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                    if debug:
                                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

#                    elif device == 'Fan':
#                        if topics[2] == 'power':
#                            sendcmd = DEVICE_LISTS[device][idx].get('command' + value)
#                            recvcmd = DEVICE_LISTS[device][idx].get('state' + value) if value == 'ON' else [
#                                DEVICE_LISTS[device][idx].get('state' + value)]
#                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                            if debug:
#                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
#                        elif topics[2] == 'speed':
#                            speed_list = ['LOW', 'MEDIUM', 'HIGH']
#                            if value in speed_list:
#                                index = speed_list.index(value)
#                                sendcmd = DEVICE_LISTS[device][idx]['CHANGE'][index]
#                                recvcmd = [DEVICE_LISTS[device][idx]['stateON'][index]]
#                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                                if debug:
#                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

                    elif device == 'light':                         
                        pwr = '01' if value == 'ON' else '00'
                        
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '030' + str(sid) + pwr + '000000')

                        if sendcmd:
                            recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                            CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        else:
                            if debug:
                                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))
                                
                    elif device == 'plug':                         
                        pwr = '01' if value == 'ON' else '00'

                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '020' + str(sid) + pwr + '0000')

                        if sendcmd:
                            recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                            CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        else:
                            if debug:
                                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))
                                
                    elif device == 'gasvalve':
                        # 가스 밸브는 ON 제어를 받지 않음
                        if value == 'OFF':
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '0' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '0100' + '0000')

                            if sendcmd:
                                recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                                CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                if debug:
                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                            else:
                                if debug:
                                    log('[DEBUG] There is no command for {}'.format('/'.join(topics)))
                                    
            else:
                if device == 'batch':
                    # 일괄 차단기는 4가지 모드로 조절
               
                    if topics[2] == 'elevator-up':
                        ELEVUP = '1'    
                    elif topics[2] == 'elevator-down':
                        ELEVDOWN = '1'
                    elif topics[2] == 'group':
                        GROUPON = '0'
                    elif topics[2] == 'outing':
                        OUTING = '1'
                            
                    CMD = "{:0>2X}".format(int('00' + ELEVDOWN + ELEVUP + '0' + GROUPON + OUTING + '0', 2))
                    
                    # 일괄 차단기는 state를 변경하여 제공해서 월패드에서 조작하도록 해야함
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['state']['id'] + '0' + str(idx) + RS485_DEVICE[device]['state']['cmd'] + '0300' + CMD + '000000')
                    recvcmd = 'NULL'
                    
                    if sendcmd:
                        CMD_QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                    else:
                        if debug:
                            log('[DEBUG] There is no command for {}'.format('/'.join(topics))) 
                else:
                    if debug:
                        log('[DEBUG] There is no command about {}'.format('/'.join(topics)))
        else:
            if debug:
                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))

    
    # EW11 전달된 메시지 처리
    async def EW11_process(raw_data):
        nonlocal DISCOVERY_MODE, DISCOVERY_LIST
        nonlocal RESIDUE
        nonlocal CMD_QUEUE
        nonlocal MSG_CACHE
        nonlocal FORCE_UPDATE
        nonlocal ELEVUP, ELEVDOWN, GROUPON, OUTING
        
        raw_data = RESIDUE + raw_data
        DISCOVERY = DISCOVERY_MODE
        
        if elfin_log:
            log('[SIGNAL] receved: {}'.format(raw_data))
        
        k = 0
        cors = []
        msg_length = len(raw_data)
        while k < msg_length:
            # F7로 시작하는 패턴을 패킷으로 분리
            if raw_data[k:k + 2] == "F7":
                # 남은 데이터가 최소 패킷 길이를 만족하지 못하면 RESIDUE에 저장 후 종료
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    # 남은 데이터가 예상되는 패킷 길이보다 짧으면 RESIDUE에 저장 후 종료
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
                        
                # 분리된 패킷이 Valid한 패킷인지 Checksum 확인                
                if packet != checksum(packet):
                    k+=1
                    continue
                else:
                    # STATE 패킷인지 우선 확인
                    if packet[2:4] in STATE_HEADER and (packet[6:8] in STATE_HEADER[packet[2:4]][1]): 
                        #                                or packet[6:8] == ACK_HEADER[packet[2:4]][1]):
                        # 현재 DISCOVERY MODE인 경우 패킷 정보 기반 장치 등록 실시
                        if DISCOVERY:
                            name = STATE_HEADER[packet[2:4]][0]                            
                            if name == 'light':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 light 갯수 + 1
                                slc = int(packet[8:10], 16) 
                                
                                for id in range(1, slc):
                                    discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, id)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload["~"] = payload["~"].format(rid, id)
                                        payload["name"] = payload["name"].format(rid, id)
                                   
                                        await mqtt_discovery(payload)                            
                                    else:
                                        onoff = 'ON' if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'
                                        
                                        await update_state(name, 'power', rid, id, onoff)
                                                                                    
                            elif name == 'thermostat':
                                # room 갯수
                                rc = int((int(packet[8:10], 16) - 5) / 2)
                                # room의 조절기 수 (현재 하나 뿐임)
                                src = 1
                                
                                for rid in range(1, rc + 1):
                                    discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, src)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload["~"] = payload["~"].format(rid, src)
                                        payload["name"] = payload["name"].format(rid, src)
                                   
                                        await mqtt_discovery(payload)   
                                    else:
                                        setT = packet[16 + 4 * rid:18 + 4 * rid]
                                        curT = packet[18 + 4 * rid:20 + 4 * rid]
                                        onoff = 'ON' if int(packet[12:14], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'
                                        awayonoff = 'ON' if int(packet[14:16], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'

                                        await update_state(name, 'power', rid, src, onoff)
                                        await update_state(name, 'away', rid, src, awayonoff)
                                        await update_temperature(name, rid, src, curT, setT)
                                        
                            elif name == 'plug':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 plug 갯수
                                spc = int(packet[10:12], 16) 
                                
                                for id in range(1, spc + 1):
                                    discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, id)

                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        for payload_template in DISCOVERY_PAYLOAD[name]:
                                            payload = payload_template.copy()
                                            payload["~"] = payload["~"].format(rid, id)
                                            payload["name"] = payload["name"].format(rid, id)
                                   
                                            await mqtt_discovery(payload)                            
                                    else:
                                        # BIT0: 대기전력 On/Off, BIT1: 자동모드 On/Off
                                        # 위와 같지만 일단 on-off 여부만 판단
                                        onoff = 'ON' if int(packet[7 + 6 * id], 16) > 0 else 'OFF'
                                        autoonoff = 'ON' if int(packet[6 + 6 * id], 16) > 0 else 'OFF'
                                        power_num = "{:.2f}".format(int(packet[8 + 6 * id: 12 + 6 * id], 16) / 100)
                                        
                                        await update_state(name, 'power', rid, id, onoff)
                                        await update_state(name, 'auto', rid, id, onoff)
                                        await update_state(name, 'current', rid, id, power_num)
                                        
                            elif name == 'gasvalve':
                                # Gas Value는 하나라서 강제 설정
                                rid = 1
                                # Gas Value는 하나라서 강제 설정
                                spc = 1 
                                
                                discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, spc)
                                    
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    payload = DISCOVERY_PAYLOAD[name][0].copy()
                                    payload["~"] = payload["~"].format(rid, spc)
                                    payload["name"] = payload["name"].format(rid, spc)
                                   
                                    await mqtt_discovery(payload)                            
                                else:
                                    onoff = 'ON' if int(packet[12:14], 16) == 1 else 'OFF'
                                        
                                    await update_state(name, 'power', rid, spc, onoff)
                                    
                            elif name == 'batch':
                                # 일괄차단기는 하나라서 강제 설정
                                rid = 1
                                # 일괄차단기는 하나라서 강제 설정
                                sbc = 1
                                
                                discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, sbc)
                                
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    for payload_template in DISCOVERY_PAYLOAD[name]:
                                        payload = payload_template.copy()
                                        payload["~"] = payload["~"].format(rid, sbc)
                                        payload["name"] = payload["name"].format(rid, sbc)
                                   
                                        await mqtt_discovery(payload)        
                                else:
                                    # 일괄 차단기는 버튼 상태 변수 업데이트
                                    states = bin(int(packet[12:14], 16))[2:].zfill(8)
                                        
                                    ELEVDOWN = states[5]                                        
                                    ELEVUP = states[4]
                                    GROUPON = states[2]
                                    OUTING = states[1]
                                    
                                    grouponoff = 'ON' if GROUPON == '1' else 'OFF'
                                    outingonoff = 'ON' if OUTING == '1' else 'OFF'
                                    
                                    # 스위치 구성은 업데이트
                                    await update_state(name, 'group', rid, sbc, grouponoff)
                                    await update_state(name, 'outing', rid, sbc, outingonoff)
                                                                                    
                        # DISCOVERY_MODE가 아닌 경우 상태 업데이트만 실시
                        else:
                            # 앞서 보낸 명령에 대한 Acknowledge 인 경우 CMD_QUEUE에서 해당 명령 삭제
                            if packet[6:8] == ACK_HEADER[packet[2:4]][1]:
                                for que in CMD_QUEUE:
                                    if packet[0:8] or 'NULL' in que['recvcmd']:
                                        CMD_QUEUE.remove(que)
                                        if debug:
                                            log('[DEBUG] Found matched hex: {}. Delete a queue: {}'.format(raw_data, que))
                                        break
                            
                            # MSG_CACHE에 없는 새로운 패킷이거나 FORCE_UPDATE 실행된 경우만 실행
                            if MSG_CACHE.get(packet[0:10]) != packet[10:] or FORCE_UPDATE:
                                name = STATE_HEADER[packet[2:4]][0]                             
                  
                                if name == 'light':
                                    # ROOM ID
                                    rid = int(packet[5], 16)
                                    # ROOM의 light 갯수 + 1
                                    slc = int(packet[8:10], 16) 
                                    
                                    for id in range(1, slc):
                                        onoff = 'ON' if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'
                                        
                                        await update_state(name, 'power', rid, id, onoff)
                                        
                                        # 한번 처리한 패턴은 CACHE 저장
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                    
                                elif name == 'thermostat':
                                    # room 갯수
                                    rc = int((int(packet[8:10], 16) - 5) / 2)
                                    # room의 조절기 수 (현재 하나 뿐임)
                                    src = 1
                                
                                    for rid in range(1, rc + 1):
                                        setT = packet[16 + 4 * rid:18 + 4 * rid]
                                        curT = packet[18 + 4 * rid:20 + 4 * rid]
                                        onoff = 'ON' if int(packet[12:14], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'
                                        awayonoff = 'ON' if int(packet[14:16], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'
                                    
                                        await update_state(name, 'power', rid, src, onoff)
                                        await update_state(name, 'away', rid, src, awayonoff)
                                        await update_temperature(name, rid, src, curT, setT)
                                        
                                        # 한번 처리한 패턴은 CACHE 저장
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                        
                                elif name == 'plug':
                                    # ROOM ID
                                    rid = int(packet[5], 16)
                                    # ROOM의 plug 갯수
                                    spc = int(packet[10:12], 16) 
                                
                                    for id in range(1, spc + 1):
                                        # BIT0: 대기전력 On/Off, BIT1: 자동모드 On/Off
                                        # 위와 같지만 일단 on-off 여부만 판단
                                        onoff = 'ON' if int(packet[7 + 6 * id], 16) > 0 else 'OFF'
                                        autoonoff = 'ON' if int(packet[6 + 6 * id], 16) > 0 else 'OFF'
                                        power_num = "{:.2f}".format(int(packet[8 + 6 * id: 12 + 6 * id], 16) / 100)
                                        
                                        await update_state(name, 'power', rid, id, onoff)
                                        await update_state(name, 'auto', rid, id, onoff)
                                        await update_state(name, 'current', rid, id, power_num)
                                        
                                elif name == 'gasvalve':
                                    # Gas Value는 하나라서 강제 설정
                                    rid = 1
                                    # Gas Value는 하나라서 강제 설정
                                    spc = 1 
                                    
                                    onoff = 'ON' if int(packet[12:14], 16) == 1 else 'OFF'
                                        
                                    await update_state(name, 'power', rid, spc, onoff)
                                    
                                elif name == 'batch':
                                    # 일괄차단기는 하나라서 강제 설정
                                    rid = 1
                                    # 일괄차단기는 하나라서 강제 설정
                                    sbc = 1
                                    
                                    # 일괄 차단기는 버튼 상태 변수 업데이트
                                    states = bin(int(packet[12:14], 16))[2:].zfill(8)
                                    
                                    ELEVDOWN = states[5]                                        
                                    ELEVUP = states[4]
                                    GROUPON = states[2]
                                    OUTING = states[1]
                                    
                                    grouponoff = 'ON' if GROUPON == '0' else 'OFF'
                                    outingonoff = 'ON' if OUTING == '1' else 'OFF'
                                    
                                    # 스위치 구성은 업데이트
                                    await update_state(name, 'group', rid, sbc, grouponoff)
                                    await update_state(name, 'outing', rid, sbc, outingonoff)
                       
                RESIDUE = ""
                k = k + packet_length
            else:
                k+=1


    async def mqtt_discovery(payload):
        intg = payload.pop("_intg")

        # MQTT 통합구성요소에 등록되기 위한 추가 내용
        payload["device"] = DISCOVERY_DEVICE
        payload["uniq_id"] = payload["name"]

        # Discovery에 등록
        topic = "homeassistant/{}/ezville_wallpad/{}/config".format(intg, payload["name"])
        log("Add new device:  {}".format(topic))
        mqtt_client.publish(topic, json.dumps(payload))

                                                                                    
    async def update_state(device, state, id1, id2, onoff):
        nonlocal HOMESTATE
        nonlocal FORCE_UPDATE
        deviceID = "{}_{:0>2d}_{:0>2d}".format(device, id1, id2)
        key = deviceID + state

        if onoff != HOMESTATE.get(key) or FORCE_UPDATE:
            HOMESTATE[key] = onoff
            topic = STATE_TOPIC.format(deviceID, state)
            mqtt_client.publish(topic, onoff.encode())
                    
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, onoff))
        else:
            if debug:
                log('[DEBUG] {} is already set: {}'.format(deviceID, onoff))
        return
    
    async def update_temperature(device, id1, id2, curTemp, setTemp):
        nonlocal HOMESTATE
        nonlocal FORCE_UPDATE
        deviceID = "{}_{:0>2d}_{:0>2d}".format(device, id1, id2)
        temperature = {'curTemp': "{}".format(str(int(curTemp, 16))), 'setTemp': "{}".format(str(int(setTemp, 16)))}
        for state in temperature:
            key = deviceID + state
            val = temperature[state]
            if val != HOMESTATE.get(key) or FORCE_UPDATE:
                HOMESTATE[key] = val
                topic = STATE_TOPIC.format(deviceID, state)
                mqtt_client.publish(topic, val.encode())
                
                if mqtt_log:
                    log('[LOG] ->> HA : {} -> {}'.format(topic, val))
            else:
                if debug:
                    log('[DEBUG] {} is already set: {}'.format(key, val))
        return  
                                                                                    
                                                                                    
    async def send_to_elfin():
        nonlocal comm_mode, soc
        nonlocal CMD_QUEUE
        nonlocal DISCOVERY_MODE
        nonlocal CMD_COUNT
        nonlocal CMD_INTERVAL
        nonlocal CMD_RETRY_COUNT
                                                                                             
        while not DISCOVERY_MODE:
            try:
#                if time.time_ns() - COLLECTDATA['LastRecv'] > 10000000000:  # 10s
#                if time.time_ns() - COLLECTDATA['LastRecv'] > 100000000000: 
#                    log(str(COLLECTDATA['LastRecv']) + "  :  " + str(time.time_ns()))
#                    log('[WARNING] 10초간 신호를 받지 못했습니다. ew11 기기를 재시작합니다.')
#                    try:
#                        elfin_id = config['elfin_id']
#                        elfin_password = config['elfin_password']
#                        elfin_server = config['elfin_server']

#                        ew11 = telnetlib.Telnet(elfin_server)

#                        ew11.read_until(b"login:")
#                        ew11.write(elfin_id.encode('utf-8') + b'\n')
#                        ew11.read_until(b"password:")
#                        ew11.write(elfin_password.encode('utf-8') + b'\n')
#                        ew11.write('Restart'.encode('utf-8') + b'\n')

#                        await asyncio.sleep(10)
#                    except:
#                        log('[WARNING] 기기 재시작 오류! 기기 상태를 확인하세요.')
#                    COLLECTDATA['LastRecv'] = time.time_ns()
#                elif time.time_ns() - COLLECTDATA['LastRecv'] > 100000000:
                    if CMD_QUEUE:
                        send_data = CMD_QUEUE.pop(0)
                        if elfin_log:
                            log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        
                        for i in range(CMD_COUNT):
                            if comm_mode == 'mqtt':
                                mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
                            else:
                                try:
                                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
                                except OSError:
                                    connect_socket(soc)
                                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
                        log(str(time.time()))
                        await asyncio.sleep(CMD_INTERVAL)

                        if send_data['count'] < CMD_RETRY_COUNT:
                            send_data['count'] = send_data['count'] + 1
                            #CMD_QUEUE.append(send_data)
                            CMD_QUEUE.insert(0, send_data)
                        else:
                            if elfin_log:
                                log('[SIGNAL] {}회 명령을 재전송하였으나 수행에 실패했습니다.. 다음의 Queue 삭제: {}'.format(str(CMD_RETRY_COUNT),send_data))
                    return
            except Exception as err:
                log('[ERROR] send_to_elfin(): {}'.format(err))
                return
                                                                                    
                                                                                    
    # MQTT 통신 시작
    mqtt_client = mqtt.Client('mqtt-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    mqtt_client.loop_start()
    
    def initiate_socket():
        # SOCKET 통신 시작
            log('Socket 연결을 시작합니다')
            
            retry_count = 0
            while True:
                try:
                    soc = socket.socket()
                    soc.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    connect_socket(soc)
                    return soc
                except ConnectionRefusedError as e:
                    log('Server에서 연결을 거부합니다. 재시도 예정 (' + str(retry_count) + '회 재시도)')
                    time.sleep(1)
                    retry_count += 1
                    continue
                    
    def connect_socket(socket):
        nonlocal SOC_ADDRESS
        nonlocal SOC_PORT
        socket.connect((SOC_ADDRESS, SOC_PORT))
        
    if comm_mode == 'mixed' or comm_mode == 'socket':
        soc = initiate_socket()  

    # Discovery 및 강제 업데이트 시간 설정
    target_time = time.time() + DISCOVERY_DURATION
    force_target_time = target_time + FORCE_PERIOD
    force_stop_time = force_target_time + FORCE_DURATION
    
    log('장치 등록을 시작합니다...')
    
    async def recv_from_elfin():
        nonlocal soc
        nonlocal MSG_QUEUE
        
        class MSG:
            topic = ''
            payload = bytearray()
        
        msg = MSG()

        # EW11 버퍼 크기만큼 데이터 받기
        DATA = soc.recv(64)
        msg.topic = ELFIN_TOPIC + '/recv'
        msg.payload = DATA
        
        MSG_QUEUE.put(msg)

        
    async def state_update_run():
        nonlocal target_time, force_target_time, force_stop_time
        nonlocal comm_mode
        nonlocal DISCOVERY_MODE
        nonlocal FORCE_PERIOD
        nonlocal FORCE_DURATION
        nonlocal FORCE_UPDATE
        nonlocal STATE_LOOP_DELAY
        
        while True:
            if comm_mode == 'socket':
                await asyncio.gather(
                    recv_from_elfin(),                    
                    process_message(),
                    send_to_elfin()
                )
            else:
                await asyncio.gather(
                    process_message(),
                    send_to_elfin()               
                )           
            
            timestamp = time.time()
            if timestamp > target_time and DISCOVERY_MODE:
                DISCOVERY_MODE = False
                log('IOT 제어 입력 수행을 시작합니다...')
            
            # 정해진 시간이 지나면 FORCE 모드 발동
            if timestamp > force_target_time and not FORCE_UPDATE:
                force_stop_time = timestamp + FORCE_DURATION
                FORCE_UPDATE = True
                
            # 정해진 시간이 지나면 FORCE 모드 종료    
            if timestamp > force_stop_time and FORCE_UPDATE:
                force_target_time = timestamp + FORCE_PERIOD
                FORCE_UPDATE = False
                
            # 0.02초 대기 후 루프 진행
            await asyncio.sleep(0.02)
            
    async def command_run():
        nonlocal COMMAND_LOOP_DELAY
        
        while True:
            send_to_elfin()               
        
            # 0.001초 대기 후 루프 진행
            await asyncio.sleep(0.001)     
        
    th1 = Thread(target = asyncio.run(state_update_run()))
    th2 = Thread(target = asyncio.run(command_run()))
    th1.start()
    th2.start()
        
#    async def main_run():
#        nonlocal target_time, force_target_time, force_stop_time
#        nonlocal comm_mode
#        nonlocal DISCOVERY_MODE
#        nonlocal FORCE_PERIOD
#        nonlocal FORCE_DURATION
#        nonlocal FORCE_UPDATE
#        nonlocal LOOP_TIME
#        
#        while True:
#            if comm_mode == 'socket':
#                await asyncio.gather(
#                    recv_from_elfin(),                    
#                    process_message(),
#                    send_to_elfin()
#                )
#            else:
#                await asyncio.gather(
#                    process_message(),
#                    send_to_elfin()               
#                )           
#            
#            timestamp = time.time()
#            if timestamp > target_time and DISCOVERY_MODE:
#                DISCOVERY_MODE = False
#                log('IOT 제어 입력 수행을 시작합니다...')
#            
#            # 정해진 시간이 지나면 FORCE 모드 발동
#            if timestamp > force_target_time and not FORCE_UPDATE:
#                force_stop_time = timestamp + FORCE_DURATION
#                FORCE_UPDATE = True
#                
#            # 정해진 시간이 지나면 FORCE 모드 종료    
#            if timestamp > force_stop_time and FORCE_UPDATE:
#                force_target_time = timestamp + FORCE_PERIOD
#                FORCE_UPDATE = False
#                
#            # 0.02초 대기 후 루프 진행
#            await asyncio.sleep(0.001)
#                                                                                 
#    asyncio.run(main_run())

if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)
    
    ezville_loop(CONFIG)
