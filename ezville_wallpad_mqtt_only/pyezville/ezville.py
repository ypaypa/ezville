import paho.mqtt.client as mqtt
import json
import time
import asyncio
import telnetlib
from queue import Queue

share_dir = '/share'
config_dir = '/data'
data_dir = '/pyezville'

HA_TOPIC = 'homenet'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
ELFIN_TOPIC = 'ew11'
ELFIN_SEND_TOPIC = ELFIN_TOPIC + '/send'
RESIDUE = ""
msg_queue = Queue()
start_flag = False
#msg_queue = asyncio.Queue()

##################################################################
# Device 정보 여기에 추가

RS485_DEVICE = {
    "light": {
        "state":    { "id": "0E", "cmd": "81", },

        "power":    { "id": "0E", "cmd": "41", "ack": "C1", },
    },
    "thermostat": {
        "state":    { "id": "36", "cmd": "81", },

        "away":    { "id": "36", "cmd": "45", "ack": "00", },
        "target":   { "id": "36", "cmd": "44", "ack": "C4", },
    },
}

DISCOVERY_DEVICE = {
    "ids": ["ezville_wallpad",],
    "name": "ezville_wallpad",
    "mf": "EzVille",
    "mdl": "EzVille Wallpad",
    "sw": "ktdo79/ha_addons/ezville_wallpad",
}
    
DISCOVERY_PAYLOAD = {
    "light": [ {
        "_intg": "light",
        "~": "homenet/light{}",
        "name": "homenet_light{}",
        "opt": True,
        "stat_t": "~/power/state",
        "cmd_t": "~/power/command",
    } ],
    "thermostat": [ {
        "_intg": "climate",
        "~": "homenet/thermostat{}",
        "name": "homenet_thermostat{}",
        "mode_stat_t": "~/power/state",
        "temp_stat_t": "~/setTemp/state",
        "temp_cmd_t": "~/setTemp/command",
        "curr_temp_t": "~/curTemp/state",
        "away_mode_stat_t": "~/away/state",
        "away_mode_cmd_t": "~/away/command",
        "modes": [ "off", "heat" ],
        "min_temp": "5",
        "max_temp": 40,
    } ],
}

STATE_HEADER = {
    prop["state"]["id"]: (device, prop["state"]["cmd"])
    for device, prop in RS485_DEVICE.items()
    if "state" in prop
}

ACK_HEADER = {
    prop[cmd]["id"]: (device, prop[cmd]["ack"])
    for device, prop in RS485_DEVICE.items()
        for cmd, code in prop.items()
            if "ack" in code
}
            
device_num = {STATE_HEADER[prefix][0]: 0 for prefix in STATE_HEADER}
device_subnum = {STATE_HEADER[prefix][0]: {} for prefix in STATE_HEADER}
collect_data = {STATE_HEADER[prefix][0]: set() for prefix in STATE_HEADER}

##################################################################


def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    print('[{}] {}'.format(date, string))
    return


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


def find_device(config):
    #with open(data_dir + '/ezville_devinfo.json') as file:
    #    dev_info = json.load(file)
    #statePrefix = {dev_info[name]['stateON'][:2]: name for name in dev_info if dev_info[name].get('stateON')}
    #device_num = {statePrefix[prefix]: 0 for prefix in statePrefix}
    #collect_data = {statePrefix[prefix]: set() for prefix in statePrefix}
#    device_num = {STATE_HEADER[prefix][0]: 0 for prefix in STATE_HEADER}
#    device_subnum = {STATE_HEADER[prefix][0]: {} for prefix in STATE_HEADER}
#    collect_data = {STATE_HEADER[prefix][0]: set() for prefix in STATE_HEADER}

    target_time = time.time() + 20

    def on_connect(client, userdata, flags, rc):
        userdata = time.time() + 20
        if rc == 0:
            log("Connected to MQTT broker..")
            log("Find devices for 20s..")
            client.subscribe('ew11/#', 0)
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])

    def on_message(client, userdata, msg):
        raw_data = msg.payload.hex().upper()
        global RESIDUE
        raw_data = RESIDUE + raw_data
        
        k = 0
        msg_length = len(raw_data)
        while k < msg_length:
            if raw_data[k:k + 2] == "F7":
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
                
                if packet != checksum(packet):
                    k+=1
                    continue
                else:
                    if packet[2:4] in STATE_HEADER and packet[6:8] in STATE_HEADER[packet[2:4]]:
                        name = STATE_HEADER[packet[2:4]][0]
                        collect_data[name].add(packet)
                            
                        if name == 'light':
                            lc = int(packet[5], 16)
                            device_num[name] = max([device_num[name], lc])
                            device_subnum[name][lc] =  int(packet[8:10], 16) - 1                             
                                                          
                        elif name == 'thermostat':
                            device_num[name] = max([device_num[name], int((int(packet[8:10], 16) - 5) / 2)])                
                
                RESIDUE = ""
                k = k + packet_length
            else:
                k+=1
            
        #for k in range(0, len(raw_data), 16):
        #    data = raw_data[k:k + 16]
        #    # log(data)
        #    if data == checksum(data) and data[:2] in statePrefix:
        #        name = statePrefix[data[:2]]
        #        collect_data[name].add(data)
        #        if dev_info[name].get('stateNUM'):
        #            device_num[name] = max([device_num[name], int(data[int(dev_info[name]['stateNUM']) - 1])])
        #        else:
        #            device_num[name] = 1

    mqtt_client = mqtt.Client('mqtt2elfin-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    mqtt_client.user_data_set(target_time)
    mqtt_client.loop_start()
    
    

    while time.time() < target_time:
        pass

#    mqtt_client.loop_stop()
    
    def mqtt_discovery(payload):
        intg = payload.pop("_intg")

        # MQTT 통합구성요소에 등록되기 위한 추가 내용
        payload["device"] = DISCOVERY_DEVICE
        payload["uniq_id"] = payload["name"]

        # discovery에 등록
        topic = "homeassistant/{}/ezville_wallpad/{}/config".format(intg, payload["name"])
        log("Add new device:  {}".format(topic))
        mqtt_client.publish(topic, json.dumps(payload))
    

    log('다음의 데이터를 찾았습니다...')
    log('======================================')

    for name in collect_data:
        collect_data[name] = sorted(collect_data[name])
#        dev_info[name]['Number'] = device_num[name]
        log('DEVICE: {}'.format(name))
        log('Packets: {}'.format(collect_data[name]))
        log('-------------------')
    log('======================================')
#    log('기기의 숫자만 변경하였습니다. 상태 패킷은 직접 수정하여야 합니다.')
#    with open(share_dir + '/ezville_found_device.json', 'w', encoding='utf-8') as make_file:
#        json.dump(dev_info, make_file, indent="\t")
#        log('기기리스트 저장 중 : /share/ezville_found_device.json')
#    return dev_info

    log('장치를 등록합니다...')
    log('======================================')
    
    for name in device_num:
        if device_subnum[name] == {}:
            for id in range(device_num[name]):
                payload = DISCOVERY_PAYLOAD[name][0].copy()
                payload["~"] = payload["~"].format(id + 1)
                payload["name"] = payload["name"].format(id + 1)
                            
                mqtt_discovery(payload)
        else:
            id = 0
            for i in range(device_num[name]):
                for j in range(device_subnum[name][i + 1]):
                    id += 1
                    
                    payload = DISCOVERY_PAYLOAD[name][0].copy()
                    payload["~"] = payload["~"].format(id)
                    payload["name"] = payload["name"].format(id)
                            
                    mqtt_discovery(payload)
    log('======================================')
    time.sleep(5)
    mqtt_client.loop_stop()
    
                    
def do_work(config):
    debug = config['DEBUG']
    mqtt_log = config['mqtt_log']
    elfin_log = config['elfin_log']
    find_signal = config['save_unregistered_signal']

#    def pad(value):
#        value = int(value)
#        return '0' + str(value) if value < 10 else str(value)

#    def make_hex(k, input_hex, change):
#        if input_hex:
#            try:
#                change = int(change)
#                input_hex = '{}{}{}'.format(input_hex[:change - 1], int(input_hex[change - 1]) + k, input_hex[change:])
#            except:
#                pass
#        return checksum(input_hex)

#    def make_hex_temp(k, curTemp, setTemp, state):  # 온도조절기 16자리 (8byte) hex 만들기
#        if state == 'OFF' or state == 'ON' or state == 'CHANGE':
#            tmp_hex = device_list['Thermo'].get('command' + state)
#            change = device_list['Thermo'].get('commandNUM')
#            tmp_hex = make_hex(k, tmp_hex, change)
#            if state == 'CHANGE':
#                setT = pad(setTemp)
#                chaTnum = OPTION['Thermo'].get('chaTemp')
#                tmp_hex = tmp_hex[:chaTnum - 1] + setT + tmp_hex[chaTnum + 1:]
#            return checksum(tmp_hex)
#        else:
#            tmp_hex = device_list['Thermo'].get(state)
#            change = device_list['Thermo'].get('stateNUM')
#            tmp_hex = make_hex(k, tmp_hex, change)
#            setT = pad(setTemp)
#            curT = pad(curTemp)
#            curTnum = OPTION['Thermo'].get('curTemp')
#            setTnum = OPTION['Thermo'].get('setTemp')
#            tmp_hex = tmp_hex[:setTnum - 1] + setT + tmp_hex[setTnum + 1:]
#            tmp_hex = tmp_hex[:curTnum - 1] + curT + tmp_hex[curTnum + 1:]
#            if state == 'stateOFF':
#                return checksum(tmp_hex)
#            elif state == 'stateON':
#                tmp_hex2 = tmp_hex[:3] + str(3) + tmp_hex[4:]
#                return [checksum(tmp_hex), checksum(tmp_hex2)]
#            else:
#                return None

#    def make_device_info(dev_name, device):
#        num = device.get('Number', 0)
#        if num > 0:
#            arr = {k + 1: {cmd + onoff: make_hex(k, device.get(cmd + onoff), device.get(cmd + 'NUM'))
#                           for cmd in ['command', 'state'] for onoff in ['ON', 'OFF']} for k in range(num)}
#            if dev_name == 'Fan':
#                tmp_hex = arr[1]['stateON']
#                change = device_list['Fan'].get('speedNUM')
#                arr[1]['stateON'] = [make_hex(k, tmp_hex, change) for k in range(3)]
#                tmp_hex = device_list['Fan'].get('commandCHANGE')
#                arr[1]['CHANGE'] = [make_hex(k, tmp_hex, change) for k in range(3)]
#               
#            arr['Num'] = num
#            return arr
#        else:
#            return None
        
#    DEVICE_LISTS = {}
#    for name in device_num:
#        device_info = make_device_info(name, device_list[name])
        
#        if name == 'light':
#            arg = {k + 1: {cmd + onoff: make_hex(k, device.get(cmd + onoff), device.get(cmd + 'NUM')) 
#                           for cmd in ['command', 'state'] for onoff in ['ON', 'OFF']} for k in range(num)}
#        elif name == 'thermostat':
#            arg = {k + 1: {cmd + onoff: make_hex(k, device.get(cmd + onoff), device.get(cmd + 'NUM')) 
#                           for cmd in ['command', 'state'] for onoff in ['ON', 'OFF']} for k in range(num)}
#        
#        if device_info:
#            DEVICE_LISTS[name] = device_info
#
#    prefix_list = {}
#    log('----------------------')
#    log('등록된 기기 목록 DEVICE_LISTS..')
#    log('----------------------')
#    for name in DEVICE_LISTS:
#        state = DEVICE_LISTS[name][1].get('stateON')
#        if state:
#            prefix = state[0][:2] if isinstance(state, list) else state[:2]
#            prefix_list[prefix] = name
#        log('{}: {}'.format(name, DEVICE_LISTS[name]))
#    log('----------------------')

    HOMESTATE = {}
    QUEUE = []
    COLLECTDATA = {'cond': find_signal, 'data': set(), 'EVtime': time.time(), 'LastRecv': time.time_ns()}
    if find_signal:
        log('[LOG] 50개의 신호를 수집 중..')

    async def recv_from_HA(topics, value):
        global QUEUE
        device = topics[1][:-1]
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        if device in RS485_DEVICE:
            key = topics[1] + topics[2]
            idx = int(topics[1][-1])
            cur_state = HOMESTATE.get(key)
            value = 'ON' if value == 'heat' else value.upper()
            if cur_state:
                if value == cur_state:
                    if debug:
                        log('[DEBUG] {} is already set: {}'.format(key, value))
                else:
                    if device == 'thermostat':
                        curTemp = HOMESTATE.get(topics[1] + 'curTemp')
                        setTemp = HOMESTATE.get(topics[1] + 'setTemp')
                        if topics[2] == 'away':
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01010000')
                            recvcmd = ['NULL']
#                            sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, value)
#                            recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'state' + value)]
                            if sendcmd:
                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                if debug:
                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        elif topics[2] == 'setTemp':
                            value = int(float(value))
                            if value == int(setTemp):
                                if debug:
                                    log('[DEBUG] {} is already set: {}'.format(topics[1], value))
                            else:
                                setTemp = value
                                sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(setTemp) + '0000')
                                recvcmd = ['F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']]
#                                sendcmd = make_hex_temp(idx - 1, curTemp, setTemp, 'CHANGE')
#                                recvcmd = [make_hex_temp(idx - 1, curTemp, setTemp, 'stateON')]
                                if sendcmd:
                                    QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
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
                        rn = 0
                        ln = 0
                      
                        while idx > 0:
                            ln = idx
                            rn += 1
                            idx = idx - device_subnum[device][rn]
                            
                        pwr = '01' if value == 'ON' else '00'
                        
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(rn) + RS485_DEVICE[device]['power']['cmd'] + '030' + str(ln) + pwr + '000000')
#                        sendcmd = DEVICE_LISTS[device][idx].get('command' + value)
                        if sendcmd:
                            recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(rn) + RS485_DEVICE[device]['power']['ack']]
                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
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

    async def slice_raw_data(raw_data):
        if elfin_log:
            log('[SIGNAL] receved: {}'.format(raw_data))
        # if COLLECTDATA['cond']:
        #     if len(COLLECTDATA['data']) < 50:
        #         if data not in COLLECTDATA['data']:
        #             COLLECTDATA['data'].add(data)
        #     else:
        #         COLLECTDATA['cond'] = False
        #         with open(share_dir + '/collected_signal.txt', 'w', encoding='utf-8') as make_file:
        #             json.dump(COLLECTDATA['data'], make_file, indent="\t")
        #             log('[Complete] Collect 50 signals. See : /share/collected_signal.txt')
        #         COLLECTDATA['data'] = None

        #cors = [recv_from_elfin(raw_data[k:k + 16]) for k in range(0, len(raw_data), 16) if raw_data[k:k + 16] == checksum(raw_data[k:k + 16])]
        
        global RESIDUE
        raw_data = RESIDUE + raw_data
        
        k = 0
        cors = []
        msg_length = len(raw_data)
        while k < msg_length:
            if raw_data[k:k + 2] == "F7":
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
        
                if packet != checksum(packet):
                    k+=1
                    continue
 #               task = asyncio.create_task(recv_from_elfin(packet))
 #               cors.append(task)
                cors.append(recv_from_elfin(packet))
                RESIDUE = ""
                k = k + packet_length
            else:
                k+=1
        await asyncio.gather(*cors)

    async def recv_from_elfin(data):
        COLLECTDATA['LastRecv'] = time.time_ns()
        global QUEUE
        if data:
#            if HOMESTATE.get('EV1power') == 'ON':
#                if COLLECTDATA['EVtime'] < time.time():
#                    await update_state('EV', 0, 'OFF')
            
            for que in QUEUE:
                if data[0:8] in que['recvcmd']:
                    QUEUE.remove(que)
                    if debug:
                        log('[DEBUG] Found matched hex: {}. Delete a queue: {}'.format(raw_data, que))
                    break
            
            if not STATE_HEADER.get(data[2:4]):
                return
            
            device_name = STATE_HEADER.get(data[2:4])[0]
            if device_name == 'thermostat':
                if data[6:8] == STATE_HEADER.get(data[2:4])[1] or data[6:8] == ACK_HEADER.get(data[2:4])[1]:
#                    cors = []                    
                    device_count = device_num[device_name]
                    for ic in range(device_count):
                        curT = str(int(data[20 + 4 * ic:22 + 4 * ic], 16))
                        setT = str(int(data[22 + 4 * ic:24 + 4 * ic], 16))
                        index = ic
                        onoff = 'ON' if int(data[12:14], 16) & 0x1F >> (device_count - 1 - ic) & 1 else 'OFF'
                        await update_state(device_name, index, onoff)
                        await update_temperature(index, curT, setT)
#                        cors.append(update_state(device_name, index, onoff))
#                        cors.append(update_temperature(index, curT, setT))
                    
#                    await asyncio.gather(*cors)

            elif device_name == 'light':
                if data[6:8] == STATE_HEADER.get(data[2:4])[1] or data[6:8] == ACK_HEADER.get(data[2:4])[1]:
                    cors = []
                    device_count = device_num[device_name]
                    light_count = device_subnum[device_name][int(data[5], 16)]
                 
                    base_index = 0
                    for c in range(int(data[5], 16) - 1):
                        base_index += device_subnum[device_name][c+1]
                
                    for ic in range(light_count):
                        index = base_index + ic
                        onoff = 'ON' if int(data[12 + 2 * ic: 14 + 2 * ic], 16) > 0 else 'OFF'
                        await update_state(device_name, index, onoff)
#                        cors.append(update_state(device_name, index, onoff))
    
#                    await  asyncio.gather(*cors)
#            elif device_name == 'Fan':
#                if data in DEVICE_LISTS['Fan'][1]['stateON']:
#                    speed = DEVICE_LISTS['Fan'][1]['stateON'].index(data)
#                    await update_state('Fan', 0, 'ON')
#                    await update_fan(0, speed)
#                elif data == DEVICE_LISTS['Fan'][1]['stateOFF']:
#                    await update_state('Fan', 0, 'OFF')
#                else:
#                    log("[WARNING] <{}> 기기의 신호를 찾음: {}".format(device_name, data))
#                    log('[WARNING] 기기목록에 등록되지 않는 패킷입니다. JSON 파일을 확인하세요..')
#            elif device_name == 'Outlet':
#                staNUM = device_list['Outlet']['stateNUM']
#                index = int(data[staNUM - 1]) - 1
#
#                for onoff in ['OFF', 'ON']:
#                    if data.startswith(DEVICE_LISTS[device_name][index + 1]['state' + onoff][:8]):
#                        await update_state(device_name, index, onoff)
#                        if onoff == 'ON':
#                            await update_outlet_value(index, data[10:14])
#                        else:
#                            await update_outlet_value(index, 0)
#            elif device_name == 'EV':
#                val = int(data[4:6], 16)
#                await update_state('EV', 0, 'ON')
#                await update_ev_value(0, val)
#                COLLECTDATA['EVtime'] = time.time() + 3
            else:
#                num = DEVICE_LISTS[device_name]['Num']
#                state = [DEVICE_LISTS[device_name][k + 1]['stateOFF'] for k in range(num)] + [
#                    DEVICE_LISTS[device_name][k + 1]['stateON'] for k in range(num)]
#                if data in state:
#                    index = state.index(data)
#                    onoff, index = ['OFF', index] if index < num else ['ON', index - num]
#                    await update_state(device_name, index, onoff)
#                else:
#                    log("[WARNING] <{}> 기기의 신호를 찾음: {}".format(device_name, data))
#                    log('[WARNING] 기기목록에 등록되지 않는 패킷입니다. JSON 파일을 확인하세요..')
                log("[WARNING] <{}> 기기의 신호를 찾음: {}".format(device_name, data))
                log('[WARNING] 기기목록에 등록되지 않는 패킷입니다...')
        
#        await asyncio.sleep(0)
        
    async def update_state(device, idx, onoff):
        state = 'power'
        deviceID = device + str(idx + 1)
        key = deviceID + state

        if onoff != HOMESTATE.get(key):
            HOMESTATE[key] = onoff
            topic = STATE_TOPIC.format(deviceID, state)

            mqtt_client.publish(topic, onoff.encode())
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, onoff))
        else:
            if debug:
                log('[DEBUG] {} is already set: {}'.format(deviceID, onoff))
        return

#    async def update_fan(idx, onoff):
#        deviceID = 'Fan' + str(idx + 1)
#        if onoff == 'ON' or onoff == 'OFF':
#            state = 'power'
#
#        else:
#            try:
#                speed_list = ['low', 'medium', 'high']
#                onoff = speed_list[int(onoff) - 1]
#                state = 'speed'
#            except:
#                return
#        key = deviceID + state
#
#        if onoff != HOMESTATE.get(key):
#            HOMESTATE[key] = onoff
#            topic = STATE_TOPIC.format(deviceID, state)
#            mqtt_client.publish(topic, onoff.encode())
#            if mqtt_log:
#                log('[LOG] ->> HA : {} >> {}'.format(topic, onoff))
#        else:
#            if debug:
#                log('[DEBUG] {} is already set: {}'.format(deviceID, onoff))
#        return

    async def update_temperature(idx, curTemp, setTemp):
        deviceID = 'thermostat' + str(idx + 1)
        temperature = {'curTemp': "{:02X}".format(int(curTemp, 16)), 'setTemp': "{:02X}".format(int(setTemp, 16))}
        for state in temperature:
            key = deviceID + state
            val = temperature[state]
            if val != HOMESTATE.get(key):
                HOMESTATE[key] = val
                topic = STATE_TOPIC.format(deviceID, state)
                mqtt_client.publish(topic, val.encode())
                if mqtt_log:
                    log('[LOG] ->> HA : {} -> {}'.format(topic, val))
            else:
                if debug:
                    log('[DEBUG] {} is already set: {}'.format(key, val))
        return

#    async def update_outlet_value(idx, val):
#        deviceID = 'Outlet' + str(idx + 1)
#        try:
#            val = '%.1f' % float(int(val) / 10)
#            topic = STATE_TOPIC.format(deviceID, 'watt')
#            mqtt_client.publish(topic, val.encode())
#            if debug:
#                log('[LOG] ->> HA : {} -> {}'.format(topic, val))
#        except:
#            pass

#    async def update_ev_value(idx, val):
#        deviceID = 'EV' + str(idx + 1)
#        try:
#            BF = device_info['EV']['BasementFloor']
#            val = str(int(val) - BF + 1) if val >= BF else 'B' + str(BF - int(val))
#            topic = STATE_TOPIC.format(deviceID, 'floor')
#            mqtt_client.publish(topic, val.encode())
#            if debug:
#                log('[LOG] ->> HA : {} -> {}'.format(topic, val))
#        except:
#            pass

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log("MQTT 접속 중..")
            client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0), (ELFIN_TOPIC + '/send', 1)])
#            if 'EV' in DEVICE_LISTS:
#                asyncio.run(update_state('EV', 0, 'OFF'))
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])
    
    async def process_message(msg):
        topics = msg.topic.split('/')
        
        if topics[0] == HA_TOPIC and topics[-1] == 'command':
            await recv_from_HA(topics, msg.payload.decode('utf-8'))
        elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
            await slice_raw_data(msg.payload.hex().upper())

    async def deque_message():
        stop = False
        out = False
        global msg_queue
        while not stop:
            if msg_queue.empty():
                stop = True
            else:
                out = True
                msg =msg_queue.get()
                await process_message(msg)
#        return out
            
    def on_message(client, userdata, msg):
        global msg_queue
        global start_flag
        if start_flag == False:
            start_flag == True
            
        msg_queue.put(msg)
  #      asyncio.ensure_future(queue.put(msg))
    #    topics = msg.topic.split('/')
    #    try:
    #        if topics[0] == HA_TOPIC and topics[-1] == 'command':
    #            msg_queue.put(
    #            #asyncio.run(recv_from_HA(topics, msg.payload.decode('utf-8')))
    #        elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
    #            #asyncio.run(slice_raw_data(msg.payload.hex().upper()))
    #    except:
    #        pass
                    
    mqtt_client = mqtt.Client('mqtt2elfin-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    # mqtt_client.user_data_set(target_time)
    mqtt_client.loop_start()

    async def send_to_elfin():
#        test = not await deque_message()
#        while test:
#            test = not await deque_message()
#            asyncio.sleep(0)
            
#        while True:
        while start_flag:
            if QUEUE:
                log("
            else:
                log("EMPTY")
            try:
                if time.time_ns() - COLLECTDATA['LastRecv'] > 10000000000:  # 10s
                    log('[WARNING] 10초간 신호를 받지 못했습니다. ew11 기기를 재시작합니다.')
                    try:
                        elfin_id = config['elfin_id']
                        elfin_password = config['elfin_password']
                        elfin_server = config['elfin_server']

                        ew11 = telnetlib.Telnet(elfin_server)

                        ew11.read_until(b"login:")
                        ew11.write(elfin_id.encode('utf-8') + b'\n')
                        ew11.read_until(b"password:")
                        ew11.write(elfin_password.encode('utf-8') + b'\n')
                        ew11.write('Restart'.encode('utf-8') + b'\n')

                        await asyncio.sleep(10)
                    except:
                        log('[WARNING] 기기 재시작 오류! 기기 상태를 확인하세요.')
                    COLLECTDATA['LastRecv'] = time.time_ns()
                elif time.time_ns() - COLLECTDATA['LastRecv'] > 100000000:
                    log("TTTTTT")
                    if QUEUE:
                        send_data = QUEUE.pop(0)
                        if elfin_log:
                            log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
                        # await asyncio.sleep(0.01)
                        if send_data['count'] < 5:
                            send_data['count'] = send_data['count'] + 1
                            QUEUE.append(send_data)
                        else:
                            if elfin_log:
                                log('[SIGNAL] Send over 5 times. Send Failure. Delete a queue: {}'.format(send_data))
            except Exception as err:
                log('[ERROR] send_to_elfin(): {}'.format(err))
                return True
            #await asyncio.sleep(0.01)
            await asyncio.sleep(0)
            
#    loop = asyncio.get_event_loop()
    
    #cors = [deque_message(), send_to_elfin()]
    #group = asyncio.gather(*cors)
    #asyncio.run(group)
    
    async def main_run():
        while True:
            await asyncio.gather(
                deque_message(),
                send_to_elfin()
            )
        
    
    asyncio.run(main_run())
    #task1 = asyncio.create_task(deque_message())
    #task2 = asyncio.create_task(send_to_elfin())
    #tasks = [task1, task2]
    #group = asyncio.gather(*tasks)
    #asyncio.run(group)
    #loop.run_until_complete(group)
#    loop.run_until_complete(deque_message())
#    loop.run_until_complete(send_to_elfin())

    #loop = asyncio.get_running_loop()
#    loop.close()
#    mqtt_client.loop_stop()


if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)
#    try:
#        with open(share_dir + '/ezville_found_device.json') as file:
#            log('기기 정보 파일을 찾음: /share/ezville_found_device.json')
#            OPTION = json.load(file)
#    except IOError:
#        log('기기 정보 파일이 없습니다.: /share/ezville_found_device.json')
#        OPTION = find_device(CONFIG)
    find_device(CONFIG)
    
    while True:
        do_work(CONFIG)
