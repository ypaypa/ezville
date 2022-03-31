# MQTT 기반 Simple EzVille Wallpad Control

## 1. 지원 기능

- 조명, 난방 (외출 모드), 대기전력차단, 엘리베이터콜 상태 조회 및 제어 지원
- 대기전력소모, 현관 스위치 상태 (외출 모드, 그룹 조명) 센서 지원
- MQTT 기반 장치 자동 Discovery 지원



## 2. 설치 방법

- ① 애드온 스토어 -> 저장소 -> [https://github.com/ktdo79/addons](https://github.com/ktdo79/addons) 추가하기 
- ② MQTT 기반 Simple EzVille Wallpad Control 설치

> 사전에 MQTT Integration 및 Mosquitto Broker Addon 설치 필수



## 3. 설정 방법

### 3.1. EW11 설정

#### 3.1.1. Serial Port 설정

- Buffer Size를 128로 변경 

#### 3.1.2. Communication Settings 설정

##### 3.1.2.1. MQTT 설정

1. +Add를 누르고 MQTT 추가 
2. Server 주소 = Home Assistant IP 주소, Port는 Mosquitto Broker 설정 Port, Buffer Size는 128 로 설정
3. Subscribe Topic는 ew11/send, Publish Topic은 ew11/recv 로 설정
4. Mosquitto Broker에 ID/Password가 있으면 MQTT Account, Password에 기입

##### 3.1.2.2. netp 설정

1. Buffer Size를 128로 변경
