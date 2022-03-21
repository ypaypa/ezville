# KT Add-ons for HAOS
 
HAKorea Addon 및 일부 Addon을 개인 환경(Ezville)에 맞춰 수정 중


### EzVille 월패드 RS485 Add-on

n-andFlash님이 만든 SDS월패드 애드온을 EzVille 용으로 수정. 현재 조명 및 난방 제어만 지원
MQTT discovery를 이용, 장치별로 yaml 파일을 직접 작성하지 않아도 집에 있는 모든 장치가 HA에 자동으로 추가됩니다.

### Universal Wallpad Controller with RS485 (wallpad)

 그레고리하우스님이 만든 코맥스 월패드 nodejs 프로그램을 애드온으로 포팅한 것입니다.
 기본적으로 코맥스로 동작하며 커스터마이징이 가능한 애드온으로 다른 아파트 nodejs 파일을 사용할 수 있습니다. 
 현재 삼성, 대림, 코맥스, 현대 월패드를 지원하면 ew11 같은 무선 소켓 연결도 지원합니다. 
 최근 HAOS에서 설치가 안되는 문제를 수정

### Simple MQTT EzVille Control

 순수 MQTT 기반으로 MQTT Discovery 기능 

