import os
import requests
import sys

# ---------------------------------------------------------
# 1. 환경변수 제대로 들어왔나 확인 (로그에 출력)
# ---------------------------------------------------------
print("🕵️‍♂️ [진단 시작] 텔레그램 연결 테스트...")

TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_RAW = os.environ.get('TELEGRAM_CHAT_ID')

# 토큰 확인
if not TOKEN:
    print("❌ [치명적 오류] TELEGRAM_TOKEN이 없습니다! (Secrets/YML 확인 필수)")
else:
    print(f"✅ 토큰 감지됨: {TOKEN[:5]}..." + "*"*10)

# 채팅 ID 확인
if not CHAT_ID_RAW:
    print("❌ [치명적 오류] TELEGRAM_CHAT_ID가 없습니다!")
    sys.exit(1) # 강제 종료

CHAT_ID_LIST = [c.strip() for c in CHAT_ID_RAW.split(',') if c.strip()]
print(f"✅ 채팅방 ID 목록: {CHAT_ID_LIST}")

# ---------------------------------------------------------
# 2. 봇 자체가 살아있는지 확인 (getMe)
# ---------------------------------------------------------
try:
    url_me = f"https://api.telegram.org/bot{TOKEN}/getMe"
    res_me = requests.get(url_me)
    if res_me.status_code == 200:
        bot_info = res_me.json()
        print(f"✅ [인증 성공] 봇 이름: {bot_info['result']['first_name']} (@{bot_info['result']['username']})")
    else:
        print(f"❌ [인증 실패] 토큰이 틀렸습니다! 응답코드: {res_me.status_code}")
        print(f"👉 메시지: {res_me.text}")
        sys.exit(1)
except Exception as e:
    print(f"❌ [연결 실패] 인터넷 연결 문제 또는 URL 에러: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# 3. 메시지 강제 발송 테스트
# ---------------------------------------------------------
print("\n📨 [발송 테스트] 메시지를 보냅니다...")

for chat_id in CHAT_ID_LIST:
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': "🚀 [테스트 성공] 선생님, 이 메시지가 보이면 연결은 완벽합니다!"
    }
    
    try:
        res = requests.post(send_url, data=payload)
        if res.status_code == 200:
            print(f"🎉 [전송 성공] Chat ID {chat_id}로 메시지 발송 완료!")
        else:
            print(f"❌ [전송 실패] Chat ID {chat_id} | 원인: {res.text}")
            print("👉 힌트: 봇에게 말을 건 적이 없거나(Start 안 누름), 채팅방 ID가 틀렸을 수 있습니다.")
    except Exception as e:
        print(f"❌ [전송 에러] {e}")

print("---------------------------------------------------")
print("🏁 진단 종료. 이 로그를 확인해주세요.")
