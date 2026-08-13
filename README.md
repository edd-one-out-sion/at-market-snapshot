# aT Wholesale Market Collector

한국 공공데이터포털의 aT 도매시장 경락 OpenAPI를 정식 인증키로 호출해 수집하고, 비공개 데이터베이스에 적재하는 개인 프로젝트용 수집기입니다.

이 저장소는 수집 코드만 공개합니다. 수집 결과와 백업 파일은 포함하지 않으며 비공개 저장소에만 보관합니다.

## Scheduled operation

GitHub Actions는 KST 02:17, 04:07, 05:17, 07:07, 12:07, 21:07에 실행됩니다.
05:17과 07:07 예약 실행은 아침 기준선을 검사합니다.

기준선은 고정 행수를 사용하지 않습니다. 실행 시점의 `collect=true` 시장 수를 읽고,
당일은 모든 활성 시장의 `at-realtime`, 전일은 모든 활성 시장의 `at-origin`이
각각 1행 이상 존재해야 통과합니다. 일요일은 휴장으로 계산하며, 그 밖의 날짜는
모든 활성 시장·품목의 해당 source가 정상 성공하면서 0행을 반환한 경우에만
휴장으로 인정합니다.

세 시장을 모두 엄격하게 검사하는 근거는 과거 07:07 예약 실행 로그입니다.
정상 완료된 영업일 2026-08-10, 2026-08-11, 2026-08-13에 가락·남촌·삼산 모두
당일 realtime 적재가 확인됐습니다. 시장별 행수는 각각
`4916/501/189`, `4548/596/235`, `4582/543/272`였습니다.

## Failure gate

예약된 야간·아침 회복 회차에서 다음 조건을 모두 만족하는 경우에만 job을
성공+warning으로 끝냅니다.

- 연속 실패 차단기가 발동했고 시도한 묶음이 모두 timeout
- 성공 묶음, 비-timeout 실패, 수집 행이 모두 0
- 적재 준비·건너뜀·보류·삭제·신규 적재가 모두 0
- 불완전 후보만 기록되어 기존 DB 데이터가 그대로 유지됨
- 비공개 백업 성공

부분 수집, 수집 후 0적재, 시간 예산 종료, 비-timeout 오류, 적재/RPC 실패,
백업 실패, 아침 기준선 미달은 계속 job 실패로 처리합니다. 12:07 회차에는
timeout 강등을 적용하지 않습니다.

## GitHub Actions Secrets

- DATA_GO_KR_API_KEY_ENC
- SUPABASE_URL
- SUPABASE_SERVICE_KEY
- BACKUP_PUSH_TOKEN
- BACKUP_REPO

## License

Source code is licensed under the MIT License. See LICENSE.
