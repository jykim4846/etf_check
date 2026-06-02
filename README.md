# etf_check

삼성(KODEX), 미래에셋(TIGER), 타임폴리오(TIME) 액티브 ETF를 매일 수집해서:

- ETF 목록
- 순자산총액(AUM, 억원)
- ETF별 구성종목 / 비중
- 모바일에서 보기 쉬운 정적 페이지

를 생성하는 프로젝트입니다.

## 생성 파일

- `data/etf_list.csv` : ETF 목록 + AUM
- `data/etf_holdings.csv` : ETF별 구성종목 + 비중
- `docs/index.html` : GitHub Pages용 모바일 조회 화면

## 로컬 실행

```bash
./scripts/run_local.sh
```

이 스크립트는 필요한 경우 `.venv`를 만들고, 의존성을 설치한 뒤 `main.py`를 실행합니다.
실행 후 아래 파일이 갱신됩니다.

- `data/*.csv`, `data/*.json`
- `data/history/<수집시각>/...`
- `docs/index.html`
- `public/index.html`

정적 페이지를 로컬에서 확인하려면:

```bash
.venv/bin/python -m http.server 8000 -d docs
```

브라우저에서 `http://localhost:8000`을 열면 됩니다.

Windows PowerShell에서 직접 실행하려면:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

## 자동 실행

GitHub Actions 자동 수집은 꺼져 있습니다. 이 저장소에는 `.github/workflows/daily.yml`을 두지 않습니다.

매일 자동으로 돌리고 싶으면 이 로컬 머신의 `cron` 또는 `launchd`에서 `scripts/run_local.sh`를 실행하세요.
예를 들어 macOS/Linux `cron`에서 매일 08:10 KST에 실행하려면:

```cron
10 8 * * * cd /Users/jongyeon.kim/Desktop/etf_check_bundle && ./scripts/run_local.sh >> logs/local-collector.log 2>&1
```

`logs/` 디렉터리는 먼저 만들어 두면 됩니다.

## Vercel 배포

1. Vercel에서 이 저장소를 import
2. 프로젝트 루트는 그대로 사용
3. Framework Preset은 `Other` 로 두기
4. `vercel.json` 을 자동 인식하도록 두기
5. Deploy

현재 설정은 저장소에 커밋된 `public/` 정적 파일을 배포하는 방식입니다.
수집은 Vercel이나 GitHub Actions가 아니라 로컬에서 `scripts/run_local.sh`로 실행합니다.

## GitHub Pages 켜기

1. GitHub 저장소 열기
2. **Settings**
3. **Pages**
4. **Build and deployment**
   - Source: `Deploy from a branch`
   - Branch: `main`
   - Folder: `/docs`
5. Save

몇 분 뒤 아래 주소로 접속:

`https://jykim4846.github.io/etf_check/`

## 주의

- AUM은 일반 주식의 시가총액이 아니라 ETF의 **순자산총액** 기준입니다.
- 데이터는 공개 페이지 기준이라 장중 실시간과 다를 수 있습니다.
- 일부 ETF는 사이트 구조 변경으로 구성종목 파싱이 실패할 수 있습니다.
- 현재 파이프라인은 실패한 ETF를 건너뛰지 않고 전체 실행을 실패시킵니다.

## 디버그 팁

처음에는 몇 개만 테스트하는 게 좋습니다.

```bash
MAX_ETFS=3 ./scripts/run_local.sh
```

## 사용 데이터 소스

실제 수집 로직은 아래 흐름으로 동작합니다.

1. `FunETF ETF 필터 엑셀`을 내려받아 액티브 ETF 전체 목록을 만든다.
2. 종목명/운용사 규칙으로 삼성(KODEX), 미래에셋(TIGER), 타임폴리오(TIME)만 필터링한다.
3. 각 ETF의 `FunETF 상세 페이지`를 다시 조회해서 AUM, 기준일, 구성종목/비중을 파싱한다.

참고용으로 공식 운용사 페이지 URL도 `data/meta.json`에 함께 남긴다.

- FunETF ETF 필터 엑셀: https://www.funetf.co.kr/api/public/download/excel/etfFilter
- FunETF ETF 상세: `https://www.funetf.co.kr/product/etf/view/{isin}`
- TIME ETF 라인업: https://timeetf.co.kr/m11.php
- TIGER 액티브 ETF 라인업: https://investments.miraeasset.com/tigeretf/ko/content/activeLineUp/list.do
- KODEX ETF 목록: https://www.samsungfund.com/etf/product/list.do
