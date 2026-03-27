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
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

## GitHub Actions 자동 실행

이 저장소에는 `.github/workflows/daily.yml` 이 포함되어 있습니다.

- 매일 08:10 KST 자동 실행
- 수동 실행도 가능
- 데이터 파일과 `docs/index.html` 을 갱신 후 자동 커밋

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
- `data/etf_list.csv` 의 `error` 컬럼에 실패 원인이 남습니다.

## 디버그 팁

처음에는 몇 개만 테스트하는 게 좋습니다.

```bash
MAX_ETFS=3 python main.py
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
