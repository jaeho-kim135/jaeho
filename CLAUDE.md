# 포함된 노드 목록

이 저장소에는 다음 커스텀 Spark 노드들이 포함되어 있습니다:

| 노드 | 패키지 | 다이얼로그 방식 | 상태 |
|------|--------|----------------|------|
| Spark Unpivot (Hyim) | `preproc.unpivot` | Swing (DataAwareNodeDialogPane) | 테스트 완료 |
| Spark Multi Query (Hyim) | `sql.multiquery` | WebUI (NodeParameters) | 테스트 완료 |
| Spark Expression (Hyim) | `sql.expression` | WebUI (커스텀 Vue.js) | 테스트 완료 |
| Spark StringToNumber (Hyim) | `preproc.stringtonumber` | WebUI (NodeParameters) | 테스트 완료 |
| Spark NumberToString (Hyim) | `preproc.numbertostring` | WebUI (NodeParameters) | 테스트 완료 |
| Spark String to Date&Time (Hyim) | `preproc.stringtodatetime` | WebUI (NodeParameters) | 테스트 완료 |

---
---

# Spark Expression Node (신규 개발)

## 개요

여러 Spark SQL 표현식을 순차적으로 적용하여 컬럼을 추가/변환하는 노드.
각 표현식마다 Expression, Output Mode (APPEND/REPLACE), Output Column Name을 설정할 수 있다.
**커스텀 Vue.js WebUI 다이얼로그**를 사용하여 코드 에디터 + 실시간 미리보기를 제공한다.

- **노드명**: Spark Expression (Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator
- **다이얼로그**: 커스텀 WebUI (`NodeDialog` 인터페이스 + Vue.js 프론트엔드)

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark_dx.node` | 노드 UI (Factory, Model, Settings, WebDialog, RPC Service, JobInput/Output) + Vue.js 프론트엔드 (`js-src/`) |
| `org.knime.bigdata_spark3_4_dx` | Spark 3.4용 ExpressionJob 구현 |
| `org.knime.bigdata_spark3_5_dx` | Spark 3.5용 ExpressionJob 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.sql.expression`)

| 파일 | 역할 |
|------|------|
| `SparkExpressionNodeFactory.java` | `DefaultSparkNodeFactory` + `NodeDialogFactory` 구현. 카테고리: "sql" |
| `SparkExpressionNodeFactory.xml` | 노드 설명 문서 |
| `SparkExpressionNodeModel.java` | 노드 모델. `isNodeConfigured()` 체크, flow variable 치환, 순차적 `withColumn()` + `expr()` 실행 |
| `SparkExpressionSettings.java` | 핵심 설정 (expressions[], outputModes[], columnNames[], configured 플래그) |
| `SparkExpressionWebNodeDialog.java` | `NodeDialog` 구현. `ScriptingNodeSettingsService` 기반, RPC 데이터 서비스 제공, 입력 컬럼/Flow Variable/Function Catalog 초기 데이터 전달 |
| `SparkExpressionWebSettings.java` | `GenericSettingsIOManager` 구현. WebUI JSON ↔ NodeSettings 브릿지 |
| `SparkExpressionRpcService.java` | JSON-RPC 서비스. Evaluate (미리보기) + Input Table 미리보기. `$${varName}` flow variable 치환 지원 |
| `SparkExpressionJobInput.java` | Job 입력 VO |
| `SparkExpressionJobOutput.java` | Job 출력 VO (previewData 포함) |
| `icon.png` | 노드 아이콘 |

### Vue.js 프론트엔드 (`js-src/`)

| 파일/디렉토리 | 역할 |
|--------------|------|
| `src/App.vue` | 메인 앱. 3-panel (enlarged) / compact 레이아웃 |
| `src/components/ExpressionEditors.vue` | 다중 표현식 에디터 (Add/Remove/Up/Down) + Output Mode + Column Name |
| `src/components/InputColumns.vue` | 입력 컬럼 목록 (더블클릭으로 에디터에 삽입) |
| `src/components/FlowVariables.vue` | Flow Variable 목록 (`$${varName}` 형태로 삽입) |
| `src/components/FunctionCatalog.vue` | 함수 카탈로그 (카테고리별 함수 목록, 더블클릭 삽입) |
| `src/components/OutputPreview.vue` | 출력 미리보기 (Output/Input 탭, Evaluate 버튼) |
| `src/knimeService.js` | KNIME 통신 브릿지 (`@knime/ui-extension-service` 사용). DialogService dirty-state 추적 |
| `dist/` | Vite 빌드 결과물 (KNIME에서 직접 사용) |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `ExpressionJob.java` | `SparkJob` 구현. `validateOnly=true`: `showString(10, 40, false)` 미리보기 반환. `validateOnly=false`: 결과 DataFrame 저장 |
| `ExpressionJobRunFactory.java` | Job 실행 팩토리 |
| `ExpressionJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 표현식 적용 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | 표현식 적용된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `expressions` | String[] | [""] | Spark SQL 표현식 목록 |
| `outputModes` | String[] | ["APPEND"] | 각 표현식의 출력 모드 (APPEND: 새 컬럼 추가, REPLACE: 기존 컬럼 교체) |
| `columnNames` | String[] | ["new_column"] | 각 표현식의 출력 컬럼명 |
| `configured` | Boolean | false | OK 버튼 클릭 여부 (최초 사용 시 경고 표시용) |

---

## 주요 기능

### Evaluate First 10 Rows
- 다이얼로그에서 "Evaluate" 버튼 클릭 시 실제 Spark 클러스터에서 표현식을 실행하고 결과 10행을 미리보기
- `SparkExpressionRpcService.evaluateExpressions()` → Spark Job (`validateOnly=true`) → `showString(10, 40, false)` 반환
- Flow variable `$${varName}` 플레이스홀더가 실제 값으로 치환됨

### Apply / Apply & Execute 버튼
- `DialogService.registerSettings("model")` → `SettingState.setValue()` 패턴으로 dirty-state 추적
- 설정 변경 시 `markDirty()` 호출 → Apply 버튼 활성화

### Flow Variable 치환
- `$${varName}` 형식으로 표현식에 flow variable 삽입
- STRING → `'value'` (SQL 쿼팅), INTEGER/DOUBLE → 숫자 리터럴

### 유효성 검증
- 빈 표현식 체크
- 빈 컬럼명 체크
- 중복 컬럼명 체크
- REPLACE 모드 시 기존 컬럼 존재 여부 (NodeModel configure에서)
- APPEND 모드 시 기존 컬럼명 충돌 (NodeModel configure에서)

---

## 테스트 완료 항목

- [x] 단일/다중 표현식 APPEND 모드 → 정상
- [x] REPLACE 모드 → 기존 컬럼 교체 정상
- [x] Evaluate First 10 Rows → Spark 클러스터에서 실행 후 미리보기 표시
- [x] Apply / Apply & Execute 버튼 활성화 → 설정 변경 시 정상 활성화
- [x] Flow Variable 치환 → `$${varName}` 정상 치환
- [x] 빈 표현식/컬럼명 → 프론트엔드 검증 에러
- [x] 중복 컬럼명 → 프론트엔드 검증 에러
- [x] Input Table 미리보기 → 정상
- [x] 설정 저장/재로드 → 설정값 유지
- [x] 다이얼로그 OK 클릭 → 노드 설정 완료, 실행 정상

---

## Pull 받아서 사용하기

이 저장소를 pull 받으면 Spark Expression 노드를 포함한 모든 노드가 바로 사용 가능합니다.

### 필요한 파일 (Expression 노드 관련)

**dx.node 플러그인:**
- `org.knime.bigdata.spark_dx.node/src/.../sql/expression/` — Java 소스 전체 (10개 파일)
- `org.knime.bigdata.spark_dx.node/js-src/` — Vue.js 프론트엔드 (src + dist)
- `org.knime.bigdata.spark_dx.node/META-INF/MANIFEST.MF` — `org.knime.scripting.editor` 의존성 포함
- `org.knime.bigdata.spark_dx.node/build.properties` — `js-src/dist/` 포함
- `org.knime.bigdata.spark_dx.node/src/.../DxSparkNodeFactoryProvider.java` — `SparkExpressionNodeFactory` 등록

**spark3_4 플러그인:**
- `org.knime.bigdata_spark3_4_dx/src/.../sql/expression/` — ExpressionJob + Factory + Provider (3개 파일)
- `org.knime.bigdata_spark3_4_dx/META-INF/MANIFEST.MF` — expression 패키지 export
- `org.knime.bigdata_spark3_4_dx/plugin.xml` — ExpressionJobRunFactoryProvider 등록

**spark3_5 플러그인:**
- `org.knime.bigdata_spark3_5_dx/src/.../sql/expression/` — ExpressionJob + Factory + Provider (3개 파일)
- `org.knime.bigdata_spark3_5_dx/META-INF/MANIFEST.MF` — expression 패키지 export
- `org.knime.bigdata_spark3_5_dx/plugin.xml` — ExpressionJobRunFactoryProvider 등록

### Eclipse에서 사용하기
1. `git pull` 으로 최신 소스 받기
2. Eclipse에서 3개 플러그인 프로젝트 Import (이미 import 되어있으면 Refresh)
3. `js-src/dist/`가 이미 빌드되어 있으므로 npm 설치 불필요
4. Run As > Eclipse Application → KNIME AP 실행 → Spark 카테고리에서 "Spark Expression (Hyim)" 노드 사용

---
---

# Spark Pivot Node 구조 분석

## 개요

Spark Pivot 노드는 **Spark GroupBy 노드를 확장**하여 피벗(Pivot) 기능을 추가한 노드다.
독립적인 NodeModel/NodeDialog를 갖지 않고, GroupBy의 `SparkGroupByNodeModel`과 `SparkGroupByNodeDialog`를 `pivotNodeMode=true`로 재사용한다.

- **패키지**: `org.knime.bigdata.spark.node.preproc.pivot`
- **최소 Spark 버전**: 2.0
- **노드 타입**: Manipulator

---

## 클래스 구조

### 1. Pivot 패키지 (`preproc/pivot/`)

| 파일 | 역할 |
|------|------|
| `SparkPivotNodeFactory.java` | 노드 팩토리. `DefaultSparkNodeFactory<SparkGroupByNodeModel>` 상속. `createNodeModel()`에서 `new SparkGroupByNodeModel(true)`, `createNodeDialogPane()`에서 `new SparkGroupByNodeDialog(true)` 반환 |
| `SparkPivotNodeFactory.xml` | 노드 UI 설명 (탭 구성, 옵션 설명) |

### 2. GroupBy 패키지 (`preproc/groupby/`) - Pivot이 재사용하는 핵심 클래스

| 파일 | 역할 |
|------|------|
| `SparkGroupByNodeModel.java` | GroupBy + Pivot 공용 NodeModel. `m_pivotNodeMode` 플래그로 분기 |
| `SparkGroupByNodeDialog.java` | GroupBy + Pivot 공용 Dialog. Pivot 모드일 때 Pivot 탭 추가 |
| `SparkGroupByJobInput.java` | Spark Job 입력 VO. GroupBy 함수 + 집계 함수 + Pivot 설정 포함 |
| `SparkGroupByJobOutput.java` | Spark Job 출력 VO. 결과 스펙 + `pivotValuesDropped` 플래그 |
| `AggregationFunctionSettings.java` | Manual/Pattern/Type 기반 집계 함수 설정 관리 |

### 3. Dialog 패키지 (`preproc/groupby/dialog/`)

| 파일 | 역할 |
|------|------|
| `PivotSettings.java` | Pivot 설정 모델 (컬럼, 모드, 값 목록, 제한, missing 처리) |
| `PivotPanel.java` | Pivot 탭 UI 패널 |
| `PivotValuesPanel.java` | Pivot 값 입력 UI 패널 |
| `AbstractAggregationFunctionRow.java` | 집계 함수 행 추상 클래스 |
| `column/` | Manual Aggregation 관련 패널/로우 |
| `pattern/` | Pattern Based Aggregation 관련 패널/로우 |
| `type/` | Type Based Aggregation 관련 패널/로우 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 피벗 대상 Spark DataFrame/RDD |
| 1 | 입력 | `BufferedDataTable` (Optional) | 피벗 값 목록 테이블 |
| 0 | 출력 | `SparkDataPortObject` | 피벗된 결과 DataFrame/RDD |

---

## Pivot 모드 (PivotSettings)

3가지 피벗 값 결정 모드:

| 모드 | 상수 | 설명 |
|------|------|------|
| **Use all values** | `MODE_ALL_VALUES` ("all") | Spark가 자동으로 distinct 값 탐색. DataFrame 물리화 필요 |
| **Use values from data table** | `MODE_INPUT_TABLE` ("inputTable") | 옵션 입력 포트(포트 1)의 테이블에서 값 로드 |
| **Manually specify values** | `MODE_MANUAL_VALUES` ("manual") | 사용자가 직접 값 지정 |

### Pivot 설정 키

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `pivot.column` | String | "" | 피벗 컬럼명 |
| `pivot.mode` | String | "all" | 피벗 모드 |
| `pivot.valuesLimit` | Integer | 500 (1~10000) | 최대 피벗 값 수 |
| `pivot.values` | String[] | [] | 수동 지정 피벗 값 |
| `pivot.ignoreMissingValues` | Boolean | true | Missing 값 무시 여부 |
| `pivot.inputValuesTableColumn` | String | "" | 입력 테이블의 피벗 값 컬럼명 |
| `pivot.validateManualValues` | Boolean | false | DataFrame 내 값과 검증 여부 |

---

## 출력 컬럼 네이밍

피벗 결과 컬럼명 형식: `{피벗값}+{집계명}`

집계명은 `ColumnNamePolicy`에 따라 결정:
- **Keep original name(s)**: 원래 컬럼명 유지
- **Aggregation method (column name)**: `method(colName)` 형식
- **Column name (aggregation method)**: `colName(method)` 형식

---

## 실행 흐름

```
SparkPivotNodeFactory
  └─ creates SparkGroupByNodeModel(pivotNodeMode=true)

configureInternal()
  ├─ Spark 버전 체크 (>= 2.0)
  ├─ 집계 함수 목록 구성 (Manual + Pattern + Type)
  ├─ Pivot 컬럼 유효성 검증
  └─ 모드별 출력 스펙 결정
       ├─ auto → null (Spark 결과에서 스펙 획득)
       ├─ manual → createPivotOutputSpec() 로 사전 계산
       └─ inputTable → null (실행 시 결정)

executeInternal()
  ├─ SparkGroupByJobInput 구성
  ├─ PivotSettings.addJobConfig() 로 피벗 설정 주입
  ├─ Spark Job 실행
  └─ 모드별 출력 스펙 결정 후 SparkDataPortObject 반환
```

---

## JobInput 피벗 관련 필드

| 필드 | 설명 |
|------|------|
| `pivotColumn` | 피벗 대상 컬럼명 |
| `pivotComputeValues` | true면 Spark에서 자동 계산 |
| `pivotComputeValuesLimit` | 자동 계산 시 최대 값 수 |
| `pivotValues` | 명시적 피벗 값 배열 |
| `pivotValidateValues` | 명시 값과 실제 DataFrame 값 비교 검증 |
| `pivotIgnoreMissingValues` | Missing 값 무시 여부 |

---

## 핵심 설계 패턴

1. **Flag 기반 모드 분기**: `SparkGroupByNodeModel`과 `SparkGroupByNodeDialog`가 `boolean pivotNodeMode`로 GroupBy/Pivot 동작 분기
2. **입력 포트 차이**: GroupBy는 `[SparkDataPortObject]` 1개, Pivot는 `[SparkDataPortObject, BufferedDataTable(Optional)]` 2개
3. **집계 함수 3단계**: Manual → Pattern → Type 순서로 적용, 앞 단계에서 선택된 컬럼은 뒷 단계에서 제외
4. **출력 스펙 지연 결정**: auto/inputTable 모드에서는 configure 시 null 반환, execute 후 Spark 결과에서 스펙 획득

---
---

# Spark Unpivot Node (신규 개발)

## 개요

Spark DataFrame을 wide format → long format으로 변환하는 Unpivot(Melt) 노드.
Spark 3.4+ 의 `Dataset.unpivot()` API를 사용한다.
기존 Pivot 노드와 달리 **독립적인 3개 플러그인**으로 구성.

- **노드명**: Spark Unpivot(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark.dx.node` | 노드 UI (Factory, Model, Dialog, Settings, JobInput/Output) |
| `org.knime.bigdata.spark3_4.dx` | Spark 3.4용 Job 구현 |
| `org.knime.bigdata.spark3_5.dx` | Spark 3.5용 Job 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.preproc.unpivot`)

| 파일 | 역할 |
|------|------|
| `SparkUnpivotNodeFactory.java` | `DefaultSparkNodeFactory<SparkUnpivotNodeModel>` 상속. 카테고리: "row" |
| `SparkUnpivotNodeFactory.xml` | 노드 설명 (4탭: Retained Columns, Value Columns, Options, Validation) |
| `SparkUnpivotNodeModel.java` | 노드 모델. configure에서 유효성 검증, execute에서 Spark Job 실행 |
| `SparkUnpivotNodeDialog.java` | `DataAwareNodeDialogPane` 상속. 4탭 UI, 타입 표시, 행 수 추정, 변수 매핑, 정렬, Validation Check |
| `SparkUnpivotSettings.java` | 설정 모델 (retainedColumns, valueColumns, variableColName, valueColName, skipMissingValues, castToString, sortOption, variableValueMap) |
| `SparkUnpivotJobInput.java` | Job 입력 VO. `JobInput` 상속. validateOnly/sortOption/varMap 지원 |
| `SparkUnpivotJobOutput.java` | Job 출력 VO. `JobOutput` 상속. previewData/inputRowCount 포함 |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `UnpivotJob.java` | `SparkJob` 구현. `Dataset.unpivot()` 호출. castToString, 변수 매핑, 정렬, validateOnly 지원 |
| `UnpivotJobRunFactory.java` | Job 실행 팩토리 |
| `UnpivotJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | unpivot 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | long format 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `retainedColumns` | FilterString | [] | 유지할 ID 컬럼 목록 |
| `valueColumns` | FilterString | [] | unpivot 대상 값 컬럼 목록 |
| `variableColName` | String | "variable" | 출력 variable 컬럼명 |
| `valueColName` | String | "value" | 출력 value 컬럼명 |
| `skipMissingValues` | Boolean | true | null 값 행 제외 여부 |
| `castToString` | Boolean | false | 모든 value 컬럼을 String으로 캐스팅 |
| `sortOption` | String | "none" | 출력 정렬 옵션 (none / retained / variable) |
| `variableValueMap` | Map (keys+values 배열) | {} | variable 컬럼 값 커스텀 매핑 |

---

## 유효성 검증 (Dialog OK + NodeModel configure 양쪽)

| 검증 | 에러 메시지 |
|------|------------|
| retained 컬럼 미선택 | "No retained columns selected." |
| value 컬럼 미선택 | "No value columns selected." |
| retained/value 중복 선택 | "The following columns are selected as both retained and value columns: ..." |
| variable 컬럼명 빈값 | "Variable column name must not be empty." |
| value 컬럼명 빈값 | "Value column name must not be empty." |
| variable = value 컬럼명 동일 | "Variable column name and Value column name must be different." |
| 출력 컬럼명이 retained 컬럼명과 충돌 | "Variable/Value column name '...' conflicts with a retained column name." |
| 타입 혼합 (숫자+문자열) + cast OFF | "Value columns have incompatible types: ... Enable 'Cast all value columns to String' option." |

---

## 실행 흐름

```
SparkUnpivotNodeFactory
  └─ creates SparkUnpivotNodeModel

configureInternal()
  ├─ retained/value 컬럼 존재 여부 검증
  ├─ 중복/빈값/충돌 검증
  ├─ 타입 호환성 검증 (castToString OFF 시)
  └─ 출력 스펙 생성: [retained 컬럼들] + [variable: String] + [value: String]

executeInternal()
  ├─ SparkUnpivotJobInput 구성 (sortOption, varMap 포함)
  ├─ SparkContextUtil.getJobRunFactory() 로 Job 실행
  └─ Spark Job 결과 스펙으로 SparkDataPortObject 반환

UnpivotJob.runJob() (Spark 측)
  ├─ castToString=true → value 컬럼들 cast(StringType)
  ├─ Dataset.unpivot(idCols, valCols, variableColName, valueColName)
  ├─ varMap 적용 → when/otherwise로 variable 컬럼 값 치환
  ├─ skipMissing=true → filter(valueCol.isNotNull())
  ├─ sortOption 적용 (retained/variable/none)
  ├─ validateOnly=true → inputFrame.count() + result.showString(5) 반환
  └─ 결과 DataFrame + IntermediateSpec 반환
```

---

## 다이얼로그 기능

### 탭 구성
- **Tab 1 - Retained Columns**: `DialogComponentColumnFilter`로 유지 컬럼 선택 (타입 표시)
- **Tab 2 - Value Columns**: `DialogComponentColumnFilter`로 값 컬럼 선택 (타입 표시)
- **Tab 3 - Options**: 출력 설정, 정렬, 행 수 추정, 변수 값 매핑
- **Tab 4 - Validation**: Check 버튼으로 샘플 데이터 미리보기

### 컬럼 타입 표시
- Retained/Value 컬럼 필터에 `columnName (Type)` 형식으로 데이터 타입 표시
- `installTypeRenderers()`로 JList에 커스텀 ListCellRenderer 적용

### 출력 행 수 추정
- 다이얼로그 열 때 백그라운드(SwingWorker)로 입력 행 수 자동 조회
- `입력 행 수 × value 컬럼 수 = 예상 출력 행 수` 표시
- Skip missing values ON 시 "(max - actual may be less due to Skip missing values)" 안내

### 출력 정렬 옵션
- No sorting (기본) / Sort by retained columns / Sort by variable column

### 변수 값 매핑 (Variable Value Mapping)
- JTable로 각 value 컬럼의 variable 값을 커스텀 이름으로 변경 가능
- 기본값: 컬럼명 자체 (변경하지 않으면 저장하지 않음)

### Validation Check
- `DataAwareNodeDialogPane` 상속하여 PortObject 접근
- Check 버튼 → SwingWorker로 validate-only Spark Job 실행
- 성공 시 초록색 + 샘플 데이터 5행 표시 / 실패 시 빨간색 에러
- 성공 시 입력 행 수도 갱신되어 행 수 추정에 반영

### syncFilterModels
- `DialogComponentColumnFilter`는 saveSettingsTo() 호출 전까지 SettingsModel을 UI와 동기화하지 않음
- `syncFilterModels()`: 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- validateSettings(), runValidation(), updateRowEstimate(), updateVarMapTable() 등에서 사용

---

## 테스트 완료 항목

- [x] 기본 unpivot 실행 (동일 타입 value 컬럼)
- [x] retained 미선택 → 다이얼로그 에러
- [x] value 미선택 → 다이얼로그 에러
- [x] retained/value 중복 → 다이얼로그 에러
- [x] 혼합 타입 + cast OFF → 다이얼로그 에러 (Spark 실행 전 차단)
- [x] 혼합 타입 + cast ON → 정상 실행
- [x] variable/value 컬럼명 동일 → 다이얼로그 에러
- [x] 출력 컬럼명 vs retained 컬럼명 충돌 → 다이얼로그 에러
- [x] 빈 DataFrame (0행) → 정상 (0행 결과)
- [x] 전부 null + skipMissing ON → 0행 결과
- [x] 전부 null + skipMissing OFF → null 포함 행 출력
- [x] value 컬럼 1개만 선택 → 정상
- [x] 설정 저장/재로드 → 설정값 유지
- [x] upstream 컬럼 변경 → 적절한 에러 메시지
- [x] 노드 Reset → 재실행 동일 결과
- [x] Integer + Double (cast OFF) → Spark 자동 변환 정상
- [x] Long Integer + Double (cast OFF) → Spark 자동 변환 정상
- [x] Sort by retained / Sort by variable → 정상 정렬
- [x] Variable Value Mapping → 커스텀 이름 적용 정상
- [x] Validation Check → 샘플 데이터 미리보기 정상
- [x] 행 수 추정 → 다이얼로그 열 때 자동 조회 정상
- [x] Skip missing values + 행 수 추정 안내 정상
- [x] 처음 사용 시 → syncFilterModels로 정상 동작

---
---

# Spark Multi Query Node (신규 개발)

## 개요

선택한 여러 컬럼에 동일한 SQL 표현식 템플릿을 적용하여 변환하는 노드.
`$columnS` 플레이스홀더가 각 대상 컬럼명으로 치환되어 Spark SQL로 실행된다.

- **노드명**: Spark Multi Query(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark.dx.node` | 노드 UI (Factory, Model, Dialog, Settings, JobInput/Output) |
| `org.knime.bigdata.spark3_4.dx` | Spark 3.4용 Job 구현 |
| `org.knime.bigdata.spark3_5.dx` | Spark 3.5용 Job 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.sql.multiquery`)

| 파일 | 역할 |
|------|------|
| `SparkMultiQueryNodeFactory.java` | 노드 팩토리. 카테고리: "sql" |
| `SparkMultiQueryNodeFactory.xml` | 노드 설명 (2탭: Column Selection, Expression & Options) |
| `SparkMultiQueryNodeModel.java` | 노드 모델. configure에서 유효성 검증 (keepOriginal+패턴 충돌, 별칭 충돌 포함), execute에서 Job 실행 |
| `SparkMultiQueryNodeDialog.java` | `DataAwareNodeDialogPane` 상속. 2탭 UI, 템플릿 드롭다운, Keep Original, Output Pattern, SQL Preview, 선택 컬럼 요약, 개선된 Check (전체+개별 컬럼 테스트 + 샘플 데이터) |
| `SparkMultiQuerySettings.java` | 설정 모델 (targetColumns, sqlExpression, keepOriginalColumns, outputColumnPattern) |
| `SparkMultiQueryJobInput.java` | Job 입력 VO. validateOnly/keepOriginal/outputPattern 지원 |
| `SparkMultiQueryJobOutput.java` | Job 출력 VO. previewData 포함 |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `MultiQueryJob.java` | SparkJob 구현. temp view 등록 → SELECT 생성 (keepOriginal/outputPattern 반영) → 실행. validateOnly시 LIMIT 5 + showString |
| `MultiQueryJobRunFactory.java` | Job 실행 팩토리 |
| `MultiQueryJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | SQL 표현식 적용된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `targetColumns` | FilterString | [] | SQL 표현식을 적용할 대상 컬럼 목록 |
| `sqlExpression` | String | "string($columnS)" | $columnS 플레이스홀더 포함 SQL 표현식 |
| `keepOriginalColumns` | Boolean | false | true 시 원본 컬럼 유지하고 변환 컬럼을 새로 추가 |
| `outputColumnPattern` | String | "$columnS" | 출력 컬럼 이름 패턴. $columnS가 컬럼명으로 치환됨 |

---

## 실행 흐름

```
SparkMultiQueryNodeFactory
  └─ creates SparkMultiQueryNodeModel

configureInternal()
  ├─ 대상 컬럼 존재 여부 검증
  ├─ SQL 표현식 빈값/플레이스홀더 검증
  ├─ Output Pattern 빈값/플레이스홀더 검증
  ├─ keepOriginal=true + pattern="$columnS" → 중복 컬럼명 에러
  ├─ keepOriginal=true → 별칭이 기존 비대상 컬럼명과 충돌 검사
  └─ 출력 스펙 null (SQL 결과 타입은 실행 시 결정)

executeInternal()
  ├─ SparkMultiQueryJobInput 구성 (keepOriginal, outputPattern 포함)
  ├─ SparkContextUtil.getJobRunFactory() 로 Job 실행
  └─ Spark Job 결과 스펙으로 SparkDataPortObject 반환

MultiQueryJob.runJob() (Spark 측)
  ├─ 입력 DataFrame을 temp view 등록
  ├─ validateOnly=true → 전체 대상 컬럼으로 테스트 쿼리 (LIMIT 5) 실행 + showString
  ├─ SELECT 절 구성:
  │   ├─ 대상 컬럼 (keepOriginal=false): expr(`col`) AS `alias`
  │   ├─ 대상 컬럼 (keepOriginal=true): `col`, expr(`col`) AS `alias`
  │   └─ 비대상 컬럼: `col` (그대로 유지)
  ├─ spark.sql(query) 실행
  ├─ temp view 정리 (finally)
  └─ 결과 DataFrame + IntermediateSpec 반환
```

---

## 다이얼로그 기능

### 탭 구성
- **Tab 1 - Column Selection**: `DialogComponentColumnFilter`로 대상 컬럼 선택 (타입 표시)
- **Tab 2 - Expression & Options**: 선택 컬럼 요약, 템플릿 드롭다운, SQL 표현식, 옵션, SQL 프리뷰, Check

### 선택 컬럼 요약 (Target Columns)
- Expression & Options 탭 상단에 현재 선택된 컬럼 요약 표시
- `"3 column(s): age (Integer), name (String), salary (Double)"` 형식

### 컬럼 타입 표시
- Column Selection 필터에 `columnName (Type)` 형식으로 데이터 타입 표시

### 표현식 템플릿 드롭다운
11개 프리셋 제공 (양방향 동기화 - 드롭다운↔텍스트영역):
- Cast to String: `string($columnS)`
- Cast to Integer: `CAST($columnS AS INT)`
- Cast to Double: `CAST($columnS AS DOUBLE)`
- Uppercase: `UPPER($columnS)`
- Lowercase: `LOWER($columnS)`
- Trim: `TRIM($columnS)`
- Replace NULL with 0: `COALESCE($columnS, 0)`
- Replace NULL with empty: `COALESCE($columnS, '')`
- Parse Date (yyyyMMdd): `TO_DATE(string($columnS), 'yyyyMMdd')`
- Regex Replace (non-digits): `REGEXP_REPLACE($columnS, '[^0-9]', '')`
- Round to 2 decimals: `ROUND($columnS, 2)`

### Keep Original Columns
- 체크 시 원본 대상 컬럼 유지 + 변환된 컬럼을 새로 추가
- Output Pattern이 `$columnS`와 같으면 중복 에러

### Output Column Pattern
- `$columnS` 포함 필수 (예: `$columnS_str`, `$columnS_new`)
- keepOriginal=false 시 기본값 `$columnS`는 원래 컬럼 이름 유지

### SQL Preview
- 실시간 프리뷰 (DocumentListener 기반)
- 현재 설정 기반으로 생성될 SELECT 절을 미리 표시

### Validation Check
- `DataAwareNodeDialogPane` 상속하여 PortObject 접근
- Check 버튼 클릭 시:
  1. 로컬 검증 (컬럼 선택, 표현식 비어있지 않음, $columnS 포함)
  2. SwingWorker로 백그라운드 Spark Job 실행 (validateOnly=true)
  3. **전체 컬럼 테스트**: 모든 대상 컬럼을 한 번에 테스트
  4. **실패 시 개별 컬럼 테스트**: 어떤 컬럼이 실패했는지 식별
  5. 성공 → 초록색 + 샘플 데이터 5행 표시 / 실패 → 빨간색 (실패 컬럼 목록 + 에러)
- upstream 노드 미실행 시 "Execute the upstream node first" 에러

### syncFilterModel
- `DialogComponentColumnFilter`는 saveSettingsTo() 호출 전까지 SettingsModel을 UI와 동기화하지 않음
- `syncFilterModel()`: 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- updateSelectedColumnsInfo(), updatePreview(), runValidation(), saveSettingsTo()에서 사용

---

## 테스트 완료 항목

- [x] 기본 string 변환: `string($columnS)` → 정상
- [x] CAST 변환, COALESCE, UPPER, TRIM 등 → 정상
- [x] 단일/다수/전체 컬럼 선택 → 정상
- [x] 비대상 컬럼 보존 → 정상
- [x] 대상 컬럼 미선택 OK → 에러
- [x] SQL 표현식 빈값/플레이스홀더 미포함 OK → 에러
- [x] Output Pattern 빈값/플레이스홀더 미포함 OK → 에러
- [x] keepOriginal + 기본 패턴 → 중복 에러
- [x] Check 성공 → 초록색 + 샘플 데이터 표시
- [x] Check 실패 → 빨간색 + 실패 컬럼 식별
- [x] 템플릿 드롭다운 양방향 동기화 → 정상
- [x] keepOriginal ON + 패턴 → 원본 유지 + 새 컬럼 추가
- [x] SQL Preview 실시간 업데이트 → 정상
- [x] 설정 저장/재로드 → 설정값 유지
- [x] 빈 DataFrame (0행) → 정상
- [x] 특수문자 컬럼명 → 백틱 이스케이프 정상
- [x] 처음 사용 시 → syncFilterModel로 정상 동작
- [x] 선택 컬럼 요약 표시 → 정상

---
---

# Spark String to Number Node (신규 개발)

## 개요

String 컬럼을 숫자 타입(Integer, Double, Long)으로 변환하는 노드.
소수점 구분자, 천 단위 구분자, d/D/f/F 접미사 처리 등을 지원한다.

- **노드명**: Spark String to Number(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator
- **다이얼로그**: WebUI (`WebUINodeFactory` + `NodeParameters`)

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark_dx.node` | 노드 UI (Factory, Model, Settings, Parameters, JobInput) |
| `org.knime.bigdata_spark3_4_dx` | Spark 3.4용 StringToNumberJob 구현 |
| `org.knime.bigdata_spark3_5_dx` | Spark 3.5용 StringToNumberJob 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.preproc.stringtonumber`)

| 파일 | 역할 |
|------|------|
| `SparkStringToNumberNodeFactory.java` | `WebUINodeFactory` + `SparkNodeFactory`. 카테고리: "row" |
| `SparkStringToNumberNodeModel.java` | 노드 모델. configure에서 컬럼/구분자 유효성 검증, execute에서 Spark Job 실행 |
| `SparkStringToNumberSettings.java` | 설정 (include, parse_type, decimal_separator, thousands_separator, generic_parse, fail_on_error) |
| `SparkStringToNumberNodeParameters.java` | WebUI 다이얼로그 (2섹션: Column Selection, Parsing Options) |
| `SparkStringToNumberJobInput.java` | Job 입력 VO. `@SparkClass` |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `StringToNumberJob.java` | `SparkJob` 구현. 단일 `select()`로 모든 컬럼 변환. failOnError 시 검증 패스 포함 |
| `StringToNumberJobRunFactory.java` | Job 실행 팩토리 |
| `StringToNumberJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | 숫자 타입으로 변환된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `include` | ColumnFilter | [] | 변환 대상 String 컬럼 |
| `parse_type` | String | "DOUBLE" | 대상 숫자 타입 (INTEGER / DOUBLE / LONG) |
| `decimal_separator` | String | "." | 소수점 구분자 (빈값 = 기본 ".") |
| `thousands_separator` | String | "" | 천 단위 구분자 (빈값 = 비활성) |
| `generic_parse` | Boolean | false | d/D/f/F 접미사 허용 여부 |
| `fail_on_error` | Boolean | false | 변환 실패 시 노드 에러 |

---

## WebUI NodeParameters 구조

### 섹션 레이아웃

```
Section 1: Column Selection
  m_inclCols: ColumnFilterWidget (String 타입 컬럼만 표시)

Section 2: Parsing Options
  m_parseType: ValueSwitchWidget (INTEGER / DOUBLE / LONG)
  m_decimalSep: String 필드 (기본 ".")
  m_thousandsSep: String 필드 (기본 "")
  m_genericParse: Boolean 체크박스
  m_failOnError: Boolean 체크박스
```

### Persistors
- `IncludedColumnsPersistor extends LegacyColumnFilterPersistor` — 구 InclList 포맷 하위 호환
- `ParseTypePersistor` — ParseTypeOption enum <-> String 매핑

### ColumnChoicesProvider
- `SparkStringColumnChoicesProvider` — `StringValue.class` 호환 컬럼만 필터

---

## Spark Job 변환 파이프라인 (buildConversionExpr)

단일 `select()` 호출로 모든 컬럼을 한 번에 변환 (반복 `withColumn()` 대신):

```
1. Blank Handling: 빈 문자열/공백 -> null
2. Thousands Separator: 구분자 문자 제거 (regexp_replace)
3. Decimal Separator: 커스텀 구분자 -> "." 변환 (DOUBLE 타입일 때)
   - "."이 이미 포함된 값 + 커스텀 구분자 -> null (모호한 값)
4. Suffix Check: genericParse=false면 d/D/f/F 접미사 -> null
5. Trim: 앞뒤 공백 제거
6. Cast: 대상 Spark 타입으로 캐스팅 (무효값 -> null)
```

### failOnError=true 모드

3단계 검증:
1. 검증 DataFrame 생성: 원본 + 변환값(`_stn_conv_`) + 유효성 플래그(`_stn_valid_`)
2. 각 대상 컬럼별로 실패 행 수 카운트 (non-empty -> null 변환 = 실패)
3. 실패 시 샘플 값 3개 수집 -> `KNIMESparkException` throw

### failOnError=false 모드 (기본)

단순 단일 패스: `select()`로 모든 변환 적용, 무효값은 null

---

## 유효성 검증

| 검증 | 에러 메시지 |
|------|------------|
| 컬럼 미선택 | "No columns selected." |
| 컬럼 미존재 | "Column 'X' not found in input table." |
| 소수점 구분자 길이 | "Decimal separator must be at most one character." |
| 천 단위 구분자 길이 | "Thousands separator must be at most one character." |
| 구분자 동일 | "Decimal separator and thousands separator must be different." |

---

## 테스트 완료 항목

- [x] INTEGER / DOUBLE / LONG 변환 -> 정상
- [x] 소수점 구분자 변경 (쉼표 등) -> 정상
- [x] 천 단위 구분자 제거 -> 정상
- [x] d/D/f/F 접미사 허용/거부 -> 정상
- [x] failOnError ON: 실패 시 샘플 값 포함 에러 -> 정상
- [x] failOnError OFF: 무효값 null 처리 -> 정상
- [x] 빈 문자열/공백 -> null 처리 -> 정상
- [x] 설정 저장/재로드 -> 설정값 유지
- [x] batch select() 방식으로 컬럼 해상도 문제 해결 -> 정상

---
---

# Spark Number to String Node (신규 개발)

## 개요

숫자 컬럼(Integer, Long, Double)을 String 타입으로 변환하는 노드.
가장 간단한 구조의 변환 노드.

- **노드명**: Spark Number to String(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator
- **다이얼로그**: WebUI (`WebUINodeFactory` + `NodeParameters`)

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark_dx.node` | 노드 UI (Factory, Model, Settings, Parameters, JobInput) |
| `org.knime.bigdata_spark3_4_dx` | Spark 3.4용 NumberToStringJob 구현 |
| `org.knime.bigdata_spark3_5_dx` | Spark 3.5용 NumberToStringJob 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.preproc.numbertostring`)

| 파일 | 역할 |
|------|------|
| `SparkNumberToStringNodeFactory.java` | `WebUINodeFactory` + `SparkNodeFactory`. 카테고리: "row" |
| `SparkNumberToStringNodeModel.java` | 노드 모델. configure에서 컬럼 존재 검증, execute에서 Spark Job 실행 |
| `SparkNumberToStringSettings.java` | 설정 (include만) |
| `SparkNumberToStringNodeParameters.java` | WebUI 다이얼로그 (1섹션: Column Selection) |
| `SparkNumberToStringJobInput.java` | Job 입력 VO. `@SparkClass` |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `NumberToStringJob.java` | `SparkJob` 구현. `withColumn(col.cast(StringType))` 반복 |
| `NumberToStringJobRunFactory.java` | Job 실행 팩토리 |
| `NumberToStringJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | String으로 변환된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `include` | ColumnFilter | [] | 변환 대상 숫자 컬럼 |

---

## WebUI NodeParameters 구조

```
Section 1: Column Selection
  m_inclCols: ColumnFilterWidget (DoubleValue 호환 컬럼만 표시)
```

### Persistors
- `IncludedColumnsPersistor extends LegacyColumnFilterPersistor` — 구 InclList 포맷 하위 호환

### ColumnChoicesProvider
- `SparkNumericColumnChoicesProvider` — `DoubleValue.class` 호환 컬럼만 필터

---

## Spark Job 로직

```java
for (colName : columns) {
    result = result.withColumn(colName, col(colName).cast(DataTypes.StringType));
}
```

- 단순 `cast(StringType)` 반복
- null 값은 null 유지
- 별도 에러 처리 없음

---

## 유효성 검증

| 검증 | 에러 메시지 |
|------|------------|
| 컬럼 미선택 | "No columns selected." |
| 컬럼 미존재 | "Column 'X' not found in input table." |

---

## 테스트 완료 항목

- [x] Integer / Double / Long -> String 변환 -> 정상
- [x] null 값 유지 -> 정상
- [x] 컬럼 미선택 -> 에러
- [x] 설정 저장/재로드 -> 설정값 유지

---
---

# Spark String to Date&Time Node (신규 개발)

## 개요

String 컬럼을 날짜/시간 타입(Date, Time, DateTime, Zoned DateTime)으로 변환하는 노드.
Java DateTimeFormatter 패턴 문법을 사용하며, Locale 설정을 지원한다.

- **노드명**: Spark String to Date&Time (Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator
- **다이얼로그**: WebUI (`WebUINodeFactory` + `NodeParameters`)

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark_dx.node` | 노드 UI (Factory, Model, Settings, Parameters, JobInput) |
| `org.knime.bigdata_spark3_4_dx` | Spark 3.4용 StringToDateTimeJob 구현 |
| `org.knime.bigdata_spark3_5_dx` | Spark 3.5용 StringToDateTimeJob 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.preproc.stringtodatetime`)

| 파일 | 역할 |
|------|------|
| `SparkStringToDateTimeNodeFactory.java` | `WebUINodeFactory` + `SparkNodeFactory`. 카테고리: "row" |
| `SparkStringToDateTimeNodeModel.java` | 노드 모델. configure에서 컬럼/포맷 유효성 검증, execute에서 Spark Job 실행 |
| `SparkStringToDateTimeSettings.java` | 설정 (include, format, output_type, locale, fail_on_error) |
| `SparkStringToDateTimeNodeParameters.java` | WebUI 다이얼로그 (2섹션: Column Selection, Type and Format) |
| `SparkStringToDateTimeJobInput.java` | Job 입력 VO. `@SparkClass` |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `StringToDateTimeJob.java` | `SparkJob` 구현. 타입별 `to_date()` / `to_timestamp()` 적용 |
| `StringToDateTimeJobRunFactory.java` | Job 실행 팩토리 |
| `StringToDateTimeJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | 날짜/시간 타입으로 변환된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `include` | ColumnFilter | [] | 변환 대상 String 컬럼 |
| `format` | String | "yyyy-MM-dd" | DateTimeFormatter 패턴 |
| `output_type` | String | "DATE" | 출력 타입 (DATE / TIME / DATE_TIME / ZONED_DATE_TIME) |
| `locale` | String | 시스템 기본 | Locale language tag (예: "en-US", "ko-KR") |
| `fail_on_error` | Boolean | false | 변환 실패 시 노드 에러 |

---

## WebUI NodeParameters 구조

### 섹션 레이아웃

```
Section 1: Column Selection
  m_inclCols: ColumnFilterWidget (String 타입 컬럼만 표시)

Section 2: Type and Format
  m_outputType: ValueSwitchWidget (Date / Time / Date&Time / Zoned Date&Time)
  m_format: String 필드 (DateTimeFormatter 패턴)
  m_locale: String 필드 (언어 태그)
  m_failOnError: Boolean 체크박스
```

### OutputTypeOption enum

```java
enum OutputTypeOption {
    DATE("Date"),
    TIME("Time"),
    DATE_TIME("Date&Time"),
    ZONED_DATE_TIME("Zoned Date&Time")
}
```

### Persistors
- `IncludedColumnsPersistor extends LegacyColumnFilterPersistor` — 구 InclList 포맷 하위 호환
- `OutputTypePersistor` — OutputTypeOption enum <-> String 매핑
- `FormatPersistor` — 포맷 문자열 저장/로드
- `LocalePersistor` — locale language tag 저장/로드
- `FailOnErrorPersistor` — boolean 저장/로드

### ColumnChoicesProvider
- `SparkStringColumnChoicesProvider` — `StringValue.class` 호환 컬럼만 필터

---

## Spark Job 변환 로직

### 타입별 Spark SQL 함수

| Output Type | Spark 함수 | 비고 |
|-------------|-----------|------|
| DATE | `to_date(col, format)` | DateType으로 변환 |
| TIME | `to_timestamp(col, format)` | Spark에 Time 전용 타입 없음. epoch date(1970-01-01) 사용 |
| DATE_TIME | `to_timestamp(col, format)` | TimestampType으로 변환 |
| ZONED_DATE_TIME | `to_timestamp(col, format)` | Spark 내부적으로 timezone 미지원 |

### failOnError=true 모드

1. 원본 값을 임시 컬럼(`_stdt_orig_`)에 저장
2. 변환 적용
3. non-null/non-blank 원본인데 변환 결과가 null인 행 카운트
4. 실패 시 샘플 값 3개 수집 -> `KNIMESparkException` throw
5. 임시 컬럼 제거

### failOnError=false 모드 (기본)

직접 `withColumn()`으로 변환 적용, 파싱 실패 시 null

---

## 유효성 검증

| 검증 | 에러 메시지 |
|------|------------|
| 컬럼 미선택 | "No columns selected." |
| 컬럼 미존재 | "Column 'X' not found in input table." |
| 포맷 비어있음 | "Format string must not be empty." |

---

## 테스트 완료 항목

- [x] Date 변환 (yyyy-MM-dd) -> 정상
- [x] Time 변환 (HH:mm:ss) -> 정상
- [x] DateTime 변환 (yyyy-MM-dd HH:mm:ss) -> 정상
- [x] Locale 변경 -> 정상
- [x] failOnError ON: 실패 시 에러 -> 정상
- [x] failOnError OFF: 파싱 실패 -> null -> 정상
- [x] 설정 저장/재로드 -> 설정값 유지

---

## 공통 기술 패턴

### DialogComponentColumnFilter 모델 동기화 문제
- KNIME의 `DialogComponentColumnFilter`는 `saveSettingsTo()` 호출 시에만 내부 `SettingsModelFilterString`을 UI 패널과 동기화함
- 다이얼로그를 처음 열거나 탭을 전환한 뒤 `getIncludeList()`를 호출하면 빈 리스트 반환될 수 있음
- **해결**: `syncFilterModels()` 헬퍼 메서드로 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- Unpivot / Multi Query 양쪽 노드에 적용

### JobOutput Number 직렬화 문제
- KNIME `JobOutput.set(key, value)`에서 `long`/`Number` 타입 직접 저장 불가
- `"Instance of Number not supported. Use dedicated methods"` 에러 발생
- **해결**: `String.valueOf(long)`으로 변환하여 저장, 읽을 때 `Long.parseLong()` 사용

### DialogComponentColumnFilter 최초 오픈 시 기본 배치 (Available 우선)
- 노드를 처음 열 때 (OK 클릭 전) 모든 컬럼이 Available(exclude) 쪽에 보이도록 하려면 `freshSettings` 방식 사용
- `CFG_CONFIGURED` 플래그(Settings 클래스에서 OK 클릭 시에만 저장)와 `m_everSavedWithOk`(Dialog 인스턴스 필드, 현 세션에서 OK 클릭 시 `true`)를 AND 조건으로 신선도 판단
- **조건**: `if (m_everSavedWithOk || settings.containsKey(CFG_CONFIGURED))` → 기존 설정 로드; 그 외 → freshSettings 생성
- freshSettings: 현재 spec의 모든 컬럼을 exclude 리스트에 넣은 `NodeSettings("defaults")`를 직접 만들어 `loadSettingsFrom()` 호출
- `SettingsModelFilterString` 두 번째 파라미터(`inclModeDefault=false`)는 EnforceExclusion; 알 수 없는 컬럼은 Available로 이동
- Unpivot: retainedColumns, valueColumns 양쪽 모두 freshSettings 적용
- Multi Query: targetColumns에 freshSettings 적용

### DialogComponentColumnFilter 컬럼 목록 비가시 문제 (Swing 레이아웃 타이밍)
- `DataAwareNodeDialogPane.loadSettingsFrom()`은 다이얼로그 표시 전에 호출됨
- 이 시점에서 내부 JScrollPane이 크기 0이기 때문에 동기적 `revalidate()` + `repaint()`는 효과 없음
- **증상**: Available 패널에 데이터는 존재하지만 화면에 보이지 않음
- **해결**: `SwingUtilities.invokeLater`로 EDT 큐 뒤로 작업 지연 + `resetSplitPaneDividers()` + `w.validate()` on window ancestor
- `JSplitPane.resetToPreferredSizes()`: 분할창 divider가 0에 고정된 경우 비율 초기화
- `resetSplitPaneDividers(container)`: 컴포넌트 트리를 재귀 탐색하여 모든 JSplitPane에 `resetToPreferredSizes()` 적용
- `SwingUtilities.getWindowAncestor(panel)`로 상위 Window를 찾아 `window.validate()` + `window.repaint()` 호출

### setShowInvalidIncludeColumns(true) 부작용
- `DialogComponentColumnFilter.setShowInvalidIncludeColumns(true)` 설정 시, include 목록에 있는 컬럼이 현재 spec에 없을 경우 빨간 테두리로 표시됨
- OK 없이 닫고 재열기 시 이전 include 목록이 현 spec과 불일치하여 Target에 빨간 테두리 + Available 비가시 현상 발생
- **해결**: 생성자에서 `setShowInvalidIncludeColumns(true)` 호출 제거 (기본값 false 유지)

---
---

# Spark Multi Query Node — WebUI 전환 (진행 중)

## 개요

`SparkMultiQueryNodeDialog.java` (Swing) → `SparkMultiQueryNodeParameters.java` (WebUI `@NodeParameters`) 로 변환 완료.
`SparkMultiQueryNodeFactory.java`도 `WebUINodeFactory` 로 변경.

---

## 변경된 파일 목록

| 파일 | 변경 내용 |
|------|-----------|
| `SparkMultiQueryNodeFactory.java` | `DefaultSparkNodeFactory` → `WebUINodeFactory<SparkMultiQueryNodeModel>`, `modelSettingsClass(SparkMultiQueryNodeParameters.class)` |
| `SparkMultiQuerySettings.java` | NameFilterConfiguration 포맷 지원 추가 (`writeColumnFilter`, `loadColumnFilter`, `validateColumnFilter` 헬퍼). 구 포맷(InclList) 하위 호환 유지 |
| `SparkMultiQueryNodeParameters.java` | 신규 WebUI 다이얼로그 클래스 (아래 상세) |
| `SparkMultiQueryNodeModel.java` | `$$varName` flow variable 치환 로직 추가 (`resolveFlowVariables()` 메서드) |

---

## SparkMultiQueryNodeParameters.java 구조

### 섹션 레이아웃
1. **Column Selection** — `@ColumnFilterWidget` (TargetColumnsPersistor로 구 포맷 하위 호환)
2. **SQL Expression** — 템플릿 드롭다운 + SQL 필드 + Flow Variable 드롭다운 + Insert 버튼
3. **Output Options** — keepOriginal, outputColumnPattern
4. **SQL Preview** — 실시간 SELECT 미리보기 (TextMessage)
5. **Validation** — Run Validation 버튼 + 결과 표시 (TextMessage)

### 주요 클래스
- `ExpressionTemplate` enum — CUSTOM + 11개 프리셋, `getSql()` 메서드
- `EphemeralTemplatePersistor` — 템플릿 저장 안 함, 항상 CUSTOM으로 초기화
- `EphemeralStringPersistor` — Flow Variable 선택 저장 안 함, 항상 "" 초기화
- `TargetColumnsPersistor extends LegacyColumnFilterPersistor` — 구 InclList 포맷 하위 호환
- `AllFlowVarsProvider implements FlowVariableChoicesProvider` — `NodeParametersInputImpl` 캐스트하여 사용 가능한 flow variable 목록 제공. `VariableTypeRegistry.getInstance().getAllTypes()` 사용
- `SqlExpressionValueProvider implements StateProvider<String>` — **`@ValueProvider`** 사용:
  - 템플릿 변경 시(`computeFromValueSupplier(TemplateRef)`) → 해당 template SQL로 교체
  - Insert 버튼 클릭 시(`computeOnButtonClick(InsertFlowVarButtonRef)`) → 현재 SQL + `$$varName` 추가
  - 템플릿 non-CUSTOM이면 항상 템플릿 우선 (Insert보다 높은 우선순위)
- `SqlPreviewProvider` — 4개 필드 변경 시 실시간 SELECT 미리보기
- `ValidationProvider` — 버튼 클릭 시 Spark Job 실행, 실패 시 컬럼별 개별 테스트. **샘플 데이터 미표시** (Unpivot과 동일)

### 필드
```java
ColumnFilter       m_targetColumns         // @ColumnFilterWidget + @Persistor(TargetColumnsPersistor)
ExpressionTemplate m_expressionTemplate    // @Persistor(EphemeralTemplatePersistor), @ValueReference(TemplateRef)
String             m_sqlExpression         // @Persist + @ValueReference + @ValueProvider(SqlExpressionValueProvider)
String             m_flowVarToInsert       // @ChoicesProvider(AllFlowVarsProvider) + @Persistor(EphemeralStringPersistor) + @ValueReference(FlowVarSelectorRef) — 드롭다운
Void               m_insertFlowVarButton   // @SimpleButtonWidget(ref=InsertFlowVarButtonRef, icon=Icon.RELOAD)
boolean            m_keepOriginalColumns
String             m_outputColumnPattern
Void               m_sqlPreview            // @TextMessage(SqlPreviewProvider)
Void               m_checkButton           // @SimpleButtonWidget(ref=CheckButtonRef, icon=Icon.RELOAD)
Void               m_validationDisplay     // @TextMessage(ValidationProvider)
```

---

## SparkMultiQueryNodeModel.java — Flow Variable 치환

```java
private static final Pattern FLOW_VAR_PATTERN =
    Pattern.compile("\\$\\$([A-Za-z_][A-Za-z0-9_.\\-]*)");

@SuppressWarnings("deprecation")
private String resolveFlowVariables(final String sql) throws InvalidSettingsException {
    if (!sql.contains("$$")) return sql;
    Map<String, FlowVariable> flowVars = getAvailableFlowVariables();
    // INTEGER → 숫자 그대로, DOUBLE → 숫자 그대로, 기타(STRING) → 'value' (싱글쿼트, '' 이스케이프)
}
```
- `executeInternal()`에서 `m_settings.getSqlExpression()` → `resolveFlowVariables()` 거쳐 jobInput에 전달

---

## 확인된 WebUI API (node.parameters 패키지)

| 확인 여부 | 항목 |
|-----------|------|
| ✅ 확인 | `@ValueReference`, `@Persistor`, `@Persist`, `@ColumnFilterWidget`, `@TextMessage`, `@SimpleButtonWidget`, `@Widget`, `@Layout`, `@Section`, `@After` |
| ✅ 확인 | `StateProviderInitializer.computeFromValueSupplier()`, `getValueSupplier()`, `computeOnButtonClick()` |
| ✅ 확인 | `LegacyColumnFilterPersistor`, `ColumnChoicesProvider`, `ColumnFilter` |
| ✅ 사용 중 | `@ValueProvider(ProviderClass.class)` — `org.knime.node.parameters.updates.ValueProvider` import |
| ✅ 확인 | `@ChoicesProvider`, `FlowVariableChoicesProvider` — flow variable 드롭다운 지원 |
| ✅ 확인 | `NodeParametersInputImpl.getAvailableInputFlowVariables(VariableType<?>...)` — flow variable 목록 접근 가능 (캐스트 필요, `@SuppressWarnings("restriction")`) |
| ❌ 없음 | `Icon.ADD` — `Icon.RELOAD` 사용할 것 |

---

## Flow Variable UI — 현재 구현 상태

### 구현 완료 (드롭다운 방식)
- `AllFlowVarsProvider implements FlowVariableChoicesProvider` — 모든 타입의 flow variable 목록 제공
- `m_flowVarToInsert` 필드에 `@ChoicesProvider(AllFlowVarsProvider.class)` 적용 → 드롭다운으로 표시
- 사용자가 드롭다운에서 flow variable 선택 → Insert 버튼 클릭 → SQL 끝에 `$$varName` 추가
- 실행 시 `resolveFlowVariables()`가 `$$varName`을 실제 값으로 치환 (STRING → 싱글쿼트, INTEGER/DOUBLE → 숫자)

### 제한 사항
- 커서 위치 삽입 불가 — `@NodeParameters` 기반에서는 커서 정보 없음, SQL 끝에 append만 가능
- expressions 노드처럼 커서 위치에 삽입하려면 `NodeDialog` + JS 프론트엔드 필요 (작업량 매우 큼)
- 현재는 드롭다운 선택 + Insert 방식으로 유지

### 참고: expressions 노드 아키텍처 (향후 커스텀 WebUI 전환 시)
- `NodeDialog` 인터페이스 직접 구현 (NOT `@NodeParameters`)
- `ScriptingNodeSettingsService` + `GenericInitialDataBuilder` — RPC로 초기 데이터 전달
- Vue/JS 커스텀 프론트엔드 (`js-src/dist/`) + Monaco Editor 번들
- `InputOutputModel.flowVariables()` + `subItemCodeAliasTemplate` (Handlebars) 으로 삽입 텍스트 생성
- 글로벌 pub/sub 이벤트 (`insertionEventHelper`) → `editor.getSelection()` 위치에 `pushEditOperations()`
- `NodeFactory implements NodeDialogFactory` → `createNodeDialog()` 에서 `NodeDialog` 반환
