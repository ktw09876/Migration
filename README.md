# 데이터 Migration
* 1.0원본.csv와 기준정보.txt를 이용해서 기준에 맞는 2.0데이터를 생성, DB에 저장  
### 문제
  1 결측값을 포함한 컬럼이 데이터 타입 문제로 insert가 안됨  
  2 .csv파일의 결측값이 포함되어있는 정수형 데이터를 읽어온 경우 컬럼의 기본 데이터 타입이 'float64'
### 해결
  1) 결측값의 경우 fillna('')를 이용해 변환 후 insert  
  2) read_csv에 dtype={'결측값이 포함되어있는 컬럼': object} 옵션을 줘서 해결

## 느낀점
1. 내가 수집하고 처리한 모든 데이터는 내가 모두 제어 가능해야 한다  
  --> 내 데이터에 내가 모르는 부분이 있어서는 안된다
2. 데이터를 단순히 텍스트와 숫자로만 이해하지 않고 생성부터 활용까지 전체 과정의 이해가 필요하다  
  --> 요구사항에 맞춰서 처리만 하기보다 기획 , 처리 , 운영 전반적인 과정을 경험해보고 싶었음
3. 데이터를 수집하는 과정을 자동화 하지 못하고 수기로 처리한 게 아쉬움
4. 몇 달 뒤에도 이력을 추적할 수 있도록 수집 , 처리 , insert 모든 과정을 세세하게 기록해야한다
5. 많은 양의 데이터를 처리하는 만큼 검증하는 과정이 꼭 필요하다
6. 바쁘지만 업무를 문서화 하는 것도 중요하다 . 업무와 함께 조금씩이라도 진행하자


<img width="1000" src="https://user-images.githubusercontent.com/93371320/235865236-6d329a87-2d13-4880-bf71-40e10737fa46.png"/>
<img width="1000" src="https://user-images.githubusercontent.com/93371320/235865359-0a0ad5a7-db3b-4c23-8af4-baad337fc841.png"/>
<img width="1000" src="https://user-images.githubusercontent.com/93371320/235865366-336f81d4-7451-42ce-9a46-a5f85e998e91.png"/>
<img width="1000" src="https://user-images.githubusercontent.com/93371320/235839658-3be25dc4-499d-4f5d-86c3-4dcfa26c0a42.png"/>
<img width="1000" src="https://user-images.githubusercontent.com/93371320/235839913-758179e0-a32c-4e86-83c4-25a089a596ca.png"/>
