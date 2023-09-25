import os
import pandas as pd
import cx_Oracle as oci



#.csv 파일이 한 개 경우
# data_scan = pd.read_csv('Nori_tool_address/data/data1.csv', encoding='cp949')

#raw 데이터 경로 
raw_path = 'data/'

#해당 경로의 파일명을 리스트형태로 가져온다
raw_list = os.listdir(raw_path)
output_data = 'Outputs/data1.csv'

#기준정보 .csv 경로 
ini_scan_file = 'ini_scans/ini_scans.csv'

#기준정보가 되는 ini 불러옴
ini_scans = pd.read_csv(ini_scan_file)

#DB 접속 정보
user = 'c##test_user'
password = '1111'
dns = 'localhost:1521/xe'


#'FULL_ADDRESS'
def create_full_address(row, ini_scan):
    ini_row = ini_scan[
            ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
            (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
            (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
        ] 
    
    #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면
    if not ini_row.empty:
        row['FULL_ADDRESS'] = ini_row.iloc[0]['레지스트 영역'][:3] + str(row['cal_scan_buffer']).rjust(5,'0')

        if not pd.isna(row['비트']): 
            row['FULL_ADDRESS'] = row['FULL_ADDRESS'] + '.' + str(row['비트']).rjust(2, '0')

    return row

#'PLC_AREA'
def create_plc_area(row, ini_scan):
    ini_row = ini_scan[
            ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
            (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
            (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
        ] 
    
    #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면
    # if ini_row.isset: #.isset: 변수에 값이 있으면 true를 반환, 데이터프레임에는 적용 불가
    if not ini_row.empty:
        # row['PLC_AREA'] = ini_row['레지스트 영역'].str[:3]
        row['PLC_AREA'] = ini_row.iloc[0]['레지스트 영역'][:3]

    return row

#데이터프레임 시리즈끼리 값 비교하는 게 너무 어렵다 좀 더 공부하자
#'cal_scan_buffer'
def create_cal_scan_buffer(row, ini_scan):
    #'AD_FLAG' == 'OK'인 대상만 가져와서
    ini_row = ini_scan[
            #ctrl + F 와 비슷한 문자열 찾기 str.contains(';')
            #컬럼을 대상으로 하기 때문에 ini_scan['레지스트 영역'].str.contains(';')와 같은 형태로 사용
            #casw = False 옵션으로 대소문자 구분을 하지 않을 수 있음
            ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
            (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
            (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
        ] 
    # print(ini_row)
    
    #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면
    if not ini_row.empty: 

        #resist_area2 = ini_row['레지스트 영역2']를 해야 '레지스트 영역2' 컬럼 전체 값을 가져올 줄 알았지만
        #resist_area2 = ini_row['레지스트 영역2'].iloc[0]를 해야지 전체 값을 가져올 수 있음 왜지?
        #.iloc[0]는 첫번재 인덱스에 해당하는 값 하나만 가져오는 게 아닌가?
        resist_area_sli = int(ini_row['레지스트 영역'].str[-5:].iloc[0])
        resist_area2 = ini_row['레지스트 영역2'].iloc[0]
        # print(resist_area_sli)

        resist_area2 = ini_row['레지스트 영역2'].iloc[0]
        # print(resist_area2)

        #조건 검사
        #'레지스트 영역' 뒤 부터 5자리가 '레지스트 영역2' 보다 작은 경우
        if resist_area_sli < resist_area2:
            row['cal_scan_buffer'] = row['scan_buffer'] - (resist_area2 - resist_area_sli)

        #'레지스트 영역' 뒤 부터 5자리가 '레지스트 영역2' 보다 큰 경우
        elif resist_area_sli > resist_area2:
            row['cal_scan_buffer'] = row['scan_buffer'] + (resist_area_sli - resist_area2)

        #'레지스트 영역' 뒤 부터 5자리가 '레지스트 영역2' 와 같은 경우
        else:
            row['cal_scan_buffer'] = row['scan_buffer']

    return row

#'AD_FLAG'
def create_ad_flag(row, ini_scan):
    row['AD_FLAG'] = 'NO_TAG_GROUP'

    #raw data의 'Tag_group'값이 ini_scan '태그 그룹'에 있는지 비교, 이때 
    #ini_scan['태그 그룹']라고 하면 raw data의 1개 row와 ini_scan의 '태그 그룹' 컬럼 전체를 비교하게 됨
    #ini_scan['태그 그룹'].values라고 하면 raw data의 1개 row와 ini_scan의 '태그 그룹' 컬럼의 1개 row를 비교하게 됨
    #.empty 조건을 만족하는 행이 없는 경우 사용, 결측값/ ''/ ... 이 있어도 false를 반환한다
    if row['Tag_group'] in ini_scan['태그 그룹'].values:
        if ini_scan[(ini_scan['태그 그룹'] == row['Tag_group']) & (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2'])].empty:
            row['AD_FLAG'] = 'ERROR_SCAN_NO'

        #해당 조건에 맞는 row를 담아와서
        else:
            ini_row = ini_scan[(ini_scan['태그 그룹'] == row['Tag_group']) & (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2'])]
            # print(ini_row)

            #'off_set' 컬럼의 값을 조건 검사
            #ini_scan의 off_set컬럼의 값이 1이면 해당 범위는 'ERROR_OFF_SET'
            if ini_row['off_set'].values == 1:
                row['AD_FLAG'] = 'ERROR_OFF_SET'
            else:
                row['AD_FLAG'] = 'OK'

    return row


#데이터 처리 함수
def process_data(data_path, data_list, ini_scans):
    data_dfs = []
    
    #raw 데이터를 불러옴
    for data_name in data_list:
        df = pd.read_csv(data_path + data_name, dtype = {'비트': object}, encoding = 'cp949')

        #'라인 구분'컬럼 생성
        df['Line'] = df['Tag_group'].str.slice(0, 3)
        data_dfs.append(df)
    
    #불러온 데이터프레임을 하나로, 인덱스는 새로 생성
    data_scan = pd.concat(data_dfs, ignore_index = True)

    #필요한 컬럼 생성
    data_scan.insert(4, 'cal_scan_buffer', '')
    data_scan.insert(5, 'AD_FLAG', '')
    data_scan.insert(6, 'PLC_AREA', '')
    data_scan.insert(7, 'FULL_ADDRESS', '')

    #'AD_FLAG', 'cal_scan_buffer', 'PLC_AREA', 'FULL_ADDRESS' 값 생성
    data_scan = data_scan.apply(create_ad_flag, axis = 1, args = (ini_scans, )) 
    data_scan = data_scan.apply(create_cal_scan_buffer, axis = 1, args = (ini_scans, )) 
    data_scan = data_scan.apply(create_plc_area, axis = 1, args = (ini_scans, )) 
    data_scan = data_scan.apply(create_full_address, axis = 1, args = (ini_scans, )) 

    print(data_scan.head(30))
                
    
    return data_scan

#오라클 인서트 함수
def insert_data_to_oracle(data_scan, user, password, dns):
    connection = oci.connect(user, password, dns) #연결
    cursor = connection.cursor() #커서 -->쿼리문에 의해 반환되는 결과값을 저장하는 메모리 공간
    
    #결측값 처리
    data_scan_ok = data_scan[data_scan['AD_FLAG'] == 'OK'].fillna('')
    
    #insert 쿼리
    insert_sql = """
        INSERT INTO address VALUES(:TAG_GROUP, :TAG_NAME, :SCAN_BUFFER, :BIT, :CAL_SCAN_BUFFER, :AD_FLAG, :PLC_AREA, :FULL_ADDRESS, :LINE)
    """

    cursor.executemany(insert_sql, data_scan_ok.values.tolist()) #insert, #데이터프레임을 2차원 리스트로
    connection.commit() #전체 연결에 대한 트랜잭션 커밋
    # cursor.execute("commit") #특정 커서에 대한 커밋
    
    #들어간 갯수 확인
    cursor.execute('select count(*) from address')
    row = cursor.fetchone()
    print('삽입된 data는 총 ' + str(row[0]) + '개 입니다')
    
    cursor.close()
    connection.close()




#데이터 처리
data_scan = process_data(raw_path, raw_list, ini_scans)

#결과물 .csv 파일로 생성 
data_scan.to_csv(
             output_data
           , encoding='utf-8-sig' 
           , index = False)

#오라클 인서트
insert_data_to_oracle(data_scan, user, password, dns)

