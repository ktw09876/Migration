import os
###현재 작업 디렉토리를 스크립트 위치로 변경
script_dir = os.path.dirname(__file__) #현재 스크립트의 절대 경로 중 폴더까지의 경로를 가져온다
os.chdir(script_dir) #해당 경로로 작업 경로를 변경한다
# print(f"script path: {os.getcwd()}") #변경된 경로 확인
import set_ini as ini

import pandas as pd
import configparser as parser
from sqlalchemy import create_engine
from sqlalchemy.types import Integer

class setAddress():
    def __init__(self):

        ###파일 경로
        self.raw_ini_path = r'../ini' #기준표 .ini 경로
        self.out_ini_path = r'../ini_scans' #.ini --> .csv 경로
        self.raw_data_path = r'../raw_data' #raw .csv 데이터 경로 
        self.out_data = r'../outputs' #결과 .csv 파일 경로

        ###DB 접속 정보
        self.host, self.port, self.dbname, self.username, self.password = self.read_conf(r'../setting/setting.ini')
    

    def read_conf(self, conf_path: str) -> None:
        config = parser.ConfigParser()
        config.read(conf_path)

        host = config['DEFAULT']['host']
        port = config['DEFAULT']['port']
        dbname = config['DEFAULT']['dbname']
        username = config['DEFAULT']['username']
        password = config['DEFAULT']['password']

        return host, port, dbname, username, password

    ###원본 데이터(.csv)를 읽어서 하나의 데이터프레임으로 만드는 함수
    def load_raw(self, raw_path: str) -> pd.DataFrame:
        data_list = os.listdir(f'{raw_path}') #리스트로 
        data_dfs = []
    
        #raw 데이터를 불러옴
        for data_name in data_list:
            df = pd.read_csv(f'{raw_path}/{data_name}', dtype = {'비트': 'Int64'}, encoding = 'cp949') 

            #'라인 구분'컬럼 생성
            df['Line'] = df['Tag_group'].str.slice(0, 3)
            data_dfs.append(df)

        ###불러온 데이터프레임을 하나로, 인덱스는 새로 생성
        df_scans = pd.concat(data_dfs, ignore_index = True)
        # print(df_scans.head(30)) #결과 확인
        
        return df_scans
    
        ###원본 데이터프레임에 필요한 데이터를 생성하는 함수
    def create_address(self, addr_df: pd.DataFrame, addr_ini: pd.DataFrame) -> pd.DataFrame:

        ###컬럼 추가
        addr_df.insert(4, 'cal_scan_buffer', None)
        addr_df.insert(5, 'AD_FLAG', None)
        addr_df.insert(6, 'PLC_AREA', None)
        addr_df.insert(7, 'FULL_ADDRESS', None)

        ###'AD_FLAG', 'cal_scan_buffer', 'PLC_AREA', 'FULL_ADDRESS' 값 생성
        addr_df = addr_df.apply(self.create_ad_flag, axis = 1, args = (addr_ini, )) 
        addr_df = addr_df.apply(self.create_cal_scan_buffer, axis = 1, args = (addr_ini, )) 
        addr_df = addr_df.apply(self.create_plc_area, axis = 1, args = (addr_ini, )) 
        addr_df = addr_df.apply(self.create_full_address, axis = 1, args = (addr_ini, )) 

        ###결측값을 포함하는 정수 타입 컬럼의 경우 자동으로 실수 타입으로 인식: 다시 정수형으로 변환
        addr_df['cal_scan_buffer'] = addr_df['cal_scan_buffer'].astype('Int64')

        print(addr_df.head(30)) #결과 확인용

        return addr_df

    #'FULL_ADDRESS'
    def create_full_address(self, row: pd.Series, ini_scan: pd.DataFrame) -> pd.Series:

        ###FULL_ADDRESS 생성 대상을 찾는다
        ini_row = ini_scan[ 
                ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
                (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
                (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
            ] 
        
        #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면 EM020000 와 같은 형태
        if not ini_row.empty:
            row['FULL_ADDRESS'] = ini_row.iloc[0]['레지스트 영역'][:3] + str(int(row['cal_scan_buffer'])).rjust(5,'0')

            #'비트'에 값이 있으면 DM040000.01 와 같은 형태
            if not pd.isna(row['비트']): 
                row['FULL_ADDRESS'] = row['FULL_ADDRESS'] + '.' + str(row['비트']).rjust(2, '0')

        return row

    ###plc_adre 생성 대상을 찾는다
    def create_plc_area(self, row: pd.Series, ini_scan: pd.DataFrame) -> pd.Series:

        ###
        ini_row = ini_scan[
                ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
                (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
                (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
            ] 
        
        #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면
        # if ini_row.isset: #.isset: 변수에 값이 있으면 true를 반환, 데이터프레임에는 적용 불가
        if not ini_row.empty:
            row['PLC_AREA'] = ini_row.iloc[0]['레지스트 영역'][:3]

        return row

    #데이터프레임 시리즈끼리 값 비교하는 게 너무 어렵다 좀 더 공부하자
    #'cal_scan_buffer'
    def create_cal_scan_buffer(self, row: pd.Series, ini_scan: pd.DataFrame) -> pd.Series:

        ###cal_scan_buffer 생성 대상을 찾는다
        ini_row = ini_scan[
                #ctrl + F 와 비슷한 문자열 찾기 str.contains(';')
                #컬럼을 대상으로 하기 때문에 ini_scan['레지스트 영역'].str.contains(';')와 같은 형태로 사용
                #casw = False 옵션으로 대소문자 구분을 하지 않을 수 있음
                ( ~ ini_scan['레지스트 영역'].str.contains(';')) & #ini_scan의 '레지스트 영역'에 ';'가 포함되어 있지 않고
                (ini_scan['태그 그룹'] == row['Tag_group']) & #data_scan의 'Tag_group'값이 ini_scan의 '태그 그룹'에 있으면서
                (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2']) #data의 'scan_buffer'값이 해당 범위에 들어오는 ini_scan 대상
            ]
        
        #ini_row가 비어있지 않으면 즉, ini_row에 값이 있으면
        if not ini_row.empty: 
            #resist_area2 = ini_row['레지스트 영역2']를 해야 '레지스트 영역2' 컬럼 전체 값을 가져올 줄 알았지만
            #resist_area2 = ini_row['레지스트 영역2'].iloc[0]를 해야지 전체 값을 가져올 수 있음 왜지?
            #.iloc[0]는 첫번재 인덱스에 해당하는 값 하나만 가져오는 게 아닌가?
            # resist_area_sli = int(ini_row['레지스트 영역'].str[-5:].iloc[0])
            resist_area_sli = int(ini_row['레지스트 영역'].str[-5:].iloc[0])
            resist_area2 = ini_row['레지스트 영역2'].iloc[0]

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
    def create_ad_flag(self, row: pd.Series, ini_scan: pd.DataFrame) -> pd.Series:
        row['AD_FLAG'] = 'NO_TAG_GROUP'

        #raw data의 'Tag_group'값이 ini_scan '태그 그룹'에 있는지 비교, 이때 
        #ini_scan['태그 그룹']라고 하면 raw data의 1개 row와 ini_scan의 '태그 그룹' 컬럼 전체를 비교하게 됨
        #ini_scan['태그 그룹'].values라고 하면 raw data의 1개 row와 ini_scan의 '태그 그룹' 컬럼의 1개 row를 비교하게 됨
        #.empty 조건을 만족하는 행이 없는 경우 사용, 결측값/ ''/ ... 이 있어도 false를 반환한다
        if row['Tag_group'] in ini_scan['태그 그룹'].values:

            #'ERROR_SCAN_NO'조건
            if ini_scan[(ini_scan['태그 그룹'] == row['Tag_group']) & (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2'])].empty:
                row['AD_FLAG'] = 'ERROR_SCAN_NO'

            #해당 조건에 맞는 row를 담아와서
            else:
                ini_row = ini_scan[(ini_scan['태그 그룹'] == row['Tag_group']) & (ini_scan['start2'] <= row['scan_buffer']) & (row['scan_buffer'] <= ini_scan['end2'])]

                #'off_set' 컬럼의 값을 조건 검사
                #ini_scan의 off_set컬럼의 값이 1이면 해당 범위는 'ERROR_OFF_SET'
                if ini_row['off_set'].values == 1:
                    row['AD_FLAG'] = 'ERROR_OFF_SET'
                else:
                    row['AD_FLAG'] = 'OK'

        return row
    
    def insert_postgre(self, postg_df: pd.DataFrame, postg_host: str, postg_port: str, postg_db: str, postg_user: str, postg_pwd: str) -> None:

        ###연결 엔진 생성
        engine = create_engine(f'postgresql+psycopg2://{postg_user}:{postg_pwd}@{postg_host}:{postg_port}/{postg_db}', connect_args={"options": "-csearch_path=public"})

        ###PostgreSQL 테이블 생성, 데이터프레임을 삽입
        postg_df.to_sql('address', engine, schema='test1', if_exists='append', index=True, dtype={'bit': Integer()})


def main():
    cli = setAddress() 

    ###기준표(.txt)에 필요한 데이터를 생성 후 하나의 .csv로 저장
    ini.ini_scans(cli.raw_ini_path, cli.out_ini_path)

    ###기준정보가 되는 ini(.csv)를 읽어서 데이터프레임으로 만듬
    ini_scans = pd.read_csv(f'{cli.out_ini_path}/ini_scans.csv')

    ###원본 데이터(.csv)를 읽어서 필요한 데이터를 생성 후 데이터프레임으로 만듬
    raw_df = cli.load_raw(cli.raw_data_path)

    ###필요한 데이터 생성
    addr_df = cli.create_address(raw_df, ini_scans)

    ###생성한 데이터프레임 로컬에 저장
    addr_df.to_csv(f'{cli.out_data}\data.csv', encoding = 'utf-8-sig')

    ###DB insert
    cli.insert_postgre(addr_df, cli.host, cli.port, cli.dbname, cli.username, cli.password)
    

if __name__ == '__main__':
    main()