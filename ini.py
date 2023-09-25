import os
import pandas as pd



#.txt 파일이 한 개의 경우
# ini = pd.read_csv('Nori_tool_address/ini/ini1.txt', sep = '\t') #구분자는 '\t' 로 지정

#해당 경로의 파일명을 리스트형태로 가져온다
ini_path = 'ini/'
ini_list = os.listdir(ini_path)
output_file = 'ini_scans/ini_scans.csv'

#기준표인 .txt를 .csv로 만드는 함수
def ini_scans(ini_path, file_list, output_file):
    ini_dfs = []
    
    #.txt를 데이터프레임으로 읽은 후 첫 번째 컬럼에 '파일명'추가
    for ini_name in file_list:
        df = pd.read_csv(ini_path + ini_name, delimiter="\t")
        df.insert(0, '파일명', '')
        df['파일명'] = os.path.basename(ini_name)
        ini_dfs.append(df)
    
    #기존 인덱스를 무시하고 새로운 인덱스를 만든다
    ini = pd.concat(ini_dfs, ignore_index = True)
    
    #raw데이터 처리에 필요한 컬럼 생성
    ini['start1'] = ini['레지스트 영역'].str.slice(3).astype(int)
    ini['end1'] = ini['레지스트 영역'].str.slice(3).astype(int) + (ini['워드 수'].astype(int) - 1)
    ini['start2'] = ini['레지스트 영역2']
    ini['end2'] = ini['start2'].astype(int) + (ini['워드 수'].astype(int) - 1)

    #'레지스트 영역'컬럼에 ';'가 포함되어 있으면 1을, 없으면 0을 'off_set'컬럼에 작성한다
    ini['off_set'] = ini['레지스트 영역'].apply(lambda x: 1 if x.startswith(';') else 0)
    
    #데이터프레임 저장
    ini.to_csv(output_file, encoding = 'utf-8-sig', index = False)



#기준표인 .txt를 .csv로 만듬
ini_scans(ini_path, ini_list, output_file)


