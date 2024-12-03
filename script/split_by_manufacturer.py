import pandas as pd
import os

# 1. 원본 데이터 경로 및 결과 폴더 설정
input_csv_path = '../raw_data/2022-Autonomous-Vehicle-Disengagement-Report.csv'  # 원본 데이터
output_folder = '../processed_data/'  # 결과 저장 폴더

# 2. 결과 폴더 생성 (폴더가 없는 경우)
os.makedirs(output_folder, exist_ok=True)

# 3. CSV 파일 읽기 (인코딩 처리 추가)
try:
    data = pd.read_csv(input_csv_path, encoding='latin1')  # 필요한 경우 다른 인코딩(e.g., 'utf-8', 'cp1252') 사용
except UnicodeDecodeError:
    print("Encoding issue detected. Trying alternative encoding...")
    data = pd.read_csv(input_csv_path, encoding='cp1252')  # 인코딩 오류 발생 시 대체

# 4. 컬럼 이름 변경
data = data.rename(columns={
    'DATE': 'Month',
    'DISENGAGEMENT LOCATION (Interstate, Freeway, Highway, Rural Road, Street, or Parking Facility)': 'Location',
    'VIN NUMBER': 'Car',
    'DISENGAGEMENT INITIATED BY (AV System, Test Driver, Remote Operator, or Passenger)': 'TypeOfTrigger',
    'DESCRIPTION OF FACTS CAUSING DISENGAGEMENT':'Cause'
})

# 5. 제조사별로 날짜 형식 처리
try:
    # AIMOTIVE INC.와 TOYOTA RESEARCH INSTITUTE의 날짜 형식 처리 (YYYY.MM.DD. HH:MM:SS)
    specific_format_manufacturers = ['AIMOTIVE INC.']
    aimotive_mask = data['Manufacturer'].isin(specific_format_manufacturers)
    data.loc[aimotive_mask, 'Month'] = pd.to_datetime(data.loc[aimotive_mask, 'Month'], format='%Y.%m.%d. %H:%M:%S', errors='coerce').dt.strftime('%Y-%m')
    
    # GHOST AUTONOMY INC의 날짜 형식 처리 (MM/DD/YY)
    ghost_mask = data['Manufacturer'] == 'GHOST AUTONOMY INC'
    data.loc[ghost_mask, 'Month'] = pd.to_datetime(data.loc[ghost_mask, 'Month'], format='%m/%d/%y', errors='coerce').dt.strftime('%Y-%m')
    
    # Woven by Toyota, U.S., Inc.의 날짜 형식 처리 (YYYY-MM-DD HH:MM:SS)
    woven_toyota_mask = data['Manufacturer'] == 'Woven by Toyota, U.S., Inc.'
    data.loc[woven_toyota_mask, 'Month'] = pd.to_datetime(data.loc[woven_toyota_mask, 'Month'], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.strftime('%Y-%m')
    
    # 나머지 제조사의 날짜 형식 처리 (MM/DD/YYYY)
    other_mask = ~(aimotive_mask | ghost_mask | woven_toyota_mask)
    data.loc[other_mask, 'Month'] = pd.to_datetime(data.loc[other_mask, 'Month'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m')
    
    # 변환 실패 데이터 처리
    if data['Month'].isnull().any():
        print("날짜 변환 실패 데이터 발견. 기본값 'Unknown'으로 대체합니다.")
        data['Month'] = data['Month'].fillna('Unknown')
except ValueError as e:
    print("Date parsing failed:", e)
    raise ValueError("날짜 변환 중 문제가 발생했습니다. 데이터의 형식을 확인하세요.")

# 6. Manufacturer(제조사) 고유 값 추출
if 'Manufacturer' in data.columns:
    manufacturers = data['Manufacturer'].unique()
else:
    raise KeyError("The column 'Manufacturer' does not exist in the CSV file.")

# 7. 제조사별 데이터를 분리하여 저장
for manufacturer in manufacturers:
    # 제조사별 데이터 필터링
    manufacturer_data = data[data['Manufacturer'] == manufacturer]
    
    # 제조사별 데이터 검증
    if manufacturer_data.empty:
        print(f"Warning: Manufacturer '{manufacturer}' has no data.")
        continue  # 데이터가 없는 제조사는 스킵
    
    print(f"Manufacturer '{manufacturer}' 데이터 검증:")
    print(manufacturer_data[['Month', 'Manufacturer']].head())  # Month와 Manufacturer 컬럼 확인

    # Permit Number와 Manufacturer 컬럼 제거
    manufacturer_data = manufacturer_data.drop(columns=['Permit Number', 'Manufacturer','VEHICLE IS CAPABLE OF OPERATING WITHOUT A DRIVER(Yes or No)','DRIVER PRESENT(Yes or No)'], errors='ignore')
    
    # 파일 이름 생성 (특수문자 제거 및 안전한 이름으로 저장)
    sanitized_name = ''.join(e for e in manufacturer if e.isalnum() or e in (' ', '_')).strip()  # 특수문자 제거
    file_name = f"{sanitized_name}_data.csv"
    output_path = os.path.join(output_folder, file_name)
    
    # 데이터 저장
    manufacturer_data.to_csv(output_path, index=False, encoding='utf-8')  # 저장 시 utf-8로 인코딩
    print(f"{file_name} 저장 완료!")

print("모든 제조사 데이터 파일이 완료되었습니다.")
