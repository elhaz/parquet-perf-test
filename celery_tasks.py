from datetime import datetime, timedelta
import sqlite3
import pytz
from pathlib import Path
import itertools
import shutil

import pandas as pd
from celery import Celery
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq

# parquet 성능 실험을 위한 celery 설정
app = Celery(main='parquet_perf_test', broker='redis://localhost:6379')

term = 70
app.conf.beat_schedule = {
    f'every-{term}-seconds': {
        'task': 'celery_tasks.test_task',
        'schedule': timedelta(seconds=term),
        'options': {
            # 'queue': 'first',
            'retry': False
        },
        'args': ()
    },
}

# 실험결과를 저장하기위한 sqlite3 설정
"""
저장할 결과물은 다음과 같음.

생성조건:
    데이터 형식: TEXT['csv', 'parquet-uncompressed', 'parquet-snappy', 'parquet-gzip']
    데이터 크기: TEXT['small', 'medium', 'large']
    데이터 복잡성: TEXT['simple', 'complex']
실험결과:
    파일 크기 : INT (Byte)
    읽기 성능 : FLOAT (sec)
    쓰기 성능 : FLOAT (sec)
    메모리 사용량 : INT (Byte)
"""
conn = sqlite3.connect('test.db')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS parquet_perf_test (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_format TEXT,
    data_size TEXT,
    data_complexity TEXT,
    file_size INT,
    read_performance FLOAT,
    write_performance FLOAT,
    modify_performance FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

@app.task
def write_result(data_format, data_size, data_complexity, file_size, read_performance, write_performance, modify_performance, created_at):
    conn = sqlite3.connect('test.db')
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO parquet_perf_test (data_format, data_size, data_complexity, file_size, read_performance, write_performance, modify_performance, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (data_format, data_size, data_complexity, file_size, read_performance, write_performance, modify_performance, created_at))
    conn.commit()
    print('write_result')
    
def read_csv(fn):
    return pd.read_csv(fn)

def read_parquet(fn):
    return pd.read_parquet(fn)
    
@app.task
def test_task():
    # write_result.apply_async(args=('csv', 'small', 'simple', 100, 0.1, 0.2, 1000, datetime.now()), queue='db_write')
    data_type = ['csv', 'parquet-uncompressed', 'parquet-snappy', 'parquet-gzip']
    data_size = ['small', 
                 'medium', 
                #  'large'
                ]
    data_complexity = ['simple', 'complex']
    
    condition = list(itertools.product(data_size, data_complexity))
    
    benchmark = {}
    for ds, dc in condition:
        filename = f'{ds}_{dc}'
        make_data = pd.DataFrame()
        read_data = pd.DataFrame()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        match ds:
            case 'small':
                new_col = 10
                new_row = 10000
            case 'medium':
                new_col = 100
                new_row = 100000
            case 'large':
                new_col = 1000
                new_row = 1000000
        
        match dc:
            case 'simple':
                # 정수형데이터, 문자열형데이터
                for i in range(new_col//2):
                    make_data[f'col_{i}'] = [i for _ in range(new_row)]
                    make_data[f'str_col_{i}'] = [f'str_{i}' for _ in range(new_row)]
            case 'complex':
                # 날짜형데이터, 부동소수점 데이터
                for i in range(new_col//2):
                    make_data[f'date_col_{i}'] = [datetime.now() for _ in range(new_row)]
                    make_data[f'float_col_{i}'] = [i * 0.1 for _ in range(new_row)]
        
        for dt in data_type:
            benchmark[(ds,dc,dt)] = {}
            write_start = datetime.now()
            if dt == 'csv':
                make_data.to_csv(f'{timestamp}_{filename}_{dt}.csv', index=False)
            else:
                if dt == 'parquet-uncompressed':
                    make_data.to_parquet(f'{timestamp}_{filename}_{dt}.parquet', index=False, compression='none')
                elif dt == 'parquet-snappy':
                    make_data.to_parquet(f'{timestamp}_{filename}_{dt}.parquet', index=False, compression='snappy')
                elif dt == 'parquet-gzip':
                    make_data.to_parquet(f'{timestamp}_{filename}_{dt}.parquet', index=False, compression='gzip')
            write_time = (datetime.now() - write_start).total_seconds()
            benchmark[(ds,dc,dt)]['write_time'] = write_time
        
            read_start = datetime.now()
            if dt == 'csv':
                read_data = read_csv(f'{timestamp}_{filename}_{dt}.csv')
            else: 
                read_data = read_parquet(f'{timestamp}_{filename}_{dt}.parquet')
            read_time = (datetime.now() - read_start).total_seconds()
            benchmark[(ds,dc,dt)]['read_time'] = read_time
            benchmark[(ds,dc,dt)]['file_size'] = Path(f'{timestamp}_{filename}_{dt}.csv').stat().st_size if dt == 'csv' else Path(f'{timestamp}_{filename}_{dt}.parquet').stat().st_size
            
        for dt in data_type:
            if dt == 'csv':
                modi_data = read_csv(f'{timestamp}_{filename}_{dt}.csv')
            else: 
                modi_data = read_parquet(f'{timestamp}_{filename}_{dt}.parquet')
                
            modify_start = datetime.now()
            
            # 데이터 복잡도에 따라 다른 연산을 수행
            # 모든 행에 대하여 문자열일경우 뒤에 'modify'를 추가
            # 모든 행에 대하여 정수일경우 1을 더함
            # 모든 행에 대하여 날짜형일경우 1일을 더함
            # 모든 행에 대하여 부동소수점일경우 0.1을 곱함
            for col in modi_data.columns:
                if col.startswith('str'):
                    # csv 를 읽은경우 해당 열의 데이터타입을 변경해주어야함.
                    if dt == 'csv': modi_data[col] = modi_data[col].astype(str)
                    modi_data[col] = modi_data[col].apply(lambda x: x + 'modify')
                elif col.startswith('col'):
                    if dt == 'csv': modi_data[col] = modi_data[col].astype(int)
                    modi_data[col] = modi_data[col].apply(lambda x: x + 1)
                elif col.startswith('date'):
                    if dt == 'csv': modi_data[col] = pd.to_datetime(modi_data[col])
                    modi_data[col] = modi_data[col].apply(lambda x: x + timedelta(days=1))
                elif col.startswith('float'):
                    if dt == 'csv': modi_data[col] = modi_data[col].astype(float)
                    modi_data[col] = modi_data[col].apply(lambda x: x * 0.1)
                    
            modify_time = (datetime.now() - modify_start).total_seconds()
            benchmark[(ds,dc,dt)]['modify_time'] = modify_time
        
        # 저장했던 파일 삭제
        for dt in data_type:
            if dt == 'csv': Path.unlink(Path(f'{timestamp}_{filename}_{dt}.csv'), missing_ok=True)
            else: Path.unlink(Path(f'{timestamp}_{filename}_{dt}.parquet'), missing_ok=True)
        
    # print(benchmark)
    for key, value in benchmark.items():
        write_result.apply_async(args=(key[2], key[0], key[1], value['file_size'], value['read_time'], value['write_time'], value['modify_time'], datetime.now()), queue='db_write')