from datetime import datetime, timedelta
import sqlite3
import pytz
from pathlib import Path

import pandas as pd
from celery import Celery
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq

# parquet 성능 실험을 위한 celery 설정
app = Celery(main='parquet_perf_test', broker='redis://localhost:6379')

term = 3
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
    memory_usage INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

@app.task
def write_result(data_format, data_size, data_complexity, file_size, read_performance, write_performance, memory_usage, created_at):
    conn = sqlite3.connect('test.db')
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO parquet_perf_test (data_format, data_size, data_complexity, file_size, read_performance, write_performance, memory_usage, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (data_format, data_size, data_complexity, file_size, read_performance, write_performance, memory_usage, created_at))
    conn.commit()
    print('write_result')
    
def read_csv(fn):
    return pd.read_csv(fn)

def read_parquet(fn):
    return pd.read_parquet(fn)
    
@app.task
def test_task():
    write_result.apply_async(args=('csv', 'small', 'simple', 100, 0.1, 0.2, 1000, datetime.now()), queue='db_write')
    
    data_type = ['csv', 'parquet-uncompressed', 'parquet-snappy', 'parquet-gzip']
    data_size = ['small', 'medium', 'large']
    data_complexity = ['simple', 'complex']
    
    condition = []
    for dt in data_type: 
        for ds in data_size:
            for dc in data_complexity:
                condition.append((dt, ds, dc))
    
    for dt, ds, dc in condition:
        
        
        
                