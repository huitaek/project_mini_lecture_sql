# DB 연결 정보(로컬)
DB_INFO = "host=127.0.0.1 dbname=test user=postgres password=postgres port=5432"

# accident 테이블의 컬럼 이름
COLUMNS_ACCIDENT = ['occr_date', 'daynight', 'wday', 'death', 'serious', 'injured', 'slight', 'wound', 'sido', 'sgg',
                    'acc_type_l',
                    'acc_type', 'violation', 'road_form_l', 'road_form', 'assault', 'damaged']

# weather 테이블의 컬럼 이름
COLUMNS_WEATHER = ['weather_id','area_id',
                   'area_nm',
                   'measure_dt',
                   'temperature',
                   'rainfall',
                   'wind_speed',
                   'wind_direction',
                   'humidity',
                   'snow_drifts',
                   'ground_temperature']