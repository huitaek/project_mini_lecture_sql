# DB 연결 정보(로컬)
dbInfo = "host=127.0.0.1 dbname=test user=postgres password=postgres port=5432"

# accident 테이블의 컬럼 이름
columns_accident = ['occr_date', 'daynight', 'wday', 'death', 'serious', 'injured', 'slight', 'wound', 'sido', 'sgg',
                    'acc_type_l',
                    'acc_type', 'violation', 'road_form_l', 'road_form', 'assault', 'damaged']
columns_accident_string = ','.join(columns_accident)

# weather 테이블의 컬럼 이름
columns_weather = ['weather_id','area_id',
                   'area_nm',
                   'measure_dt',
                   'temperature',
                   'rainfall',
                   'wind_speed',
                   'wind_direction',
                   'humidity',
                   'snow_drifts',
                   'ground_temperature']
columns_weather_string = ','.join(columns_weather)