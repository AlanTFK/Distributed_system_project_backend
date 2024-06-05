from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)

sg_lst_ = ["root.car_1"]

# create databases

ts_path_lst_ = [
    "root.car_01.ms_01",
    "root.car_01.ms_02",
    "root.car_01.ms_03",
    "root.car_01.ms_04",
]

data_type_lst_ = [
    TSDataType.FLOAT,
    TSDataType.FLOAT,
    TSDataType.FLOAT,
    TSDataType.FLOAT,
]

encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]

# session.create_multi_time_series(ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_)

for ts in ts_path_lst_:
    print(
        ts + ", checking status:" ,
        session.check_time_series_exists(ts)
    )

session.delete_time_series(ts_path_lst_)

for ts in ts_path_lst_:
    print(
        ts + ", checking status:" ,
        session.check_time_series_exists(ts)
    )

measurements_lst_ = [
    "ms_01",
    "ms_02",
    "ms_03",
    "ms_04",
]


for meas in measurements_lst_:
    print(
        meas + ", checking status:" ,
        session.check_time_series_exists("root.car_01." + meas)
    )
    
measurements_list_ = [
    ["ms_01", "ms_02", "ms_03"],
]

values_list_ = [
    [9., 55.1, 12.],
]

data_type_list_ = [data_type_lst_, data_type_lst_, data_type_lst_]
device_ids_ = ["root.car_01",]

session.insert_aligned_records(
    device_ids_, [0], measurements_list_, data_type_list_, values_list_    
)

with session.execute_query_statement(
    "select * from root.car_01.*"
) as session_data_base:
    while(session_data_base.has_next()):
        print(session_data_base.next())

tmp_time_series = []
for meas in measurements_lst_:
    tmp_time_series.append("root.car_01" + meas)

session.delete_time_series(tmp_time_series)

for meas in measurements_lst_:
    print(
        meas + ", checking status after deletion:" ,
        session.check_time_series_exists("root.sg_01.wf_01.wt_01." + meas)
    )

session.delete_storage_groups(sg_lst_)

session.close()