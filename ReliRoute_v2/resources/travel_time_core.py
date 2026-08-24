import os, time, statistics
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, expr, when

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://dtm-0.svc-dt.dt.svc.superman.k8s:8020")
DATASET_PATH = f"{HDFS_BASE}/dataset/parquet_1"

DATA_START_DATE = datetime(2021, 6, 22)
DATA_END_DATE = datetime(2026, 5, 4)

GANTRY_DICT = {
    "Keelung": "01F0005", "Badu": "01F0029", "Wudu": "01F0061", "Xizhi": "01F0099", 
    "Donghu": "01F0147", "Neihu": "01F0153", "Yuanshan": "01F0182", "Taipei": "01F0248", 
    "Sanchong": "01F0264", "Wugu": "01F0293", "Linkou": "01F0413", "Taoyuan": "01F0492", 
    "Neili": "01F0578", "Zhongli": "01F0633", "Youshi": "01F0681", "Yangmei": "01F0699", 
    "Hukou": "01F0880", "Zhubei": "01F0928", "Hsinchu": "01F0980", "Toufen": "01F1123", 
    "Touwu": "01F1292", "Miaoli": "01F1389", "Tongluo": "01F1465", "Sanyi": "01F1572", 
    "Houli": "01F1621", "Fengyuan": "01F1699", "Daya": "01F1774", "Taichung": "01F1802", 
    "Nantun": "01F1839", "Wangtian": "01F1906", "Changhua": "01F2011", "Yuanlin": "01F2156", 
    "Beidou": "01F2249", "Xiluo": "01F2322", "Huwei": "01F2394", "Dounan": "01F2425", 
    "Dalin": "01F2514", "Minxiong": "01F2603", "Chiayi": "01F2674", "Shuishang": "01F2714", 
    "Xinying": "01F2930", "Madou": "01F3083", "Yongkang": "01F3227", "Rende": "01F3286", 
    "Luzhu": "01F3398", "Gangshan": "01F3535", "Nanzi": "01F3590", "Kaohsiung": "01F3736"
}

NETWORK_ROWS = [
    {"id": 1, "direction": "S", "gantry_id": "01F0005S"},
    {"id": 2, "direction": "S", "gantry_id": "01F0017S"},
    {"id": 3, "direction": "S", "gantry_id": "01F0029S"},
    {"id": 4, "direction": "S", "gantry_id": "01F0061S"},
    {"id": 5, "direction": "S", "gantry_id": "01F0099S"},
    {"id": 6, "direction": "S", "gantry_id": "01F0147S"},
    {"id": 7, "direction": "S", "gantry_id": "01F0153S"},
    {"id": 8, "direction": "S", "gantry_id": "01F0182S"},
    {"id": 9, "direction": "S", "gantry_id": "01F0248S"},
    {"id": 10, "direction": "S", "gantry_id": "01F0264S"},
    {"id": 11, "direction": "S", "gantry_id": "01F0293S"},
    {"id": 12, "direction": "S", "gantry_id": "01F0339S"},
    {"id": 13, "direction": "S", "gantry_id": "01F0376S"},
    {"id": 14, "direction": "S", "gantry_id": "01F0413S"},
    {"id": 15, "direction": "S", "gantry_id": "01F0467S"},
    {"id": 16, "direction": "S", "gantry_id": "01F0492S"},
    {"id": 17, "direction": "S", "gantry_id": "01F0511S"},
    {"id": 18, "direction": "S", "gantry_id": "01F0532S"},
    {"id": 19, "direction": "S", "gantry_id": "01F0557S"},
    {"id": 20, "direction": "S", "gantry_id": "01F0578S"},
    {"id": 21, "direction": "S", "gantry_id": "01F0633S"},
    {"id": 22, "direction": "S", "gantry_id": "01F0664S"},
    {"id": 23, "direction": "S", "gantry_id": "01F0681S"},
    {"id": 24, "direction": "S", "gantry_id": "01F0699S"},
    {"id": 25, "direction": "S", "gantry_id": "01F0750S"},
    {"id": 26, "direction": "S", "gantry_id": "01F0880S"},
    {"id": 27, "direction": "S", "gantry_id": "01F0928S"},
    {"id": 28, "direction": "S", "gantry_id": "01F0950S"},
    {"id": 29, "direction": "S", "gantry_id": "01F0980S"},
    {"id": 30, "direction": "S", "gantry_id": "01F1045S"},
    {"id": 31, "direction": "S", "gantry_id": "01F1123S"},
    {"id": 32, "direction": "S", "gantry_id": "01F1292S"},
    {"id": 33, "direction": "S", "gantry_id": "01F1389S"},
    {"id": 34, "direction": "S", "gantry_id": "01F1465S"},
    {"id": 35, "direction": "S", "gantry_id": "01F1572S"},
    {"id": 36, "direction": "S", "gantry_id": "01F1621S"},
    {"id": 37, "direction": "S", "gantry_id": "01F1664S"},
    {"id": 38, "direction": "S", "gantry_id": "01F1699S"},
    {"id": 39, "direction": "S", "gantry_id": "01F1774S"},
    {"id": 40, "direction": "S", "gantry_id": "01F1802S"},
    {"id": 41, "direction": "S", "gantry_id": "01F1839S"},
    {"id": 42, "direction": "S", "gantry_id": "01F1906S"},
    {"id": 43, "direction": "S", "gantry_id": "01F1960S"},
    {"id": 44, "direction": "S", "gantry_id": "01F2011S"},
    {"id": 45, "direction": "S", "gantry_id": "01F2089S"},
    {"id": 46, "direction": "S", "gantry_id": "01F2156S"},
    {"id": 47, "direction": "S", "gantry_id": "01F2249S"},
    {"id": 48, "direction": "S", "gantry_id": "01F2322S"},
    {"id": 49, "direction": "S", "gantry_id": "01F2394S"},
    {"id": 50, "direction": "S", "gantry_id": "01F2425S"},
    {"id": 51, "direction": "S", "gantry_id": "01F2472S"},
    {"id": 52, "direction": "S", "gantry_id": "01F2514S"},
    {"id": 53, "direction": "S", "gantry_id": "01F2603S"},
    {"id": 54, "direction": "S", "gantry_id": "01F2674S"},
    {"id": 55, "direction": "S", "gantry_id": "01F2714S"},
    {"id": 56, "direction": "S", "gantry_id": "01F2827S"},
    {"id": 57, "direction": "S", "gantry_id": "01F2866S"},
    {"id": 58, "direction": "S", "gantry_id": "01F2930S"},
    {"id": 59, "direction": "S", "gantry_id": "01F3019S"},
    {"id": 60, "direction": "S", "gantry_id": "01F3083S"},
    {"id": 61, "direction": "S", "gantry_id": "01F3126S"},
    {"id": 62, "direction": "S", "gantry_id": "01F3185S"},
    {"id": 63, "direction": "S", "gantry_id": "01F3227S"},
    {"id": 64, "direction": "S", "gantry_id": "01F3252S"},
    {"id": 65, "direction": "S", "gantry_id": "01F3286S"},
    {"id": 66, "direction": "S", "gantry_id": "01F3366S"},
    {"id": 67, "direction": "S", "gantry_id": "01F3398S"},
    {"id": 68, "direction": "S", "gantry_id": "01F3460S"},
    {"id": 69, "direction": "S", "gantry_id": "01F3535S"},
    {"id": 70, "direction": "S", "gantry_id": "01F3561S"},
    {"id": 71, "direction": "S", "gantry_id": "01F3590S"},
    {"id": 72, "direction": "S", "gantry_id": "01F3640S"},
    {"id": 73, "direction": "S", "gantry_id": "01F3676S"},
    {"id": 74, "direction": "S", "gantry_id": "01F3686S"},
    {"id": 75, "direction": "S", "gantry_id": "01F3736S"},
    {"id": 76, "direction": "N", "gantry_id": "01F3736N"},
    {"id": 77, "direction": "N", "gantry_id": "01F3696N"},
    {"id": 78, "direction": "N", "gantry_id": "01F3676N"},
    {"id": 79, "direction": "N", "gantry_id": "01F3640N"},
    {"id": 80, "direction": "N", "gantry_id": "01F3590N"},
    {"id": 81, "direction": "N", "gantry_id": "01F3559N"},
    {"id": 82, "direction": "N", "gantry_id": "01F3535N"},
    {"id": 83, "direction": "N", "gantry_id": "01F3460N"},
    {"id": 84, "direction": "N", "gantry_id": "01F3398N"},
    {"id": 85, "direction": "N", "gantry_id": "01F3366N"},
    {"id": 86, "direction": "N", "gantry_id": "01F3286N"},
    {"id": 87, "direction": "N", "gantry_id": "01F3252N"},
    {"id": 88, "direction": "N", "gantry_id": "01F3227N"},
    {"id": 89, "direction": "N", "gantry_id": "01F3185N"},
    {"id": 90, "direction": "N", "gantry_id": "01F3126N"},
    {"id": 91, "direction": "N", "gantry_id": "01F3083N"},
    {"id": 92, "direction": "N", "gantry_id": "01F3019N"},
    {"id": 93, "direction": "N", "gantry_id": "01F2930N"},
    {"id": 94, "direction": "N", "gantry_id": "01F2866N"},
    {"id": 95, "direction": "N", "gantry_id": "01F2827N"},
    {"id": 96, "direction": "N", "gantry_id": "01F2714N"},
    {"id": 97, "direction": "N", "gantry_id": "01F2674N"},
    {"id": 98, "direction": "N", "gantry_id": "01F2603N"},
    {"id": 99, "direction": "N", "gantry_id": "01F2514N"},
    {"id": 100, "direction": "N", "gantry_id": "01F2472N"},
    {"id": 101, "direction": "N", "gantry_id": "01F2425N"},
    {"id": 102, "direction": "N", "gantry_id": "01F2394N"},
    {"id": 103, "direction": "N", "gantry_id": "01F2322N"},
    {"id": 104, "direction": "N", "gantry_id": "01F2249N"},
    {"id": 105, "direction": "N", "gantry_id": "01F2156N"},
    {"id": 106, "direction": "N", "gantry_id": "01F2089N"},
    {"id": 107, "direction": "N", "gantry_id": "01F2011N"},
    {"id": 108, "direction": "N", "gantry_id": "01F1960N"},
    {"id": 109, "direction": "N", "gantry_id": "01F1906N"},
    {"id": 110, "direction": "N", "gantry_id": "01F1839N"},
    {"id": 111, "direction": "N", "gantry_id": "01F1802N"},
    {"id": 112, "direction": "N", "gantry_id": "01F1774N"},
    {"id": 113, "direction": "N", "gantry_id": "01F1699N"},
    {"id": 114, "direction": "N", "gantry_id": "01F1664N"},
    {"id": 115, "direction": "N", "gantry_id": "01F1621N"},
    {"id": 116, "direction": "N", "gantry_id": "01F1572N"},
    {"id": 117, "direction": "N", "gantry_id": "01F1465N"},
    {"id": 118, "direction": "N", "gantry_id": "01F1389N"},
    {"id": 119, "direction": "N", "gantry_id": "01F1292N"},
    {"id": 120, "direction": "N", "gantry_id": "01F1123N"},
    {"id": 121, "direction": "N", "gantry_id": "01F1045N"},
    {"id": 122, "direction": "N", "gantry_id": "01F0979N"},
    {"id": 123, "direction": "N", "gantry_id": "01F0956N"},
    {"id": 124, "direction": "N", "gantry_id": "01F0928N"},
    {"id": 125, "direction": "N", "gantry_id": "01F0880N"},
    {"id": 126, "direction": "N", "gantry_id": "01F0750N"},
    {"id": 127, "direction": "N", "gantry_id": "01F0699N"},
    {"id": 128, "direction": "N", "gantry_id": "01F0681N"},
    {"id": 129, "direction": "N", "gantry_id": "01F0664N"},
    {"id": 130, "direction": "N", "gantry_id": "01F0633N"},
    {"id": 131, "direction": "N", "gantry_id": "01F0584N"},
    {"id": 132, "direction": "N", "gantry_id": "01F0557N"},
    {"id": 133, "direction": "N", "gantry_id": "01F0532N"},
    {"id": 134, "direction": "N", "gantry_id": "01F0511N"},
    {"id": 135, "direction": "N", "gantry_id": "01F0492N"},
    {"id": 136, "direction": "N", "gantry_id": "01F0467N"},
    {"id": 137, "direction": "N", "gantry_id": "01F0413N"},
    {"id": 138, "direction": "N", "gantry_id": "01F0376N"},
    {"id": 139, "direction": "N", "gantry_id": "01F0340N"},
    {"id": 140, "direction": "N", "gantry_id": "01F0293N"},
    {"id": 141, "direction": "N", "gantry_id": "01F0256N"},
    {"id": 142, "direction": "N", "gantry_id": "01F0233N"},
    {"id": 143, "direction": "N", "gantry_id": "01F0213N"},
    {"id": 144, "direction": "N", "gantry_id": "01F0153N"},
    {"id": 145, "direction": "N", "gantry_id": "01F0147N"},
    {"id": 146, "direction": "N", "gantry_id": "01F0099N"},
    {"id": 147, "direction": "N", "gantry_id": "01F0061N"},
    {"id": 148, "direction": "N", "gantry_id": "01F0029N"},
    {"id": 149, "direction": "N", "gantry_id": "01F0017N"},
    {"id": 150, "direction": "N", "gantry_id": "01F0005N"}
]

# 門架路段經緯度對照表（由 network_structure.csv 相鄰門架配對 + location.csv 產生，
# 共 148 段 = 南下 74 段 + 北上 74 段，涵蓋本程式所有可能組出的路段）
SEGMENT_LOCATIONS = {
    "01F0005S_01F0017S": {"start_lon": 121.731764, "start_lat": 25.118786, "end_lon": 121.725906, "end_lat": 25.109567},
    "01F0017S_01F0029S": {"start_lon": 121.725906, "start_lat": 25.109567, "end_lon": 121.717856, "end_lat": 25.102836},
    "01F0029S_01F0061S": {"start_lon": 121.717856, "start_lat": 25.102836, "end_lon": 121.693644, "end_lat": 25.088272},
    "01F0061S_01F0099S": {"start_lon": 121.693644, "start_lat": 25.088272, "end_lon": 121.659478, "end_lat": 25.076264},
    "01F0099S_01F0147S": {"start_lon": 121.659478, "start_lat": 25.076264, "end_lon": 121.613519, "end_lat": 25.065644},
    "01F0147S_01F0153S": {"start_lon": 121.613519, "start_lat": 25.065644, "end_lon": 121.607778, "end_lat": 25.064911},
    "01F0153S_01F0182S": {"start_lon": 121.607778, "start_lat": 25.064911, "end_lon": 121.580231, "end_lat": 25.068986},
    "01F0182S_01F0248S": {"start_lon": 121.580231, "start_lat": 25.068986, "end_lon": 121.516603, "end_lat": 25.077631},
    "01F0248S_01F0264S": {"start_lon": 121.516603, "start_lat": 25.077631, "end_lon": 121.501386, "end_lat": 25.077464},
    "01F0264S_01F0293S": {"start_lon": 121.501386, "start_lat": 25.077464, "end_lon": 121.472961, "end_lat": 25.074797},
    "01F0293S_01F0339S": {"start_lon": 121.472961, "start_lat": 25.074797, "end_lon": 121.428764, "end_lat": 25.068239},
    "01F0339S_01F0376S": {"start_lon": 121.428764, "start_lat": 25.068239, "end_lon": 121.397306, "end_lat": 25.058667},
    "01F0376S_01F0413S": {"start_lon": 121.397306, "start_lat": 25.058667, "end_lon": 121.363561, "end_lat": 25.064969},
    "01F0413S_01F0467S": {"start_lon": 121.363561, "start_lat": 25.064969, "end_lon": 121.317761, "end_lat": 25.047075},
    "01F0467S_01F0492S": {"start_lon": 121.317761, "start_lat": 25.047075, "end_lon": 121.295531, "end_lat": 25.036481},
    "01F0492S_01F0511S": {"start_lon": 121.295531, "start_lat": 25.036481, "end_lon": 121.281731, "end_lat": 25.025953},
    "01F0511S_01F0532S": {"start_lon": 121.281731, "start_lat": 25.025953, "end_lon": 121.266194, "end_lat": 25.012944},
    "01F0532S_01F0557S": {"start_lon": 121.266194, "start_lat": 25.012944, "end_lon": 121.247722, "end_lat": 24.998583},
    "01F0557S_01F0578S": {"start_lon": 121.247722, "start_lat": 24.998583, "end_lon": 121.233197, "end_lat": 24.984414},
    "01F0578S_01F0633S": {"start_lon": 121.233197, "start_lat": 24.984414, "end_lon": 121.195594, "end_lat": 24.949281},
    "01F0633S_01F0664S": {"start_lon": 121.195594, "start_lat": 24.949281, "end_lon": 121.176919, "end_lat": 24.929544},
    "01F0664S_01F0681S": {"start_lon": 121.176919, "start_lat": 24.929544, "end_lon": 121.167586, "end_lat": 24.917967},
    "01F0681S_01F0699S": {"start_lon": 121.167586, "start_lat": 24.917967, "end_lon": 121.159761, "end_lat": 24.903297},
    "01F0699S_01F0750S": {"start_lon": 121.159761, "start_lat": 24.903297, "end_lon": 121.111731, "end_lat": 24.893436},
    "01F0750S_01F0880S": {"start_lon": 121.111731, "start_lat": 24.893436, "end_lon": 121.019031, "end_lat": 24.848719},
    "01F0880S_01F0928S": {"start_lon": 121.019031, "start_lat": 24.848719, "end_lon": 121.010306, "end_lat": 24.808453},
    "01F0928S_01F0950S": {"start_lon": 121.010306, "start_lat": 24.808453, "end_lon": 121.005247, "end_lat": 24.789622},
    "01F0950S_01F0980S": {"start_lon": 121.005247, "start_lat": 24.789622, "end_lon": 120.998861, "end_lat": 24.763586},
    "01F0980S_01F1045S": {"start_lon": 120.998861, "start_lat": 24.763586, "end_lon": 120.951897, "end_lat": 24.727131},
    "01F1045S_01F1123S": {"start_lon": 120.951897, "start_lat": 24.727131, "end_lon": 120.904267, "end_lat": 24.679317},
    "01F1123S_01F1292S": {"start_lon": 120.904267, "start_lat": 24.679317, "end_lon": 120.838925, "end_lat": 24.550442},
    "01F1292S_01F1389S": {"start_lon": 120.838925, "start_lat": 24.550442, "end_lon": 120.781903, "end_lat": 24.485919},
    "01F1389S_01F1465S": {"start_lon": 120.781903, "start_lat": 24.485919, "end_lon": 120.775731, "end_lat": 24.420514},
    "01F1465S_01F1572S": {"start_lon": 120.775731, "start_lat": 24.420514, "end_lon": 120.72085, "end_lat": 24.339147},
    "01F1572S_01F1621S": {"start_lon": 120.72085, "start_lat": 24.339147, "end_lon": 120.697325, "end_lat": 24.301186},
    "01F1621S_01F1664S": {"start_lon": 120.697325, "start_lat": 24.301186, "end_lon": 120.691956, "end_lat": 24.262647},
    "01F1664S_01F1699S": {"start_lon": 120.691956, "start_lat": 24.262647, "end_lon": 120.686625, "end_lat": 24.231536},
    "01F1699S_01F1774S": {"start_lon": 120.686625, "start_lat": 24.231536, "end_lon": 120.635611, "end_lat": 24.184592},
    "01F1774S_01F1802S": {"start_lon": 120.635611, "start_lat": 24.184592, "end_lon": 120.621933, "end_lat": 24.163706},
    "01F1802S_01F1839S": {"start_lon": 120.621933, "start_lat": 24.163706, "end_lon": 120.617247, "end_lat": 24.130767},
    "01F1839S_01F1906S": {"start_lon": 120.617247, "start_lat": 24.130767, "end_lon": 120.568131, "end_lat": 24.113572},
    "01F1906S_01F1960S": {"start_lon": 120.568131, "start_lat": 24.113572, "end_lon": 120.52785, "end_lat": 24.085622},
    "01F1960S_01F2011S": {"start_lon": 120.52785, "start_lat": 24.085622, "end_lon": 120.522786, "end_lat": 24.040639},
    "01F2011S_01F2089S": {"start_lon": 120.522786, "start_lat": 24.040639, "end_lon": 120.506117, "end_lat": 23.971794},
    "01F2089S_01F2156S": {"start_lon": 120.506117, "start_lat": 23.971794, "end_lon": 120.49765, "end_lat": 23.912769},
    "01F2156S_01F2249S": {"start_lon": 120.49765, "start_lat": 23.912769, "end_lon": 120.484881, "end_lat": 23.830569},
    "01F2249S_01F2322S": {"start_lon": 120.484881, "start_lat": 23.830569, "end_lon": 120.469342, "end_lat": 23.766328},
    "01F2322S_01F2394S": {"start_lon": 120.469342, "start_lat": 23.766328, "end_lon": 120.474917, "end_lat": 23.702603},
    "01F2394S_01F2425S": {"start_lon": 120.474917, "start_lat": 23.702603, "end_lon": 120.462281, "end_lat": 23.677375},
    "01F2425S_01F2472S": {"start_lon": 120.462281, "start_lat": 23.677375, "end_lon": 120.443114, "end_lat": 23.639556},
    "01F2472S_01F2514S": {"start_lon": 120.443114, "start_lat": 23.639556, "end_lon": 120.435417, "end_lat": 23.601989},
    "01F2514S_01F2603S": {"start_lon": 120.435417, "start_lat": 23.601989, "end_lon": 120.405583, "end_lat": 23.526739},
    "01F2603S_01F2674S": {"start_lon": 120.405583, "start_lat": 23.526739, "end_lon": 120.3777, "end_lat": 23.469403},
    "01F2674S_01F2714S": {"start_lon": 120.3777, "start_lat": 23.469403, "end_lon": 120.361561, "end_lat": 23.436367},
    "01F2714S_01F2827S": {"start_lon": 120.361561, "start_lat": 23.436367, "end_lon": 120.322192, "end_lat": 23.347469},
    "01F2827S_01F2866S": {"start_lon": 120.322192, "start_lat": 23.347469, "end_lon": 120.298703, "end_lat": 23.320994},
    "01F2866S_01F2930S": {"start_lon": 120.298703, "start_lat": 23.320994, "end_lon": 120.265769, "end_lat": 23.272589},
    "01F2930S_01F3019S": {"start_lon": 120.265769, "start_lat": 23.272589, "end_lon": 120.236422, "end_lat": 23.197286},
    "01F3019S_01F3083S": {"start_lon": 120.236422, "start_lat": 23.197286, "end_lon": 120.23327, "end_lat": 23.140751},
    "01F3083S_01F3126S": {"start_lon": 120.23327, "start_lat": 23.140751, "end_lon": 120.247681, "end_lat": 23.104817},
    "01F3126S_01F3185S": {"start_lon": 120.247681, "start_lat": 23.104817, "end_lon": 120.253025, "end_lat": 23.051311},
    "01F3185S_01F3227S": {"start_lon": 120.253025, "start_lat": 23.051311, "end_lon": 120.249997, "end_lat": 23.014172},
    "01F3227S_01F3252S": {"start_lon": 120.249997, "start_lat": 23.014172, "end_lon": 120.248175, "end_lat": 22.991814},
    "01F3252S_01F3286S": {"start_lon": 120.248175, "start_lat": 22.991814, "end_lon": 120.249822, "end_lat": 22.960903},
    "01F3286S_01F3366S": {"start_lon": 120.249822, "start_lat": 22.960903, "end_lon": 120.27215, "end_lat": 22.892619},
    "01F3366S_01F3398S": {"start_lon": 120.27215, "start_lat": 22.892619, "end_lon": 120.284989, "end_lat": 22.867419},
    "01F3398S_01F3460S": {"start_lon": 120.284989, "start_lat": 22.867419, "end_lon": 120.312731, "end_lat": 22.818039},
    "01F3460S_01F3535S": {"start_lon": 120.312731, "start_lat": 22.818039, "end_lon": 120.334056, "end_lat": 22.752806},
    "01F3535S_01F3561S": {"start_lon": 120.334056, "start_lat": 22.752806, "end_lon": 120.333764, "end_lat": 22.728956},
    "01F3561S_01F3590S": {"start_lon": 120.333764, "start_lat": 22.728956, "end_lon": 120.328181, "end_lat": 22.703364},
    "01F3590S_01F3640S": {"start_lon": 120.328181, "start_lat": 22.703364, "end_lon": 120.332081, "end_lat": 22.658892},
    "01F3640S_01F3676S": {"start_lon": 120.332081, "start_lat": 22.658892, "end_lon": 120.336222, "end_lat": 22.627819},
    "01F3676S_01F3686S": {"start_lon": 120.336222, "start_lat": 22.627819, "end_lon": 120.336647, "end_lat": 22.619119},
    "01F3686S_01F3736S": {"start_lon": 120.336647, "start_lat": 22.619119, "end_lon": 120.32315, "end_lat": 22.581933},
    "01F3736N_01F3696N": {"start_lon": 120.32315, "start_lat": 22.581933, "end_lon": 120.339608, "end_lat": 22.609428},
    "01F3696N_01F3676N": {"start_lon": 120.339608, "start_lat": 22.609428, "end_lon": 120.336222, "end_lat": 22.627819},
    "01F3676N_01F3640N": {"start_lon": 120.336222, "start_lat": 22.627819, "end_lon": 120.332081, "end_lat": 22.658892},
    "01F3640N_01F3590N": {"start_lon": 120.332081, "start_lat": 22.658892, "end_lon": 120.328181, "end_lat": 22.703364},
    "01F3590N_01F3559N": {"start_lon": 120.328181, "start_lat": 22.703364, "end_lon": 120.334286, "end_lat": 22.730903},
    "01F3559N_01F3535N": {"start_lon": 120.334286, "start_lat": 22.730903, "end_lon": 120.334056, "end_lat": 22.752806},
    "01F3535N_01F3460N": {"start_lon": 120.334056, "start_lat": 22.752806, "end_lon": 120.312731, "end_lat": 22.818039},
    "01F3460N_01F3398N": {"start_lon": 120.312731, "start_lat": 22.818039, "end_lon": 120.284989, "end_lat": 22.867419},
    "01F3398N_01F3366N": {"start_lon": 120.284989, "start_lat": 22.867419, "end_lon": 120.27215, "end_lat": 22.892619},
    "01F3366N_01F3286N": {"start_lon": 120.27215, "start_lat": 22.892619, "end_lon": 120.249822, "end_lat": 22.960903},
    "01F3286N_01F3252N": {"start_lon": 120.249822, "start_lat": 22.960903, "end_lon": 120.248175, "end_lat": 22.991814},
    "01F3252N_01F3227N": {"start_lon": 120.248175, "start_lat": 22.991814, "end_lon": 120.249997, "end_lat": 23.014172},
    "01F3227N_01F3185N": {"start_lon": 120.249997, "start_lat": 23.014172, "end_lon": 120.253025, "end_lat": 23.051311},
    "01F3185N_01F3126N": {"start_lon": 120.253025, "start_lat": 23.051311, "end_lon": 120.247681, "end_lat": 23.104817},
    "01F3126N_01F3083N": {"start_lon": 120.247681, "start_lat": 23.104817, "end_lon": 120.23327, "end_lat": 23.140751},
    "01F3083N_01F3019N": {"start_lon": 120.23327, "start_lat": 23.140751, "end_lon": 120.236422, "end_lat": 23.197286},
    "01F3019N_01F2930N": {"start_lon": 120.236422, "start_lat": 23.197286, "end_lon": 120.265769, "end_lat": 23.272589},
    "01F2930N_01F2866N": {"start_lon": 120.265769, "start_lat": 23.272589, "end_lon": 120.298703, "end_lat": 23.320994},
    "01F2866N_01F2827N": {"start_lon": 120.298703, "start_lat": 23.320994, "end_lon": 120.322192, "end_lat": 23.347469},
    "01F2827N_01F2714N": {"start_lon": 120.322192, "start_lat": 23.347469, "end_lon": 120.361561, "end_lat": 23.436367},
    "01F2714N_01F2674N": {"start_lon": 120.361561, "start_lat": 23.436367, "end_lon": 120.3777, "end_lat": 23.469403},
    "01F2674N_01F2603N": {"start_lon": 120.3777, "start_lat": 23.469403, "end_lon": 120.405583, "end_lat": 23.526739},
    "01F2603N_01F2514N": {"start_lon": 120.405583, "start_lat": 23.526739, "end_lon": 120.435417, "end_lat": 23.601989},
    "01F2514N_01F2472N": {"start_lon": 120.435417, "start_lat": 23.601989, "end_lon": 120.443114, "end_lat": 23.639556},
    "01F2472N_01F2425N": {"start_lon": 120.443114, "start_lat": 23.639556, "end_lon": 120.462281, "end_lat": 23.677375},
    "01F2425N_01F2394N": {"start_lon": 120.462281, "start_lat": 23.677375, "end_lon": 120.474917, "end_lat": 23.702603},
    "01F2394N_01F2322N": {"start_lon": 120.474917, "start_lat": 23.702603, "end_lon": 120.469342, "end_lat": 23.766328},
    "01F2322N_01F2249N": {"start_lon": 120.469342, "start_lat": 23.766328, "end_lon": 120.484881, "end_lat": 23.830569},
    "01F2249N_01F2156N": {"start_lon": 120.484881, "start_lat": 23.830569, "end_lon": 120.49765, "end_lat": 23.912769},
    "01F2156N_01F2089N": {"start_lon": 120.49765, "start_lat": 23.912769, "end_lon": 120.506117, "end_lat": 23.971794},
    "01F2089N_01F2011N": {"start_lon": 120.506117, "start_lat": 23.971794, "end_lon": 120.522786, "end_lat": 24.040639},
    "01F2011N_01F1960N": {"start_lon": 120.522786, "start_lat": 24.040639, "end_lon": 120.52785, "end_lat": 24.085622},
    "01F1960N_01F1906N": {"start_lon": 120.52785, "start_lat": 24.085622, "end_lon": 120.568131, "end_lat": 24.113572},
    "01F1906N_01F1839N": {"start_lon": 120.568131, "start_lat": 24.113572, "end_lon": 120.617247, "end_lat": 24.130767},
    "01F1839N_01F1802N": {"start_lon": 120.617247, "start_lat": 24.130767, "end_lon": 120.621933, "end_lat": 24.163706},
    "01F1802N_01F1774N": {"start_lon": 120.621933, "start_lat": 24.163706, "end_lon": 120.635611, "end_lat": 24.184592},
    "01F1774N_01F1699N": {"start_lon": 120.635611, "start_lat": 24.184592, "end_lon": 120.686625, "end_lat": 24.231536},
    "01F1699N_01F1664N": {"start_lon": 120.686625, "start_lat": 24.231536, "end_lon": 120.691956, "end_lat": 24.262647},
    "01F1664N_01F1621N": {"start_lon": 120.691956, "start_lat": 24.262647, "end_lon": 120.697325, "end_lat": 24.301186},
    "01F1621N_01F1572N": {"start_lon": 120.697325, "start_lat": 24.301186, "end_lon": 120.72085, "end_lat": 24.339147},
    "01F1572N_01F1465N": {"start_lon": 120.72085, "start_lat": 24.339147, "end_lon": 120.775731, "end_lat": 24.420514},
    "01F1465N_01F1389N": {"start_lon": 120.775731, "start_lat": 24.420514, "end_lon": 120.781903, "end_lat": 24.485919},
    "01F1389N_01F1292N": {"start_lon": 120.781903, "start_lat": 24.485919, "end_lon": 120.838925, "end_lat": 24.550442},
    "01F1292N_01F1123N": {"start_lon": 120.838925, "start_lat": 24.550442, "end_lon": 120.904267, "end_lat": 24.679317},
    "01F1123N_01F1045N": {"start_lon": 120.904267, "start_lat": 24.679317, "end_lon": 120.951897, "end_lat": 24.727131},
    "01F1045N_01F0979N": {"start_lon": 120.951897, "start_lat": 24.727131, "end_lon": 120.999897, "end_lat": 24.764414},
    "01F0979N_01F0956N": {"start_lon": 120.999897, "start_lat": 24.764414, "end_lon": 121.003725, "end_lat": 24.783944},
    "01F0956N_01F0928N": {"start_lon": 121.003725, "start_lat": 24.783944, "end_lon": 121.010306, "end_lat": 24.808453},
    "01F0928N_01F0880N": {"start_lon": 121.010306, "start_lat": 24.808453, "end_lon": 121.019031, "end_lat": 24.848719},
    "01F0880N_01F0750N": {"start_lon": 121.019031, "start_lat": 24.848719, "end_lon": 121.111731, "end_lat": 24.893436},
    "01F0750N_01F0699N": {"start_lon": 121.111731, "start_lat": 24.893436, "end_lon": 121.159761, "end_lat": 24.903297},
    "01F0699N_01F0681N": {"start_lon": 121.159761, "start_lat": 24.903297, "end_lon": 121.167586, "end_lat": 24.917967},
    "01F0681N_01F0664N": {"start_lon": 121.167586, "start_lat": 24.917967, "end_lon": 121.176919, "end_lat": 24.929544},
    "01F0664N_01F0633N": {"start_lon": 121.176919, "start_lat": 24.929544, "end_lon": 121.195594, "end_lat": 24.949281},
    "01F0633N_01F0584N": {"start_lon": 121.195594, "start_lat": 24.949281, "end_lon": 121.228944, "end_lat": 24.980558},
    "01F0584N_01F0557N": {"start_lon": 121.228944, "start_lat": 24.980558, "end_lon": 121.247722, "end_lat": 24.998583},
    "01F0557N_01F0532N": {"start_lon": 121.247722, "start_lat": 24.998583, "end_lon": 121.266194, "end_lat": 25.012944},
    "01F0532N_01F0511N": {"start_lon": 121.266194, "start_lat": 25.012944, "end_lon": 121.281731, "end_lat": 25.025953},
    "01F0511N_01F0492N": {"start_lon": 121.281731, "start_lat": 25.025953, "end_lon": 121.295531, "end_lat": 25.036481},
    "01F0492N_01F0467N": {"start_lon": 121.295531, "start_lat": 25.036481, "end_lon": 121.317761, "end_lat": 25.047075},
    "01F0467N_01F0413N": {"start_lon": 121.317761, "start_lat": 25.047075, "end_lon": 121.363561, "end_lat": 25.064969},
    "01F0413N_01F0376N": {"start_lon": 121.363561, "start_lat": 25.064969, "end_lon": 121.397306, "end_lat": 25.058667},
    "01F0376N_01F0340N": {"start_lon": 121.397306, "start_lat": 25.058667, "end_lon": 121.428056, "end_lat": 25.068},
    "01F0340N_01F0293N": {"start_lon": 121.428056, "start_lat": 25.068, "end_lon": 121.472961, "end_lat": 25.074797},
    "01F0293N_01F0256N": {"start_lon": 121.472961, "start_lat": 25.074797, "end_lon": 121.509106, "end_lat": 25.078067},
    "01F0256N_01F0233N": {"start_lon": 121.509106, "start_lat": 25.078067, "end_lon": 121.530703, "end_lat": 25.073019},
    "01F0233N_01F0213N": {"start_lon": 121.530703, "start_lat": 25.073019, "end_lon": 121.550358, "end_lat": 25.073053},
    "01F0213N_01F0153N": {"start_lon": 121.550358, "start_lat": 25.073053, "end_lon": 121.607778, "end_lat": 25.064911},
    "01F0153N_01F0147N": {"start_lon": 121.607778, "start_lat": 25.064911, "end_lon": 121.613519, "end_lat": 25.065644},
    "01F0147N_01F0099N": {"start_lon": 121.613519, "start_lat": 25.065644, "end_lon": 121.659422, "end_lat": 25.076044},
    "01F0099N_01F0061N": {"start_lon": 121.659422, "start_lat": 25.076044, "end_lon": 121.693644, "end_lat": 25.088272},
    "01F0061N_01F0029N": {"start_lon": 121.693644, "start_lat": 25.088272, "end_lon": 121.717856, "end_lat": 25.102836},
    "01F0029N_01F0017N": {"start_lon": 121.717856, "start_lat": 25.102836, "end_lon": 121.725906, "end_lat": 25.109567},
    "01F0017N_01F0005N": {"start_lon": 121.725906, "start_lat": 25.109567, "end_lon": 121.731636, "end_lat": 25.118311},
}

WEEKDAY_MAP = {
    'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4,
    'Friday': 5, 'Saturday': 6, 'Sunday': 7,
    '星期一': 1, '星期二': 2, '星期三': 3, '星期四': 4,
    '星期五': 5, '星期六': 6, '星期日': 7, '星期天': 7,
    '週一': 1, '週二': 2, '週三': 3, '週四': 4,
    '週五': 5, '週六': 6, '週日': 7,
}

def get_route_sequence(origin, destination):
    """
    不依賴 Spark 與 HDFS，直接使用寫死的 Python 記憶體資料計算路徑
    """
    # 從本地 Dict 獲取起終點的 base_id [cite: 1]
    orig_base_id = GANTRY_DICT.get(origin)
    dest_base_id = GANTRY_DICT.get(destination)

    if not orig_base_id or not dest_base_id:
        raise ValueError(f"找不到起點或終點: {origin} -> {destination}")

    # 判斷方向
    orig_mileage = int(orig_base_id[3:])
    dest_mileage = int(dest_base_id[3:])
    if orig_mileage < dest_mileage:
        direction = 'S'
    else:
        direction = 'N'

    actual_orig_id = f"{orig_base_id}{direction}"
    actual_dest_id = f"{dest_base_id}{direction}"

    print(f"[*] origin: {actual_orig_id}")
    print(f"[*] destination: {actual_dest_id}")

    # 用純 Python 本地尋找對應的 id 範圍 [cite: 2]
    orig_seq_id = None
    dest_seq_id = None

    for row in NETWORK_ROWS:
        if row["gantry_id"] == actual_orig_id:
            orig_seq_id = row["id"]
        if row["gantry_id"] == actual_dest_id:
            dest_seq_id = row["id"]

    if orig_seq_id is None or dest_seq_id is None:
        raise ValueError(f"路網結構中找不到門架: {actual_orig_id} -> {actual_dest_id}")

    # 5. 用純 Python List Comprehension 擷取範圍內的 gantry_id（不需額外排序，保持原本順序）
    route_list = [
        row["gantry_id"] for row in NETWORK_ROWS 
        if orig_seq_id <= row["id"] <= dest_seq_id
    ]
    
    return route_list


def parse_weekday_input(raw_input: str):
    """
    是「星期幾」的話回傳 (weekday數字1~7, 該星期在資料範圍內的所有實際日期)。
    不是的話回傳 (None, [])，代表要走「明確日期清單」模式。
    """
    target_weekday = WEEKDAY_MAP.get(raw_input.strip())
    if target_weekday is None:
        return None, []

    matching_dates = []
    current_date = DATA_START_DATE
    while current_date <= DATA_END_DATE:
        if (current_date.weekday() + 1) == target_weekday:  # 0=Monday...6=Sunday -> 對齊 1~7
            matching_dates.append(f"{current_date.year}/{current_date.month}/{current_date.day}")
        current_date += timedelta(days=1)

    return target_weekday, matching_dates


def parse_and_align_depart_time(depart_time_str):
    """
    解析使用者輸入的出發時間字串，並對齊到 5 分鐘區間。
    回傳 (depart_hour, depart_minute, clean_time_str)。
    抽成共用函式，讓「Phase 3 時間窗口過濾」與「Phase 4 模擬計算」用同一份對齊邏輯，避免兩處各自解析造成不一致。
    """
    depart_hour, depart_minute = map(int, depart_time_str.split(':'))
    depart_minute = (depart_minute // 5) * 5
    clean_time_str = f"{depart_hour:02d}:{depart_minute:02d}"
    return depart_hour, depart_minute, clean_time_str


def compute_time_window(depart_hour, depart_minute, window_hours=12):
    """
    計算出發時間往後 window_hours 小時的時間窗口（以「一天中的分鐘數」表示，0~1439）。

    回傳:
        window_start_min: 窗口起點（分鐘數，即出發時間本身）
        crosses_midnight: 窗口是否跨過午夜、需要多抓「隔天」的資料
        same_day_end_min: 「當天」需要保留到的分鐘數（未跨夜時為窗口終點，跨夜時為 1439）
        next_day_end_min: 跨夜時「隔天」需要保留到的分鐘數（未跨夜時為 None）
    """
    window_start_min = depart_hour * 60 + depart_minute
    window_end_min = window_start_min + window_hours * 60  # 12 小時 = 720 分鐘

    if window_end_min <= 1440:
        return window_start_min, False, window_end_min, None
    else:
        return window_start_min, True, 1439, window_end_min - 1440


def build_segment_names(route_gantries):
    """把門架序列組成 route 欄位對應的路段名稱清單，例如 ['A_B', 'B_C', ...]"""
    return [f"{route_gantries[i]}_{route_gantries[i+1]}" for i in range(len(route_gantries) - 1)]


def load_parquet_lookup_df(spark, query_mode, target_weekday, matching_dates,
                            depart_hour, depart_minute, segment_names,
                            vehicle_type=31, window_hours=12):
    """
    讀取 Parquet 資料 (schema: date INT, weekday INT, time STRING, route STRING,
    vehicle_type INT, travel_time INT, volume INT，PARTITIONED BY year INT, month INT)。

    過濾順序經過刻意安排，重點是保留 Parquet Predicate Pushdown：
      1) 先套用 vehicle_type / route / year / month / weekday(或date) 這些「單純欄位比較」
         的過濾條件——這些都可以被 Spark 下推到 Parquet reader 層做 row-group 跳過，
         選擇性又高（route 只留 ~29 段、vehicle_type 只留 1 種），此時資料量已大幅縮小。
      2) 資料量縮小之後，才計算 time 欄位轉換出的 _time_min，並套用 [改善點 6] 的
         12 小時時間窗口過濾。

      注意：weekday/date 的過濾「絕對不能」用 OR 直接和 _time_min（衍生欄位、無法下推）
      混在同一個條件式裡——只要 OR 的任一邊有不可下推的條件，Spark 就會放棄下推
      整個 OR 運算式，等於連本來很便宜的 weekday 過濾都失去 Parquet 層級的
      row-group 跳過能力，導致要整批解壓縮資料才能過濾（這正是上一版變慢的原因）。
      這裡改用 when/otherwise 讓 weekday/date 的過濾維持在最外層的 AND，
      时间窗口只在 when 的「值」裡出現，不影響 weekday 這層的下推。
    """
    df = spark.read.parquet(DATASET_PATH)

    if not matching_dates:
        return df.filter("1=0")

    parsed_dates = []
    for d_str in matching_dates:
        d_str = d_str.strip().replace("-", "/")
        try:
            dt_obj = datetime.strptime(d_str, "%Y/%m/%d")
            parsed_dates.append(dt_obj)
        except ValueError:
            continue

    if not parsed_dates:
        return df.filter("1=0")

    # --- 第 1 階段：先套用選擇性最高、可下推的簡單欄位過濾，把資料量先砍到最小 ---
    df = df.filter(col("vehicle_type") == lit(vehicle_type))
    df = df.filter(col("route").isin(segment_names))

    window_start_min, crosses_midnight, same_day_end_min, next_day_end_min = \
        compute_time_window(depart_hour, depart_minute, window_hours)

    if query_mode == 'weekday':
        years = list(set(d.year for d in parsed_dates))
        months = list(set(d.month for d in parsed_dates))
        df = df.filter(col("year").isin(years) & col("month").isin(months))

        next_weekday = (target_weekday % 7) + 1

        if crosses_midnight:
            # weekday 過濾維持單純 isin，不與時間條件用 OR 混合，確保仍可下推
            df = df.filter(col("weekday").isin([target_weekday, next_weekday]))
        else:
            df = df.filter(col("weekday") == lit(target_weekday))

        # --- 第 2 階段：資料量已大幅縮小，才計算 _time_min 並套用時間窗口過濾 ---
        df = df.withColumn(
            "_time_min",
            expr("CAST(split(time, ':')[0] AS INT) * 60 + CAST(split(time, ':')[1] AS INT)")
        )
        same_day_time_cond = (col("_time_min") >= lit(window_start_min)) & \
                              (col("_time_min") <= lit(same_day_end_min))

        if crosses_midnight:
            next_day_time_cond = (col("_time_min") >= lit(0)) & \
                                  (col("_time_min") <= lit(next_day_end_min))
            time_ok = when(col("weekday") == lit(target_weekday), same_day_time_cond) \
                      .when(col("weekday") == lit(next_weekday), next_day_time_cond) \
                      .otherwise(lit(False))
            lookup_df = df.filter(time_ok)
        else:
            lookup_df = df.filter(same_day_time_cond)

    else:
        # date 欄位為 INT，需搭配 year / month 一起比對，避免不同年月出現相同日期造成誤判
        current_keys = set()
        next_keys = set()
        years = set()
        months = set()

        for d in parsed_dates:
            years.add(d.year)
            months.add(d.month)
            current_keys.add(d.year * 10000 + d.month * 100 + d.day)
            if crosses_midnight:
                d_next = d + timedelta(days=1)
                years.add(d_next.year)
                months.add(d_next.month)
                next_keys.add(d_next.year * 10000 + d_next.month * 100 + d_next.day)

        df = df.filter(col("year").isin(list(years)) & col("month").isin(list(months)))

        # date_key 本身就是衍生欄位（無法下推），但這一步是在 route/vehicle_type/year/month
        # 已經先大幅縮減資料量之後才做，成本已經很低，不像 weekday 模式那樣是主要瓶頸
        df = df.withColumn(
            "_date_key",
            (col("year") * 10000 + col("month") * 100 + col("date"))
        )
        df = df.withColumn(
            "_time_min",
            expr("CAST(split(time, ':')[0] AS INT) * 60 + CAST(split(time, ':')[1] AS INT)")
        )

        same_day_time_cond = (col("_time_min") >= lit(window_start_min)) & \
                              (col("_time_min") <= lit(same_day_end_min))

        if crosses_midnight:
            next_day_time_cond = (col("_time_min") >= lit(0)) & \
                                  (col("_time_min") <= lit(next_day_end_min))
            time_ok = (col("_date_key").isin(list(current_keys)) & same_day_time_cond) | \
                      (col("_date_key").isin(list(next_keys)) & next_day_time_cond)
        else:
            time_ok = col("_date_key").isin(list(current_keys)) & same_day_time_cond

        lookup_df = df.filter(time_ok)

    return lookup_df


def calculate_travel_times(spark, lookup_df, matching_dates, route_gantries, depart_time_str):
    """
    將計算移回 Driver 端本地執行。利用大數據過濾後 collect() 回 Driver，
    改用 Python 本地迴圈與 Dict 進行高效 O(1) 模擬計算，徹底根除 Spark 循序作業排程與 Shuffle 瓶頸。
    """
    start_time = time.time()
    print("[*] calculating travel time...")
    
    # 1. 整理出該路徑所包含的所有細部路段名稱（route/vehicle_type 過濾已在
    #    load_parquet_lookup_df 中提早套用，這裡不再重複過濾，避免多一次無謂的 filter）
    segment_names = build_segment_names(route_gantries)

    # 2. 關鍵優化：先在 Spark 分散式端用 isin 篩選出目標路段，再 collect() 回 Driver
    #    此處只 select 原始欄位 (year, month, date, time, travel_time)，
    #    不在 Spark 端組合字串，欄位整合改到 collect() 之後於 Driver 端處理
    local_rows = lookup_df.filter(col("route").isin(segment_names) & (col("vehicle_type") == 31)) \
                          .select("route", "year", "month", "date", "time", "travel_time") \
                          .collect()
    collect_time = round(time.time() - start_time, 2)
    print(f"[*] Collect data to driver: {collect_time} seconds")

    # 3. 建立 Driver 端的 O(1) 快速查詢字典
    # collect() 回 Driver 後，才將 year/month/date 組合成標準無前導零格式 "YYYY/M/D"，
    # 避免 Spark 與 Python 型態不一致造成字串對不上有 bug
    start_time = time.time()
    lookup_dict = {}
    for row in local_rows:
        if row["travel_time"] is not None and row["date"] is not None and row["travel_time"] > 0:
            try:
                norm_date = f"{int(row['year'])}/{int(row['month'])}/{int(row['date'])}"
                lookup_dict[(row["route"], norm_date, row["time"])] = int(row["travel_time"])
            except (ValueError, TypeError):
                continue

    to_dict_time = round(time.time() - start_time, 2)
    print(f"[*] Loaded {len(lookup_dict)} valid data points into Driver memory dict, {to_dict_time} seconds.")

    # 解析出發時間並對齊到 5 分鐘區間（與 Phase 3 時間窗口過濾用同一份對齊邏輯）
    start_time = time.time()
    _, _, clean_time_str = parse_and_align_depart_time(depart_time_str)

    valid_results = []
    
    # 4. 在 Driver 端本地對每一天進行順序模擬計算 (完全取代原本 DataFrame 內部迴圈)
    for date_str in matching_dates:
        try:
            journey_start_dt = datetime.strptime(f"{date_str} {clean_time_str}", "%Y/%m/%d %H:%M")
        except ValueError:
            continue
            
        total_seconds = 0
        skip_date = False
        ordered_map = {}
        
        # 依序計算路徑上的每個路段
        for segment_name in segment_names:
            # 計算當前累積時間對齊至 5 分鐘（300秒）的區間偏移量 (等同於原本的 .cast("int") * 300)
            current_interval_offset = (total_seconds // 300) * 300
            
            match_found = False
            # 進行最多 13 次時間窗口嘗試 (attempt 0 ~ 12)
            for attempt in range(13):
                offset_seconds = current_interval_offset + attempt * 300
                search_ts = journey_start_dt + timedelta(seconds=offset_seconds)
                
                # 建立無前導零的 YYYY/M/D 日期與標準 HH:MM 時間 (完全與原 Spark 時間對齊)
                search_date_str = f"{search_ts.year}/{search_ts.month}/{search_ts.day}"
                search_time_str = search_ts.strftime("%H:%M")
                
                key = (segment_name, search_date_str, search_time_str)
                if key in lookup_dict:
                    travel_time = lookup_dict[key]
                    ordered_map[segment_name] = travel_time
                    total_seconds += travel_time
                    match_found = True
                    break  # 找到最早匹配的 attempt，立刻跳出窗口嘗試
                    
            if not match_found:
                skip_date = True
                break  # 該日期在此路段無資料或超時，標記跳過並中斷該日後續模擬
                
        if not skip_date:
            valid_results.append((date_str, total_seconds, ordered_map))
    
    calculate_time = round(time.time() - start_time, 2)
    print(f"[*] get {len(valid_results)} results, {calculate_time} seconds.")
    return valid_results


# =====================================================================
# API 入口 (取代原本的 main())
#
# 與測試腳本的三個關鍵差異:
#   1. spark 由外部注入 —— 用 Flask 啟動時建好的常駐 YARN session,
#      這裡「絕對不能」自己 builder,也「絕對不能」spark.stop()
#      (停掉的話下一個請求就沒 session 可用了)
#   2. 回傳 dict 給 Flask jsonify,不 print 到 stdout
#   3. 錯誤 raise ValueError,由 Resource 轉成 HTTP 404
# =====================================================================
def run_query(spark, origin, destination, weekday, depart_time):
    """回傳結果 dict;查無資料回傳 None。"""
    query_start = time.time()

    # Phase 1: 路徑門架序列(純本地運算,不碰 Spark)
    route_gantries = get_route_sequence(origin, destination)

    # Phase 2: 日期展開
    target_weekday, weekday_dates = parse_weekday_input(weekday)
    if target_weekday is not None:
        query_mode = 'weekday'
        matching_dates = weekday_dates
    else:
        query_mode = 'dates'
        matching_dates = [d.strip() for d in weekday.split(",") if d.strip()]
        if not matching_dates:
            raise ValueError(f"無法解析星期或日期: {weekday}")

    depart_hour, depart_minute, _ = parse_and_align_depart_time(depart_time)
    segment_names = build_segment_names(route_gantries)

    # Phase 3: Parquet 過濾(叢集端,含 predicate pushdown)
    lookup_df = load_parquet_lookup_df(
        spark, query_mode, target_weekday, matching_dates,
        depart_hour, depart_minute, segment_names,
        vehicle_type=31, window_hours=12
    )

    try:
        # Phase 4: collect 回 driver 後本地模擬
        results = calculate_travel_times(
            spark, lookup_df, matching_dates, route_gantries, depart_time)
    finally:
        lookup_df.unpersist()   # 只釋放這次查詢的快取,不動 SparkSession

    if not results:
        return None

    # Phase 5: 統計與組裝
    all_seconds = [r[1] for r in results]
    median_seconds = statistics.median(all_seconds)
    median_record = min(results, key=lambda x: abs(x[1] - median_seconds))
    median_date = median_record[0]
    median_segments = median_record[2]

    segments = {}
    for segment_name, seconds in median_segments.items():
        loc = SEGMENT_LOCATIONS.get(segment_name)
        segments[segment_name] = {
            "seconds": seconds,
            "start_lon": loc["start_lon"] if loc else None,
            "start_lat": loc["start_lat"] if loc else None,
            "end_lon": loc["end_lon"] if loc else None,
            "end_lat": loc["end_lat"] if loc else None,
        }

    return {
        "origin": origin,
        "destination": destination,
        "weekday": weekday,
        "depart_time": depart_time,
        "median_minutes": round(median_seconds / 60, 1),
        "median_date": median_date,
        "sample_count": len(results),
        "query_seconds": round(time.time() - query_start, 2),
        "segments": segments,
    }
