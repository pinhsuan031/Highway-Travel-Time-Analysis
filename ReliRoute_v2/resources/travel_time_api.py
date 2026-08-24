# travel_time_api.py
# 旅行時間查詢 API 的 Resource(對應 POST /api/travel-time)
#
# 職責:純轉接層(adapter),不含任何查詢邏輯
#   入口:把 travel.js 的三種 payload 模式 (date / weekday / holiday)
#         統一轉成 travel_time_core.run_query() 的 weekday 參數
#   出口:把 core 回傳的 dict 轉成 travel.js 期望的
#         { segments: [...], total_travel_time_min: ... } 格式

from datetime import datetime

from flask import request
from flask_restful import Resource

from . import travel_time_core as core
#from . import travel_time_core1 as core

def _normalize_date(raw):
    """
    把 '2026-07-15' 或 '2021/2/8' 統一轉成無前導零的 'YYYY/M/D'。
    這一步不能省:core 的 calculate_travel_times() 用 '%Y/%m/%d' 解析日期,
    前端 date 模式送來的 '2026-07-15'(帶 - 號)直接丟進去會整批解析失敗。
    解析不了的字串回傳 None,由呼叫端決定略過或報錯。
    """
    if not raw:
        return None
    cleaned = str(raw).strip().replace("-", "/")
    try:
        dt = datetime.strptime(cleaned, "%Y/%m/%d")
    except ValueError:
        return None
    return f"{dt.year}/{dt.month}/{dt.day}"


def _build_query_arg(payload):
    """
    依 mode 把 payload 轉成 run_query() 的第四個參數(字串)。
    core 的 parse_weekday_input() 是多載設計:
      - 傳 'Monday' / '星期一' → 星期模式
      - 傳 '2026/7/15' 或 '2026/7/15,2026/7/16' → 明確日期清單模式
    轉不出來時 raise ValueError,由 post() 統一轉成 HTTP 400。
    """
    mode = payload.get("mode")

    if mode == "weekday":
        weekday = (payload.get("weekday") or "").strip()
        if not weekday:
            raise ValueError("weekday 模式缺少 weekday 欄位")
        return weekday

    if mode == "date":
        norm = _normalize_date(payload.get("date"))
        if norm is None:
            raise ValueError(f"無法解析日期: {payload.get('date')}")
        return norm

    if mode == "holiday":
        raw_dates = payload.get("dates") or []
        norm_dates = [d for d in (_normalize_date(x) for x in raw_dates) if d]
        if not norm_dates:
            raise ValueError("holiday 模式的 dates 為空或格式錯誤")
        return ",".join(norm_dates)

    raise ValueError(f"不支援的查詢模式: {mode}")


def _to_frontend_format(result):
    """
    core 回傳: segments 是 dict {路段名: {seconds, start_lon, start_lat, end_lon, end_lat}}
    travel.js 要的: segments 是 list [{travel_time_sec, start_latitude, ...}]
    Python 3.7+ dict 保留插入順序,而 core 是照路徑順序逐段塞入 ordered_map,
    所以這裡直接迭代就是正確的路段順序(地圖顏色輪替依賴這個順序)。
    """
    segments = [
        {
            "segment": name,
            "travel_time_sec": info["seconds"],
            "start_latitude": info["start_lat"],
            "start_longitude": info["start_lon"],
            "end_latitude": info["end_lat"],
            "end_longitude": info["end_lon"],
        }
        for name, info in result["segments"].items()
    ]

    return {
        "segments": segments,
        "total_travel_time_min": result["median_minutes"],
        # 以下是附加資訊,前端目前沒用到,但保留給之後顯示「取樣天數 / 中位數日期」用
        "median_date": result["median_date"],
        "sample_count": result["sample_count"],
        "query_seconds": result["query_seconds"],
    }


class TravelTimeResource(Resource):
    # spark 由 Flask 啟動時注入(常駐 YARN session),註冊方式:
    #   api.add_resource(TravelTimeResource, "/api/travel-time",
    #                    resource_class_kwargs={"spark": spark})
    # 這裡「絕對不能」自己 builder,也「絕對不能」spark.stop()
    def __init__(self, spark):
        self.spark = spark

    def post(self):
        payload = request.get_json(force=True, silent=True) or {}

        origin = payload.get("start")
        destination = payload.get("end")
        depart_time = payload.get("time")

        if not origin or not destination or not depart_time:
            return {"message": "缺少必要欄位 (start / end / time)"}, 400

        # 三種模式統一轉成 run_query 的日期/星期參數
        try:
            query_arg = _build_query_arg(payload)
        except ValueError as e:
            return {"message": str(e)}, 400

        # get_route_sequence 找不到起訖點時會 raise ValueError → 404
        try:
            result = core.run_query(
                self.spark, origin, destination, query_arg, depart_time
            )
        except ValueError as e:
            return {"message": str(e)}, 404

        if result is None:
            return {"message": "查無符合條件的旅行時間資料"}, 404

        return _to_frontend_format(result), 200
