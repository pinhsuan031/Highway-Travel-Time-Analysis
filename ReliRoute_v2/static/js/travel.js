/* ==========================================================================
   ReliRoute 旅行時間查詢頁 (travel.js)
   ========================================================================== */

let travelMap = null;
let travelLayer = null;
let gantryListCache = [];

// 【新增】連假 → 實際日期對照表(來自 search.js 的 holidayData)
// key 對應 travel.html 的 holidaySelect option value
// 內層 key 對應 rangeSelect option value(-2 ~ 2)
const HOLIDAY_DATES = {
    lunar_new_year: {
        "-2": ["2021/2/8", "2022/1/27", "2023/1/18", "2024/2/6", "2025/1/23", "2026/2/12"],
        "-1": ["2021/2/9", "2022/1/28", "2023/1/19", "2024/2/7", "2025/1/24", "2026/2/13"],
        "0": ["2021/2/11", "2022/1/29", "2023/1/20", "2024/2/8", "2025/1/25", "2026/2/14"],
        "1": ["2021/2/16", "2022/2/6", "2023/1/29", "2024/2/14", "2025/2/2", "2026/2/22"],
        "2": ["2021/2/17", "2022/2/7", "2023/1/30", "2024/2/15", "2025/2/3", "2026/2/23"]
    },

    peace_remembrance_day: {
        "-2": ["2021/2/25", "2022/2/24", "2023/2/23", "2025/2/26", "2026/2/25"],
        "-1": ["2021/2/26", "2022/2/25", "2023/2/24", "2025/2/27", "2026/2/26"],
        "0": ["2021/2/27", "2022/2/26", "2023/2/25", "2025/2/28", "2026/2/27"],
        "1": ["2021/3/1", "2022/2/28", "2023/2/28", "2025/3/2", "2026/3/1"],
        "2": ["2021/3/2", "2022/3/1", "2023/3/1", "2025/3/3", "2026/3/2"]
    },

    qingming_festival: {
        "-2": ["2021/3/31", "2022/3/31", "2023/3/30", "2024/4/2", "2025/4/1", "2026/4/1"],
        "-1": ["2021/4/1", "2022/4/1", "2023/3/31", "2024/4/3", "2025/4/2", "2026/4/2"],
        "0": ["2021/4/2", "2022/4/2", "2023/4/1", "2024/4/4", "2025/4/3", "2026/4/3"],
        "1": ["2021/4/5", "2022/4/5", "2023/4/5", "2024/4/7", "2025/4/6", "2026/4/6"],
        "2": ["2021/4/6", "2022/4/6", "2023/4/6", "2024/4/8", "2025/4/7", "2026/4/7"]
    },

    dragon_boat: {
        "-2": ["2021/6/10", "2022/6/1", "2023/6/20", "2024/6/6", "2025/5/28", "2026/6/17"],
        "-1": ["2021/6/11", "2022/6/2", "2023/6/21", "2024/6/7", "2025/5/29", "2026/6/18"],
        "0": ["2021/6/12", "2022/6/3", "2023/6/22", "2024/6/8", "2025/5/30", "2026/6/19"],
        "1": ["2021/6/14", "2022/6/5", "2023/6/25", "2024/6/10", "2025/6/1", "2026/6/21"],
        "2": ["2021/6/15", "2022/6/6", "2023/6/26", "2024/6/11", "2025/6/2", "2026/6/22"]
    },

    mid_autumn: {
        "-2": ["2021/9/16", "2022/9/7", "2023/9/27", "2024/9/12", "2026/9/23"],
        "-1": ["2021/9/17", "2022/9/8", "2023/9/28", "2024/9/13", "2026/9/24"],
        "0": ["2021/9/18", "2022/9/9", "2023/9/29", "2024/9/14", "2026/9/25"],
        "1": ["2021/9/21", "2022/9/11", "2023/10/1", "2024/9/17", "2026/9/28"],
        "2": ["2021/9/22", "2022/9/12", "2023/10/2", "2024/9/18", "2026/9/29"]
    },

    national_day: {
        "-2": ["2021/10/7", "2022/10/6", "2023/10/5", "2024/10/8", "2026/10/7"],
        "-1": ["2021/10/8", "2022/10/7", "2023/10/6", "2024/10/9", "2026/10/8"],
        "0": ["2021/10/9", "2022/10/8", "2023/10/7", "2024/10/10", "2026/10/9"],
        "1": ["2021/10/11", "2022/10/10", "2023/10/10", "2024/10/13", "2026/10/11"],
        "2": ["2021/10/12", "2022/10/11", "2023/10/11", "2024/10/14", "2026/10/12"]
    }
};

// 呼叫 GET /api/gantries(網址集中定義在 api.js 的 API.gantries),取得起訖點清單
function fetchGantryList() {
    return fetch(API.gantries).then((res) => res.json());
}

// 把 API 回傳的資料填入起點/終點下拉選單
// option 顯示 name,option value 使用 gantry_id
function populateGantrySelects(gantries) {
    const startSelect = document.getElementById("startSelect");
    const endSelect = document.getElementById("endSelect");

    gantries.forEach((g) => {
        startSelect.add(new Option(g.name, g.gantry_id));
        endSelect.add(new Option(g.name, g.gantry_id));
    });

    if (gantries.length > 1) {
        startSelect.value = gantries[0].gantry_id;
        endSelect.value = gantries[gantries.length - 1].gantry_id;
    }
}

function updateModeVisibility() {
    const mode = document.getElementById("modeSelect").value;
    document.getElementById("dateGroup").classList.toggle("d-none", mode !== "date");
    document.getElementById("weekdayGroup").classList.toggle("d-none", mode !== "weekday");
    document.getElementById("holidayGroup").classList.toggle("d-none", mode !== "holiday");
}

//收集資料
function buildPayload() {
    const mode = document.getElementById("modeSelect").value;
    const payload = {
        start: document.getElementById("startSelect").value,
        end: document.getElementById("endSelect").value,
        mode: mode,
        time: document.getElementById("timeInput").value
    };

    if (mode === "date") {
        payload.date = document.getElementById("dateInput").value;
    } else if (mode === "weekday") {
        payload.weekday = document.getElementById("weekdaySelect").value;
    } else if (mode === "holiday") {
        // 【修改】不再直接送 holiday / range,改成先查對照表,送出實際日期陣列
        const holiday = document.getElementById("holidaySelect").value;
        const range = document.getElementById("rangeSelect").value;
        payload.dates =
            (HOLIDAY_DATES[holiday] && HOLIDAY_DATES[holiday][range]) || [];
    }

    return payload;
}

// 路段顏色:只用來區分相鄰的 segment,不代表壅塞、風險或穩定度
// 依 ReliRoute 科技藍視覺,固定用這組藍、青、紫藍、靛色階輪流分配
const SEGMENT_COLORS = ["#2563EB", "#38BDF8", "#6366F1", "#7C3AED"];

function travelPopupHtml(segment) {
    return `<div class="popup-row">旅行時間:<strong>${segment.travel_time_sec} 秒</strong></div>`;
}

// 依 segment 陣列,逐段建立 Leaflet Polyline,放進同一個 LayerGroup 管理
// 注意:Leaflet 座標順序是 [latitude, longitude],跟 GeoJSON 的 [lon, lat] 相反
function buildSegmentLayerGroup(segments) {
    const layerGroup = L.layerGroup();

    segments.forEach((segment, index) => {
        const color = SEGMENT_COLORS[index % SEGMENT_COLORS.length];
        const latlngs = [
            [segment.start_latitude, segment.start_longitude],
            [segment.end_latitude, segment.end_longitude]
        ];

        const polyline = L.polyline(latlngs, {
            color: color,
            weight: 7,
            opacity: 0.85,
            lineCap: "round"
        });

        polyline.bindPopup(travelPopupHtml(segment));
        layerGroup.addLayer(polyline);
    });

    return layerGroup;
}

// 在右側資訊卡顯示訊息(共用:錯誤訊息、提示訊息都用這個)
function showInfoCardMessage(message, iconClass) {
    if (travelLayer) {
        travelMap.removeLayer(travelLayer);
        travelLayer = null;
    }

    document.getElementById("totalTimeContent").classList.add("d-none");

    const placeholder = document.getElementById("totalTimePlaceholder");
    placeholder.classList.remove("d-none");
    placeholder.innerHTML = `<i class="bi ${iconClass} d-block mb-2" style="font-size: 1.6rem;"></i>${message}`;
}

function showNoDataMessage(message) {
    showInfoCardMessage(message, "bi-exclamation-circle");
}

// 檢查查詢條件是否合法,合法回傳 null,不合法回傳錯誤訊息文字
function validatePayload(payload) {
    if (!payload.start) return "起點不可為空";
    if (!payload.end) return "終點不可為空";
    if (payload.start === payload.end) return "起點與終點不可相同";
    if (!payload.time) return "時間不可為空";

    if (payload.mode === "date" && !payload.date) return "日期不可為空";
    if (payload.mode === "weekday" && !payload.weekday) return "星期不可為空";
    // 【修改】連假模式改成檢查對照表有沒有查到日期
    if (payload.mode === "holiday" && (!payload.dates || payload.dates.length === 0)) {
        return "查無此連假區間的日期資料";
    }

    return null;
}

function updateTotalTimePanel(totalMinutes) {
    document.getElementById("totalTimePlaceholder").classList.add("d-none");
    document.getElementById("totalTimeContent").classList.remove("d-none");
    document.getElementById("totalTimeValue").textContent = totalMinutes;
}

function performSearch() {
    // 每次新查詢前,先清掉上一輪畫的路段 Layer

    if (travelLayer) {
        travelMap.removeLayer(travelLayer);
        travelLayer = null;
    }

    const payload = buildPayload();
    const errorMessage = validatePayload(payload);

    if (errorMessage) {
        showNoDataMessage(errorMessage);
        return;
    }

    // 呼叫 travel-time API,payload 已包含起訖點與查詢條件
    console.log(payload);
    const t0 = performance.now();
    fetchTravelTime(payload)
        .then((result) => {
            // 處理 API 回傳

            console.log(`[*] 查詢耗時 ${((performance.now() - t0) / 1000).toFixed(2)} 秒`);
            const segments = (result && result.segments) || [];

            if (segments.length === 0) {
                showNoDataMessage("查無符合條件的旅行時間資料");
                return;
            }

            // 繪製分段路線
            travelLayer = buildSegmentLayerGroup(segments);
            travelLayer.addTo(travelMap);

            updateTotalTimePanel(result.total_travel_time_min);
        })
        .catch((error) => {
            console.error(error);
            showNoDataMessage("旅行時間查詢失敗,請稍後再試");
        });
}

document.addEventListener("DOMContentLoaded", () => {
    travelMap = initMap("travelMap", [23.9, 121.0], 8);

    const now = new Date();
    document.getElementById("dateInput").value = now.toISOString().slice(0, 10);
    document.getElementById("timeInput").value = now.toTimeString().slice(0, 5);

    fetchGantryList().then((list) => {
        gantryListCache = list;
        populateGantrySelects(list);
    });

    // 部分瀏覽器重新整理時會自動還原下拉選單先前的值(且不觸發 change 事件),
    // 因此明確重置為預設模式,確保欄位顯示與選單文字一致。
    document.getElementById("modeSelect").value = "date";
    updateModeVisibility();

    document.getElementById("modeSelect").addEventListener("change", updateModeVisibility);
    document.getElementById("searchBtn").addEventListener("click", performSearch);
});
