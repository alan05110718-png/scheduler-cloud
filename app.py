import os
import traceback
import streamlit as st

# ==== 匯入你的排程核心（內容不用改） ====
import core_greedy_scheduler as core

# ----------------- Streamlit 版面 -----------------
st.set_page_config(page_title="批次排程（2A/2B｜A/B/C｜Greedy）", layout="wide")
st.title("📅 批次排程系統（2A/2B｜A/B/C｜Greedy）")

# ----------- Sidebar：全域參數 -----------
st.sidebar.header("全域參數")
start_dt    = st.sidebar.text_input("時間 0 原點 START_SCHEDULE_STR", value=core.START_SCHEDULE_STR)
setup_time  = st.sidebar.number_input("SETUP_TIME（分鐘）", min_value=0, value=core.SETUP_TIME, step=5)
lock_window = st.sidebar.number_input("A 類換製程鎖定 LOCK_WINDOW（分鐘）", min_value=0,
                                      value=core.LOCK_WINDOW, step=60)

# ----------- Sidebar：廠區可用機台 -----------
st.sidebar.header("廠區可用機台設定")
def parse_list(txt: str):
    return [x.strip() for x in txt.split(",") if x.strip()]

site_2a_txt = st.sidebar.text_area(
    "2A 機台清單（逗號分隔）",
    value=",".join(core.SITE_MACHINE_IDS.get("2A", [])),
    height=120
)
site_2b_txt = st.sidebar.text_area(
    "2B 機台清單（逗號分隔）",
    value=",".join(core.SITE_MACHINE_IDS.get("2B", [])),
    height=120
)

# ---------------- 上傳區 ----------------
st.header("📤 上傳資料")
col1, col2 = st.columns(2)
with col1:
    wip_file  = st.file_uploader("WIP 工單檔：新排程0710.xlsx",            type=["xlsx"])
    ref_file  = st.file_uploader("參考對應檔：find_time_T.xlsx",          type=["xlsx"])
    time_file = st.file_uploader("工時檔：time_T_update.xlsx",            type=["xlsx"])
with col2:
    mc_file   = st.file_uploader("機台容量檔：Machine ID Capacity.xlsx",   type=["xlsx"])
    rule_file = st.file_uploader("製程規則（工作表：水合）：rule_0111更新曆慧版.xlsx", type=["xlsx"])

# ----------- 共用工具 -----------
TEMP_DIR = "temp_uploads"
os.makedirs(TEMP_DIR, exist_ok=True)

def save_uploaded(file, fixed_name):
    """把使用者上傳檔另存成核心程式預期的檔名，並回傳完整路徑"""
    path = os.path.join(TEMP_DIR, fixed_name)
    with open(path, "wb") as f:
        f.write(file.read())
    return path

def apply_runtime_overrides():
    """把 sidebar / 上傳設定套用到核心模組的全域變數上"""
    core.START_SCHEDULE_STR = start_dt
    core.SETUP_TIME         = int(setup_time)
    core.LOCK_WINDOW        = int(lock_window)
    core.SITE_MACHINE_IDS   = {
        "2A": parse_list(site_2a_txt),
        "2B": parse_list(site_2b_txt),
    }

def safe_plot(df, prefix):
    """
    呼叫內建 plot_gantt，
    成功時顯示圖片；若字體不存在則給 warning 但不中斷。
    """
    try:
        core.plot_gantt(df, prefix, save_prefix="GANTT")
        png_file = f"GANTT_{prefix}_ProcColor_Gantt.png"        # ← 檔名統一
        if os.path.exists(png_file):
            st.image(png_file, caption=f"{prefix} 甘特圖", use_column_width=True)
        else:
            st.warning(f"甘特圖檔案 {png_file} 不存在（可能繪圖失敗）。")
    except Exception as e:
        st.warning(f"甘特圖繪製時出現問題（多半是字體路徑 msjh.ttc 不存在）：{e}")

# ================= 主作業區 =================
run = st.button("🚀 執行排程")

if run:

    # ------- 1) 檢查上傳 -------
    missing = []
    if not wip_file:  missing.append("WIP 工單檔")
    if not ref_file:  missing.append("參考對應檔")
    if not time_file: missing.append("工時檔")
    if not mc_file:   missing.append("機台容量檔")
    if not rule_file: missing.append("製程規則檔")
    if missing:
        st.error("請上傳：" + "、".join(missing))
        st.stop()

    # ------- 2) 覆寫全域變數與存檔 -------
    try:
        apply_runtime_overrides()
        core.wipmq23_path        = save_uploaded(wip_file,  "新排程0710.xlsx")
        core.reference_path      = save_uploaded(ref_file,  "find_time_T.xlsx")
        core.time_T_path         = save_uploaded(time_file, "time_T_update.xlsx")
        core.machine_capacity_path = save_uploaded(mc_file, "Machine ID Capacity.xlsx")
        core.rule_path           = save_uploaded(rule_file, "rule_0111更新曆慧版.xlsx")
    except Exception as e:
        st.error(f"存檔或設定覆寫失敗：{e}")
        st.stop()

    # ------- 3) 執行核心流程 -------
    try:
        with st.spinner("整併資料中…"):
            merged_file = core.read_and_merge_data(debug=False, export_unmatched=True)

        with st.spinner("分廠區 Greedy 排程中…"):
            sched_2A, sched_2B, mk = core.schedule_all(merged_file)

        # ⭐ 產生 Proc_Serial / Batch_Label
        sched_2A = core.add_proc_labels(sched_2A)
        sched_2B = core.add_proc_labels(sched_2B)

        st.success(f"完成！Overall makespan：{mk:.0f} 分")

        tabs = st.tabs(["2A 結果", "2B 結果", "Summary / 下載"])

        # ----------------- 2A 分頁 -----------------
        with tabs[0]:
            if sched_2A.empty:
                st.info("2A 無可排任務。")
            else:
                st.subheader("2A 排程表")
                st.dataframe(sched_2A, use_container_width=True, hide_index=True)
                safe_plot(sched_2A, "2A")

        # ----------------- 2B 分頁 -----------------
        with tabs[1]:
            if sched_2B.empty:
                st.info("2B 無可排任務。")
            else:
                st.subheader("2B 排程表")
                st.dataframe(sched_2B, use_container_width=True, hide_index=True)
                safe_plot(sched_2B, "2B")

        # ----------------- Summary / 下載 -----------------
        with tabs[2]:
            st.subheader("輸出下載")
            # 排程結果 Excel
            excel_out = "批次排程結果_greedy.xlsx"
            if os.path.exists(excel_out):
                with open(excel_out, "rb") as f:
                    st.download_button(
                        "📥 下載排程結果（Excel）",
                        data=f.read(),
                        file_name=excel_out,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.warning("找不到輸出 Excel（核心未產生或檔名不同）。")

            # 甘特圖下載
            for p in ("2A", "2B"):
                png = f"GANTT_{p}_ProcColor_Gantt.png"          # ← 同步檔名
                if os.path.exists(png):
                    with open(png, "rb") as f:
                        st.download_button(f"🖼️ 下載 {p} 甘特圖",
                                           data=f.read(),
                                           file_name=png,
                                           mime="image/png")

    except Exception as e:
        st.error("執行發生錯誤，請檢查輸入格式或查看錯誤訊息。")
        with st.expander("顯示錯誤堆疊"):
            st.code(traceback.format_exc())
