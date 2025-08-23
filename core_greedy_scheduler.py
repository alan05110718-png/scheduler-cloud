import pandas as pd
from datetime import datetime, timedelta
import random
import sys
import ast
from collections import defaultdict
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.cm import get_cmap
from matplotlib.font_manager import FontProperties

# ============== 全域基本參數 ==============
SETUP_TIME = 15
START_SCHEDULE_STR = "2025-07-20 00:00"   # 時間 0 原點
LOCK_WINDOW = 2880        # 48 小時 (分鐘) 只對 A 類機台換製程鎖定
INTER_STEP_GAP = 0
INFINITE_CAPACITY = 10**12  # 整理箱用
DEBUG = False
# =========================================

# ============== 檔案路徑（按需要修改） ==============
wipmq23_path = '新排程0710.xlsx'
reference_path = 'find_time_T.xlsx'
time_T_path = 'time_T_update.xlsx'
machine_capacity_path = r"C:/Users/User/PycharmProjects/永勝/0423/Machine ID Capacity.xlsx"
rule_path = r"C:/Users/User/PycharmProjects/永勝/rule_0111更新曆慧版.xlsx"
# ==============================================
# ====== 廠區可用機台設定 ======
SITE_MACHINE_IDS = {
    '2A': [
        'P_1706','P_1708','P_1709','P_1710','P_1711',
        'P_6217','P_6218','P_6219','P_6220','P_6221',
        'SK12_2A01','SK12_2A02'
    ],
    '2B': [
        'P_1712','P_1713','P_1714','P_1720','P_1721','P_1722',
        'P_6222','P_6223','P_6224','P_6225','P_6226',
        'SK12_2A03','SK12_2A04','SK12_2A05'
    ]
}

_dup = set(SITE_MACHINE_IDS['2A']) & set(SITE_MACHINE_IDS['2B'])
if _dup:
    print("⚠️ 警告：2A 與 2B 機台集合有重複：", _dup)

# ===================================================================
# 一、資料讀取與整併
# ===================================================================
def read_and_merge_data(
        keep_all_centers: bool = True,
        export_unmatched: bool = True,
        debug: bool = True
) -> str:
    cols = ["工單編號", "料號", "批號", "工單", "排隊數"]
    try:
        df_wip = pd.read_excel(wipmq23_path)[cols]
    except FileNotFoundError:
        sys.exit(f"❌ 找不到 WIP 檔案：{wipmq23_path}")
    if debug:
        print("[0] 原始 WIP 列數：", len(df_wip))

    df_wip = df_wip.rename(columns={'料號': '料品編號'})
    df_wip.insert(df_wip.columns.get_loc("料品編號") + 1, "產品編號",
                  df_wip["料品編號"].astype(str).str[:5])
    df_wip["批號"] = df_wip["批號"].astype(str)
    df_wip.insert(df_wip.columns.get_loc("批號") + 1,
                  "工單別", df_wip["批號"].str[:1].fillna(''))

    if debug:
        print("[2] 產品編號缺失：", df_wip["產品編號"].isna().sum(),
              "工單別空字串：", df_wip["工單別"].eq('').sum())

    try:
        xls = pd.ExcelFile(reference_path)
        df_ref = pd.read_excel(xls, sheet_name="料號水合對應表")
        df_product = pd.read_excel(xls, sheet_name="產品水合對應表")
    except FileNotFoundError:
        sys.exit(f"❌ 找不到參考檔：{reference_path}")

    df_ref["產品編號"] = df_ref["濕片群組"].astype(str).str.zfill(5).str[:5]
    df_ref["工單別"] = df_ref["工單別"].astype(str).fillna('')
    product_mapping = df_product.set_index("產品碼分類")["產品"].to_dict()

    unique_ref = (
        df_ref[["產品編號", "工單別", "產品分類", "水合代號"]]
        .drop_duplicates()
        .assign(產品=lambda d: d["產品分類"].map(product_mapping))
    )

    final_data = df_wip.merge(unique_ref,
                              on=["產品編號", "工單別"],
                              how="left",
                              indicator=True)
    if debug:
        print("[3] merge 結果：\n", final_data["_merge"].value_counts())

    if export_unmatched:
        unmatched = final_data[final_data["_merge"] == "left_only"]
        if not unmatched.empty:
            unmatched_file = "unmatched_wip.xlsx"
            unmatched.to_excel(unmatched_file, index=False)
            print(f"⚠️ 有 {len(unmatched)} 筆 WIP 無對應參考表，已輸出 {unmatched_file}")

    final_data.drop(columns="_merge", inplace=True)

    try:
        time_T = pd.read_excel(time_T_path, sheet_name="工作表1").ffill()
    except FileNotFoundError:
        sys.exit(f"❌ 找不到工時檔：{time_T_path}")

    time_columns = [c for c in time_T.columns if c not in ["產品分類", "水合代號"]]
    time_map = (
        time_T.set_index(["產品分類", "水合代號"])[time_columns]
        .to_dict(orient="index")
    )

    def fetch_times(row):
        return time_map.get(
            (row["產品分類"], row["水合代號"]),
            {c: None for c in time_columns}
        )

    times_df = final_data.apply(fetch_times, axis=1, result_type="expand")
    final_data[time_columns] = times_df

    if export_unmatched:
        missing_time = final_data[times_df.isna().all(axis=1)]
        if not missing_time.empty:
            missing_file = "missing_time_rows.xlsx"
            missing_time.to_excel(missing_file, index=False)
            print(f"⚠️ 有 {len(missing_time)} 筆缺少工時資料，已輸出 {missing_file}")

    output_file = "scheduling_result 20250718.xlsx"
    final_data.to_excel(output_file, index=False)
    print(f"✅ 整合資料已輸出：{output_file}")
    return output_file

# ========== 2. 準備批次資料（轉成製程步驟展開）==========
def prepare_batch_scheduling_data(scheduling_file):
    machine_df = pd.read_excel(machine_capacity_path)
    rule_df = pd.read_excel(rule_path, sheet_name='水合')
    scheduling_df = pd.read_excel(scheduling_file)

    # 給隨機交期 (可換成真實)
    base_date = datetime.strptime(START_SCHEDULE_STR, "%Y-%m-%d %H:%M")
    scheduling_df['交期'] = [
        (base_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        for _ in range(len(scheduling_df))
    ]

    # 假設第 12~30 欄是製程時間欄位（依你原始）
    process_cols = scheduling_df.columns[12:30]
    # 移除全空製程列
    scheduling_df = scheduling_df[
        scheduling_df[process_cols].apply(
            lambda row: row.notna().any() and (row.astype(str).str.strip() != '').any(),
            axis=1
        )
    ].reset_index(drop=True)

    # 生成製程組合與組合編號
    # === 建立製程組合（tuple of (欄名, float值)） ===
    def extract_process_tuple(row):
        combo = []
        for c in process_cols:
            v = row[c]
            try:
                if pd.notna(v) and str(v).strip() != '' and float(v) > 0:
                    combo.append((c, float(v)))
            except:
                continue
        return tuple(combo)  # 可能為空 tuple()

    scheduling_df['製程組合'] = scheduling_df.apply(extract_process_tuple, axis=1)

    # === 去掉完全空的組合（如果希望保留可刪掉這段）===
    empty_cnt = (scheduling_df['製程組合'].map(len) == 0).sum()
    if empty_cnt > 0:
        print(f"[prepare] 移除空製程組合列 {empty_cnt} 筆")
        scheduling_df = scheduling_df[scheduling_df['製程組合'].map(len) > 0].reset_index(drop=True)

    # === 轉成標準字串（避免 tuple 長度不同的問題 / 也方便比對）===
    def combo_to_str(tup):
        # tup 形式: ((ProcCol, duration), ...)
        return '|'.join(f"{p}:{d:g}" for p, d in tup)

    scheduling_df['製程組合_str'] = scheduling_df['製程組合'].apply(combo_to_str)

    # === 用 factorize 產生組合編號（從 1 起算）===
    codes, uniques = pd.factorize(scheduling_df['製程組合_str'], sort=False)
    scheduling_df['組合編號'] = codes + 1

    print(f"[prepare] 不同製程組合數量：{len(uniques)}")
    if len(uniques) <= 10:
        print("[prepare] 前幾個組合示例：")
        for i, u in enumerate(uniques[:10], 1):
            print(f"  組合 {i}: {u}")
    scheduling_df.to_excel(scheduling_file, index=False)
    return scheduling_df, machine_df, rule_df

# =============== 核心：建 job 表 ===============

def build_jobs_from_scheduling(scheduling_df):
    process_cols = scheduling_df.columns[12:29]  # 確認 slicing 正確
    records = []
    for _, row in scheduling_df.iterrows():
        wo = row['工單編號']
        item = row['料品編號']
        qty = row['排隊數']
        due = row['交期']
        combo = row['組合編號']
        seq = 1
        for pcol in process_cols:
            val = row.get(pcol)
            if pd.notna(val) and str(val).strip() != '':
                try:
                    ft = float(val)
                    if ft > 0:
                        records.append({
                            '工單編號': wo,
                            '料品編號': item,
                            '排隊數': qty,
                            '交期': due,
                            '組合編號': combo,
                            '製程順序': seq,
                            '製程名稱': pcol,
                            '製程時間(分鐘)': ft
                        })
                        seq += 1
                except:
                    continue
    jobs_df = pd.DataFrame(records)
    if jobs_df.empty:
        raise ValueError("⚠️ 無任何可排製程列。")
    start_origin = datetime.strptime(START_SCHEDULE_STR, "%Y-%m-%d %H:%M")
    jobs_df['交期_dt'] = pd.to_datetime(jobs_df['交期'])
    jobs_df['交期_相對分'] = (jobs_df['交期_dt'] - start_origin).dt.total_seconds() / 60.0
    jobs_df['交期_相對分'] = jobs_df['交期_相對分'].fillna(jobs_df['交期_相對分'].max() or 0)
    return jobs_df


# =============== 機台資料處理 ===============

def build_machine_tables(machine_df, rule_df):
    # 依 index 切 A/B/C (與你原程式一致，如有異動需調整)
    df_type1 = machine_df.iloc[0:16].copy(); df_type1['類別'] = 'A'
    df_type2 = machine_df.iloc[17:27].copy(); df_type2['類別'] = 'B'
    df_type3 = machine_df.iloc[28:34].copy(); df_type3['類別'] = 'C'
    all_machines_df = pd.concat([df_type1, df_type2, df_type3], ignore_index=True)

    machine_liquid_restrictions = {}
    for _, row in rule_df.iterrows():
        m_id = row["Machine_ID"]
        allow = str(row.get("說明", "")).split(",") if pd.notna(row.get("說明")) else []
        machine_liquid_restrictions[m_id] = [a.strip() for a in allow if a.strip()]

    # 手動覆寫
    mapping_override = {
        'P_1706': ['泊洛沙姆','預泡純水','標準鹽水'],
        'P_1708': ['碳酸鈉'],
        'P_1709': ['碳酸鈉'],
        'P_1710': ['BP鹽水'],
        'P_1711': ['標準鹽水','純水'],
        'P_1712': ['碳酸鈉'],
        'P_1713': ['碳酸鈉'],
        'P_1714': ['碳酸鈉'],
        'P_1720': ['標準鹽水'],
        'P_1721': ['泊洛沙姆'],
        'P_1722': ['預泡純水','純水'],
    }
    machine_liquid_restrictions.update(mapping_override)

    def ensure_list(x):
        if isinstance(x, list): return x
        if isinstance(x, str):
            try:
                obj = ast.literal_eval(x)
                if isinstance(obj, list):
                    return [str(z).strip() for z in obj]
            except:
                return [s.strip() for s in x.split(",") if s.strip()]
        return []

    all_machines_df['可用製程名稱'] = (
        all_machines_df['Machine_ID']
        .map(machine_liquid_restrictions)
        .apply(ensure_list)
    )

    # 細類別
    category_capacity_groups = defaultdict(list)
    for _, row in all_machines_df.iterrows():
        key = (row['類別'], row['容量'])
        category_capacity_groups[key].append(row['Machine_ID'])

    sub_map = {}
    for cat in ['A', 'B', 'C']:
        sub_id = 1
        for (main_cat, cap), mlst in sorted(category_capacity_groups.items()):
            if main_cat == cat:
                sub_name = f"{cat}{sub_id}"
                for mid in mlst:
                    sub_map[mid] = sub_name
                sub_id += 1
    all_machines_df['機台細類別'] = all_machines_df['Machine_ID'].map(sub_map)
    return all_machines_df


# =============== 分廠區 (2A / 2B) ===============

def split_sites(jobs_df):
    def detect_prefix(wo):
        s = str(wo)
        if s.startswith("2A"): return "2A"
        if s.startswith("2B"): return "2B"
        return "2A"  # 預設
    jobs_df = jobs_df.copy()
    jobs_df['廠區'] = jobs_df['工單編號'].apply(detect_prefix)
    jobs_2A = jobs_df[jobs_df['廠區'] == '2A'].reset_index(drop=True)
    jobs_2B = jobs_df[jobs_df['廠區'] == '2B'].reset_index(drop=True)
    return jobs_2A, jobs_2B


# =============== 機台 State ===============

class MachineState:
    def __init__(self, machine_id, main_cat, sub_cat, capacity, allowed_processes, is_dummy=False):
        self.machine_id = machine_id
        self.main_cat = main_cat
        self.sub_cat = sub_cat
        self.capacity = capacity
        self.allowed = set([p.strip() for p in allowed_processes]) if allowed_processes else set()
        self.is_dummy = is_dummy
        self.available_time = 0
        self.last_process = None
        self.last_change_time = -10**12
        self.batches = []


# =============== 核心排程（單廠區） ===============

def schedule_one_site(site_jobs_df, machines_df, site_label):
    if site_jobs_df.empty:
        return pd.DataFrame(), 0

    process_to_categories = defaultdict(set)
    for _, m in machines_df.iterrows():
        for proc in m['可用製程名稱']:
            process_to_categories[proc.strip()].add(m['類別'])

    def assign_main_category(proc):
        poss = process_to_categories.get(proc, set())
        for c in ['A', 'B', 'C']:
            if c in poss: return c
        return None

    jobs = site_jobs_df.copy()
    jobs['機台主類別'] = jobs['製程名稱'].apply(assign_main_category)

    all_supported = set()
    for _, m in machines_df.iterrows():
        for proc in m['可用製程名稱']:
            all_supported.add(proc.strip())

    # 若要改成：若無支援直接報錯 (不進整理箱)：
    # unsupported = ~jobs['製程名稱'].isin(all_supported)
    # if unsupported.any():
    #     raise ValueError(f"{site_label} 有製程無機台支援：{jobs.loc[unsupported,'製程名稱'].unique()}")
    # jobs['是否整理箱'] = False

    jobs['是否整理箱'] = ~jobs['製程名稱'].isin(all_supported)

    jobs = jobs.sort_values(['工單編號', '料品編號', '製程順序']).reset_index(drop=True)
    jobs['Job_ID'] = jobs.index
    predecessor = {}
    chain_map = defaultdict(list)
    for _, r in jobs.iterrows():
        chain_map[(r['工單編號'], r['料品編號'])].append(r['Job_ID'])
    for chain_ids in chain_map.values():
        for i, jid in enumerate(chain_ids):
            predecessor[jid] = chain_ids[i-1] if i > 0 else None

    machine_states = []
    for _, m in machines_df.iterrows():
        machine_states.append(
            MachineState(
                machine_id=m['Machine_ID'],
                main_cat=m['類別'],
                sub_cat=m['機台細類別'],
                capacity=m['容量'],
                allowed_processes=m['可用製程名稱'],
                is_dummy=False
            )
        )

    dummy_machine = MachineState(
        machine_id="整理箱",
        main_cat="整理箱",
        sub_cat=None,
        capacity=INFINITE_CAPACITY,
        allowed_processes=None,
        is_dummy=True
    )

    unscheduled = set(jobs['Job_ID'])
    scheduled_info = {}

    def job_ready(jid):
        pred = predecessor[jid]
        if pred is None:
            return True
        return pred in scheduled_info

    def predecessor_finish_time(jid):
        pred = predecessor[jid]
        if pred is None:
            return 0
        return scheduled_info[pred]['end']

    current_time = 0
    batch_id_counter = 0

    while unscheduled:
        # 先排整理箱 ready
        ready_dummy_jobs = [
            jid for jid in list(unscheduled)
            if jobs.loc[jid, '是否整理箱'] and job_ready(jid)
        ]
        for jid in ready_dummy_jobs:
            row = jobs.loc[jid]
            start_t = predecessor_finish_time(jid)
            if start_t < current_time:
                start_t = current_time
            duration = row['製程時間(分鐘)'] + SETUP_TIME
            end_t = start_t + duration
            scheduled_info[jid] = {
                'start': start_t,
                'end': end_t,
                'machine': dummy_machine.machine_id,
                'machine_cat': dummy_machine.main_cat,
                'machine_sub': dummy_machine.sub_cat,
                'process': row['製程名稱'],
                'batch_id': f"D{batch_id_counter}",
                'batch_seq': 1
            }
            batch_id_counter += 1
            unscheduled.remove(jid)

        active_jobs = [
            jid for jid in unscheduled
            if not jobs.loc[jid, '是否整理箱']
        ]
        if not active_jobs:
            if not unscheduled:
                break
            future_pred_finishes = []
            for jid in unscheduled:
                pred = predecessor[jid]
                if pred is not None and pred in scheduled_info:
                    future_pred_finishes.append(scheduled_info[pred]['end'])
            if future_pred_finishes:
                current_time = max(current_time, min(future_pred_finishes))
            else:
                current_time += 1
            continue

        candidate_batches = []
        ready_jobs = [jid for jid in active_jobs if job_ready(jid)]
        if not ready_jobs:
            future_times = []
            for jid in active_jobs:
                pred = predecessor[jid]
                if pred is not None and pred in scheduled_info:
                    future_times.append(scheduled_info[pred]['end'])
            if future_times:
                current_time = max(current_time, min(future_times))
            else:
                current_time += 1
            continue

        for m in machine_states:
            earliest_machine_time = max(current_time, m.available_time)
            feasible_jobs = []
            for jid in ready_jobs:
                proc = jobs.loc[jid, '製程名稱']
                if proc in m.allowed:
                    feasible_jobs.append(jid)

            if not feasible_jobs:
                continue

            proc_groups = defaultdict(list)
            for jid in feasible_jobs:
                p = jobs.loc[jid, '製程名稱']
                proc_groups[p].append(jid)

            for proc_name, jids in proc_groups.items():
                jids.sort(
                    key=lambda x: (
                        jobs.loc[x, '交期_相對分'],
                        predecessor_finish_time(x),
                        jobs.loc[x, '排隊數']
                    )
                )
                batch_list = []
                used_qty = 0
                for jid in jids:
                    qty = jobs.loc[jid, '排隊數']
                    if used_qty + qty <= m.capacity:
                        batch_list.append(jid)
                        used_qty += qty
                    else:
                        continue
                    if used_qty == m.capacity:
                        break
                if not batch_list:
                    continue

                pred_ready_time = max(predecessor_finish_time(j) for j in batch_list)
                start_t = max(earliest_machine_time, pred_ready_time)
                if m.main_cat == 'A':
                    if m.last_process is not None and m.last_process != proc_name:
                        start_t = max(start_t, m.last_change_time + LOCK_WINDOW)

                durations = [
                    jobs.loc[j, '製程時間(分鐘)'] + SETUP_TIME
                    for j in batch_list
                ]
                batch_dur = max(durations)
                end_t = start_t + batch_dur
                candidate_batches.append({
                    'machine': m,
                    'jobs': batch_list,
                    'process': proc_name,
                    'start': start_t,
                    'end': end_t,
                    'avg_due': np.mean([jobs.loc[j, '交期_相對分'] for j in batch_list])
                })

        if not candidate_batches:
            future_preds = []
            for jid in active_jobs:
                pred = predecessor[jid]
                if pred is not None and pred in scheduled_info:
                    future_preds.append(scheduled_info[pred]['end'])
            if future_preds:
                current_time = max(current_time, min(future_preds))
            else:
                current_time += 1
            continue

        candidate_batches.sort(
            key=lambda b: (b['end'], b['start'], b['avg_due'])
        )
        chosen = candidate_batches[0]
        m = chosen['machine']
        start_t = chosen['start']
        end_t = chosen['end']
        proc_name = chosen['process']
        job_ids = chosen['jobs']

        m.available_time = end_t
        if m.main_cat == 'A' and (m.last_process is None or m.last_process != proc_name):
            m.last_change_time = start_t
        m.last_process = proc_name
        m.batches.append((batch_id_counter, start_t, end_t, proc_name, job_ids))

        for seq_in_batch, jid in enumerate(job_ids, start=1):
            row = jobs.loc[jid]
            j_dur = row['製程時間(分鐘)'] + SETUP_TIME
            indiv_end = start_t + j_dur
            scheduled_info[jid] = {
                'start': start_t,
                'end': indiv_end,
                'machine': m.machine_id,
                'machine_cat': m.main_cat,
                'machine_sub': m.sub_cat,
                'process': proc_name,
                'batch_id': f"B{batch_id_counter}",
                'batch_seq': seq_in_batch
            }
            unscheduled.remove(jid)
        batch_id_counter += 1

    sched_records = []
    for jid, info in scheduled_info.items():
        row = jobs.loc[jid]
        sched_records.append({
            '廠區': row['廠區'],
            '工單編號': row['工單編號'],
            '料品編號': row['料品編號'],
            '組合編號': row['組合編號'],
            '製程順序': row['製程順序'],
            '製程名稱': row['製程名稱'],
            '排隊數': row['排隊數'],
            '製程時間(分鐘)': row['製程時間(分鐘)'],
            '實際處理時間(含SETUP)': row['製程時間(分鐘)'] + SETUP_TIME,
            '交期': row['交期'],                # ← 新增
            '開始(分)': info['start'],
            '結束(分)': info['end'],
            '機台': info['machine'],
            '機台主類別': info['machine_cat'],
            '機台細類別': info['machine_sub'],
            '批次ID': info['batch_id'],
            '批次內序': info['batch_seq'],
            '是否整理箱': (info['machine'] == '整理箱'),
            'Job_ID': jid
        })

    schedule_df = pd.DataFrame(sched_records)
    if schedule_df.empty:
        return schedule_df, 0

    schedule_df = schedule_df.sort_values(
        ['工單編號', '料品編號', '製程順序']
    ).reset_index(drop=True)
    schedule_df['Gap_prev'] = 0
    grp = schedule_df.groupby(['工單編號', '料品編號'])
    for (wo, item), g in grp:
        prev_end = None
        for idx2 in g.index:
            if prev_end is None:
                schedule_df.at[idx2, 'Gap_prev'] = 0
            else:
                gap = schedule_df.at[idx2, '開始(分)'] - prev_end
                schedule_df.at[idx2, 'Gap_prev'] = gap
            prev_end = schedule_df.at[idx2, '結束(分)']

    # 新增你要的中文欄位
    schedule_df['間隔時間(分)'] = schedule_df['Gap_prev']

    makespan = schedule_df['結束(分)'].max()
    return schedule_df, makespan


# =============== 主排程 (含 2A / 2B 機台過濾 + 輸出多工作表) ===============

def schedule_all(scheduling_file):
    scheduling_df, machine_df, rule_df = prepare_batch_scheduling_data(scheduling_file)
    jobs_df = build_jobs_from_scheduling(scheduling_df)
    all_machines_df = build_machine_tables(machine_df, rule_df)

    # 廠區拆分
    jobs_2A, jobs_2B = split_sites(jobs_df)

    # 過濾廠區專用機台
    def filter_site_machines(site_label):
        wanted = set(SITE_MACHINE_IDS.get(site_label, []))
        site_df = all_machines_df[all_machines_df['Machine_ID'].isin(wanted)].copy()
        missing = wanted - set(site_df['Machine_ID'])
        if missing:
            print(f"⚠️ {site_label} 設定機台在 machine_df 找不到：{missing}")
        if site_df.empty:
            print(f"❌ {site_label} 無有效機台，該廠區全部工作將進整理箱！")
        return site_df

    machines_2A = filter_site_machines('2A')
    machines_2B = filter_site_machines('2B')

    overlap = set(machines_2A['Machine_ID']) & set(machines_2B['Machine_ID'])
    if overlap:
        print("⚠️ 警告：2A / 2B 實際機台集合有重疊：", overlap)

    sched_2A, mk_2A = schedule_one_site(jobs_2A, machines_2A, "2A")
    sched_2B, mk_2B = schedule_one_site(jobs_2B, machines_2B, "2B")

    start_origin = datetime.strptime(START_SCHEDULE_STR, "%Y-%m-%d %H:%M")
    for df in (sched_2A, sched_2B):
        if not df.empty:
            df['開始時間'] = df['開始(分)'].apply(lambda m: start_origin + timedelta(minutes=m))
            df['結束時間'] = df['結束(分)'].apply(lambda m: start_origin + timedelta(minutes=m))

    full_makespan = max(mk_2A, mk_2B)

    # 產出 Excel：2A / 2B 分 Sheet
    out_file = "批次排程結果_greedy.xlsx"
    with pd.ExcelWriter(out_file, engine='xlsxwriter') as writer:
        sched_2A.to_excel(writer, sheet_name='2A', index=False)
        sched_2B.to_excel(writer, sheet_name='2B', index=False)
        # 可選 Summary：
        summary_df = pd.DataFrame([
            {'廠區': '2A', 'Makespan(分)': mk_2A, '任務數': len(sched_2A)},
            {'廠區': '2B', 'Makespan(分)': mk_2B, '任務數': len(sched_2B)},
            {'廠區': 'Overall', 'Makespan(分)': full_makespan,
             '任務數': len(sched_2A) + len(sched_2B)}
        ])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    print(f"✅ 排程完成，輸出：{out_file}")
    print(f"👉 2A makespan: {mk_2A} 分, 2B makespan: {mk_2B} 分, Overall makespan: {full_makespan} 分")
    return sched_2A, sched_2B, full_makespan

def add_proc_labels(df):
    """
    為 DataFrame 加上:
    ─ Proc_Serial : 每個製程名稱的流水號，依開始時間遞增
                    (同一機台、同一開始時間共用同號)
    ─ Batch_Label : 製程名稱-流水號，用於甘特圖標籤
    """
    if df.empty:
        return df

    df = df.copy()
    # 先依 製程名稱 → 開始(分) → 機台 做穩定排序
    df.sort_values(['製程名稱', '開始(分)', '機台'], inplace=True)

    # key = (製程名稱, 機台, 開始時間)
    serial_dict = defaultdict(int)          # 每個製程目前編號
    key2serial = {}                         # (proc, m, start) ➜ serial
    proc_serial_col = []

    for _, row in df.iterrows():
        key = (row['製程名稱'], row['機台'], row['開始(分)'])
        proc = row['製程名稱']

        if key in key2serial:
            s = key2serial[key]             # 同時段同機台共用編號
        else:
            serial_dict[proc] += 1
            s = serial_dict[proc]
            key2serial[key] = s
        proc_serial_col.append(s)

    df['Proc_Serial'] = proc_serial_col
    df['Batch_Label'] = df['製程名稱'] + '-' + df['Proc_Serial'].astype(str)
    return df


# ===================================================================
# 二、甘特圖
# ===================================================================
def plot_gantt(
    cur_df,
    prefix,
    save_prefix="ALL",
    legend_max_show=30,
    label_threshold=5   # 橫條寬度 < threshold (分鐘) 就不顯示編號
):
    """
    依「製程名稱」決定顏色；同製程同色。
    每段橫條中央顯示該製程的流水號 (Proc_Serial)。
    若 w < label_threshold 則略過文字，以免擠在一起。
    """
    # ---------- 1. 基本設定 ----------
    plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei"]
    plt.rcParams["axes.unicode_minus"] = False
    kai = FontProperties(fname="C:/Windows/Fonts/msjh.ttc")

    df = cur_df.copy()
    if df.empty:
        print(f"⚠️ {prefix} 無資料，甘特圖略過。")
        return

    # y 軸標籤：主類別-機台
    df["y_label"] = df["機台主類別"].astype(str) + "-" + df["機台"].astype(str)
    order_df = (
        df[["機台主類別", "機台"]]
        .drop_duplicates()
        .sort_values(["機台主類別", "機台"])
    )
    ylabels = (order_df["機台主類別"] + "-" + order_df["機台"].astype(str)).tolist()
    y2idx = {lbl: i for i, lbl in enumerate(ylabels)}

    # ---------- 2. 顏色表 ----------
    proc_names = df["製程名稱"].unique()
    cmap = get_cmap("tab20", len(proc_names))
    proc2color = {p: cmap(i) for i, p in enumerate(proc_names)}

    # ---------- 3. 繪圖 ----------
    fig, ax = plt.subplots(figsize=(14, max(6, len(ylabels) * 0.5)))

    for lbl, idx in y2idx.items():
        rows = df[df["y_label"] == lbl].sort_values("開始(分)")
        prev_end = 0
        for _, r in rows.iterrows():
            s, e = r["開始(分)"], r["結束(分)"]
            w = e - s

            # 3-1 空閒段
            if s > prev_end:
                ax.broken_barh(
                    [(prev_end, s - prev_end)],
                    (idx - 0.3, 0.6),
                    facecolors="lightgrey",
                    alpha=0.7,
                    edgecolor="none",
                )

            # 3-2 工作段
            ax.broken_barh(
                [(s, w)],
                (idx - 0.3, 0.6),
                facecolors=proc2color[r["製程名稱"]],
                edgecolor="black",
                alpha=0.9,
            )

            # 3-3 標流水號
            if w >= label_threshold:
                mid_x = s + w / 2
                ax.text(
                    mid_x,
                    idx,
                    str(r["Proc_Serial"]),       # 只顯示編號；改成 r['Batch_Label'] 也可
                    ha="center",
                    va="center",
                    fontsize=6,
                    color="white",
                    weight="bold",
                )

            prev_end = e

    # ---------- 4. 軸與標題 ----------
    ax.set_yticks(range(len(ylabels)))
    ax.set_yticklabels(ylabels, fontproperties=kai, fontsize=12)
    ax.set_xlabel("時間 (分鐘)", fontproperties=kai, fontsize=14)
    ax.set_title(
        f"{prefix} 機台甘特圖（按製程著色）",
        fontproperties=kai,
        fontsize=18,
    )
    ax.grid(True, axis="x", linestyle="--", alpha=0.7)

    # ---------- 5. 圖例 ----------
    show_procs = proc_names[:legend_max_show]
    handles = [Patch(facecolor=proc2color[p], label=p) for p in show_procs]
    handles.insert(0, Patch(facecolor="lightgrey", label="空閒"))
    ax.legend(
        handles=handles,
        title="製程名稱",
        loc="center left",
        bbox_to_anchor=(1.01, 0.5),
        fontsize=8,
        title_fontsize=10,
        frameon=True,
    )

    plt.tight_layout(rect=[0, 0, 0.75, 1])

    out_png = f"{save_prefix}_{prefix}_ProcColor_Gantt.png"
    plt.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"🎨 已輸出甘特圖：{out_png}")



# =============== 主程式執行 ===============
if __name__ == "__main__":
    merged_file = read_and_merge_data()
    sched_2A, sched_2B, mk = schedule_all(merged_file)

    # 補上 Batch_Label 供甘特圖使用
    if not sched_2A.empty:
        sched_2A = add_proc_labels(sched_2A)
    if not sched_2B.empty:
        sched_2B = add_proc_labels(sched_2B)

    print("2A 頭幾列：")
    print(sched_2A.head())
    print("2B 頭幾列：")
    print(sched_2B.head())

    # 繪製甘特圖
    if not sched_2A.empty:
        plot_gantt(sched_2A, '2A', save_prefix="GANTT")
    if not sched_2B.empty:
        plot_gantt(sched_2B, '2B', save_prefix="GANTT")

    print("🎯 甘特圖已完成。")
