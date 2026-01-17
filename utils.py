import pandas as pd
from openpyxl import load_workbook

import os
import sys
import json
from IPython.display import display

def get_exe_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))
    
def get_base_dir():
    """
    ✅ 获取程序真实运行目录：
    - PyInstaller 打包后 → dist/single_quater(.exe/.app)
    - 源码运行 → utils.py 所在目录
    """
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def run_all_pipelines_and_save_intermediate(
    quarter_folder: str,     # 例如 "Q4"
    year: int,
    quarter: str,            # "Q1" / "Q2" / "Q3" / "Q4"
    save_dir: str
):
    """
    ✅ 最终统一输出规范版：
    - 自动匹配 IND / NDA / FDA / NMPA 四个文件
    - 分别跑四套流水线
    - ✅ 所有 output_file 和中间量，统一保存到：
        Q4_intermediate / Q3_intermediate 这种文件夹中
    - ✅ 防止任何文件互相覆盖
    """

    import os

    # ===== ✅ 0️⃣ 统一中间目录命名 =====
    intermediate_dir = os.path.join(save_dir,f"{quarter}_intermediate")

    os.makedirs(intermediate_dir, exist_ok=True)

    print("\n==============================")
    print("🚀 开始自动运行四大监管流水线")
    # ✅ 关键：强制输出到 dist 目录
    # base_dir = os.path.dirname(os.path.abspath(__file__))   # dist 目录
    # intermediate_dir = os.path.join(base_dir, f"{quarter}_intermediate")
    # os.makedirs(intermediate_dir, exist_ok=True)

    print(f"📁 统一输出目录：{intermediate_dir}")
    print("==============================\n")

    # ===== ✅ 1️⃣ 自动匹配文件 =====
    file_paths = match_regulatory_files(quarter_folder)

    ind_file  = file_paths.get("IND")
    nda_file  = file_paths.get("NDA")
    fda_file  = file_paths.get("FDA")
    nmpa_file = file_paths.get("NMPA")

    results = {}
    stats_dict = {}

    # =============================
    # ✅ 2️⃣ IND
    # =============================
    if ind_file:
        ind_out = os.path.join(intermediate_dir, f"{quarter}_IND_结果.xlsx")

        res_ind = run_ind_nda_pipeline(
            input_file=ind_file,
            output_file=ind_out,
            source="IND"
        )

        # save_intermediate_df(df_ind, intermediate_dir, f"{quarter}_IND")
        df_ind = res_ind["df"]
        
        stats_dict["China IND"] = {
            "【粗分类统计】": res_ind["stat_coarse"],
            "【疾病领域统计】": res_ind["stat_disease"],
            "【靶点统计】": res_ind["stat_target"]
        }
        results["IND"] = df_ind
    else:
        print("⚠️ 未找到 IND 文件，已跳过")

    # =============================
    # ✅ 3️⃣ NDA
    # =============================
    if nda_file:
        nda_out = os.path.join(intermediate_dir, f"{quarter}_NDA_结果.xlsx")

        res_nda = run_ind_nda_pipeline(
            input_file=nda_file,
            output_file=nda_out,
            source="NDA"
        )

        # save_intermediate_df(df_nda, intermediate_dir, f"{quarter}_NDA")
        df_nda = res_nda["df"]

        stats_dict["China NDA"] = {
            "【粗分类统计】": res_nda["stat_coarse"],
            "【疾病领域统计】": res_nda["stat_disease"],
            "【靶点统计】": res_nda["stat_target"]
        }
        results["NDA"] = df_nda
    else:
        print("⚠️ 未找到 NDA 文件，已跳过")

    # =============================
    # ✅ 4️⃣ FDA
    # =============================
    if fda_file:
        fda_out = os.path.join(intermediate_dir, f"{quarter}_FDA_结果.xlsx")

        res_fda = run_fda_pipeline(
            input_file=fda_file,
            output_file=fda_out
        )

        # save_intermediate_df(df_fda, intermediate_dir, f"{quarter}_FDA")
        df_fda = res_fda["df"]

        stats_dict["FDA approved drugs"] = {
            "【粗分类统计】": res_fda["stat_coarse"],
            "【靶点统计】": res_fda["stat_target"]
        }

        results["FDA"] = df_fda
    else:
        print("⚠️ 未找到 FDA 文件，已跳过")

    # =============================
    # ✅ 5️⃣ NMPA
    # =============================
    if nmpa_file:
        nmpa_out = os.path.join(intermediate_dir, f"{quarter}_NMPA_结果.xlsx")

        res_nmpa = run_nmpa_quarter_pipeline(
            input_file=nmpa_file,
            output_file=nmpa_out,
            year=year,
            quarter=quarter
        )

        # save_intermediate_df(df_nmpa, intermediate_dir, f"{quarter}_NMPA")
        df_nmpa = res_nmpa["df"]

        stats_dict["NMPA approved drugs"] = {
            "【粗分类统计】": res_nmpa["stat_coarse"],
            "【疾病领域统计】": res_nmpa["stat_disease"],
            "【靶点统计】": res_nmpa["stat_target"]
        }
        results["NMPA"] = df_nmpa
    else:
        print("⚠️ 未找到 NMPA 文件，已跳过")

    print("\n==============================")
    print("✅ 四大监管流水线全部执行完成")
    print(f"📁 所有结果 & 中间量统一保存在：{intermediate_dir}")
    print("==============================\n")

    return results,stats_dict

def get_exe_base_dir():
    """
    ✅ 获取程序真实运行目录：
    - PyInstaller 打包后 → dist/
    - 源码运行 → utils.py 所在目录
    """
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))


def match_regulatory_files(quarter_folder: str):
    """
    ✅ 永远从【程序所在目录】下面找 Q4 / Q1 / Q2 目录
    """
    base_dir = get_exe_base_dir()
    quarter_dir = os.path.join(base_dir, quarter_folder)

    print(f"📁 实际搜索目录：{quarter_dir}")

    if not os.path.exists(quarter_dir):
        raise FileNotFoundError(f"❌ 找不到季度文件夹：{quarter_dir}")

    files = os.listdir(quarter_dir)

    result = {"IND": None, "NDA": None, "FDA": None, "NMPA": None}

    for f in files:
        f_upper = f.upper()
        full_path = os.path.join(quarter_dir, f)

        if "IND" in f_upper:
            result["IND"] = full_path
        elif "NDA" in f_upper:
            result["NDA"] = full_path
        elif "FDA" in f_upper:
            result["FDA"] = full_path
        elif "NMPA" in f_upper:
            result["NMPA"] = full_path

    print("✅ 匹配到的监管文件：")
    for k, v in result.items():
        print(f"   {k}: {v}")

    return result

def step1_dedup_only_keep_latest_NDA_IND(
    input_path: str,
    sheet_name: str = "数据详情",
    date_col: str = "CDE承办日期",
):
    df = pd.read_excel(input_path, sheet_name=sheet_name)

    print("✅ 原始数据行数：", len(df))
    # display(df.head())

    # ===== 去重键，先检查是否存在 =====
    dedup_cols = ["通用名", "剂型", "持证商"]
    missing = [c for c in dedup_cols if c not in df.columns]

    if missing:
        print(f"⚠️ 未找到去重关键列：{missing}，将跳过去重，直接返回原始表。")
        print("🔍 当前表头为：", list(df.columns))
        display(df.head())
        # 这里直接返回原表，不做任何修改（连序号都不加）
        return df

    # ===== ✅ 情况 1：存在日期列 → 按日期排序后保留最新 =====
    if date_col in df.columns:
        print(f"✅ 使用日期列【{date_col}】进行排序去重（保留最新）")
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(by=date_col)
        df = df.drop_duplicates(subset=dedup_cols, keep="last").copy()

    # ===== ✅ 情况 2：不存在日期列 → 直接按原顺序保留最后一行 =====
    else:
        print(f"⚠️ 未发现日期列【{date_col}】，改为直接保留最后一条记录")
        df = df.drop_duplicates(subset=dedup_cols, keep="last").copy()

    # ===== ✅ 删除【受理号】=====
    if "受理号" in df.columns:
        df = df.drop(columns=["受理号"])

    # ===== ✅ 添加【序号】=====
    # df.insert(0, "序号", range(1, len(df) + 1))
    if "序号" not in df.columns:
        df.insert(0, "序号", range(1, len(df) + 1))
    else:
        print("ℹ️ 已存在【序号】列，跳过自动生成")

    print("✅ 去重后行数（最终保留规则生效）：", len(df))
    # display(df.head())

    return df

# def step1_nmpa_filter_by_quarter(
#     input_path: str,
#     sheet_name: str = "数据详情",
#     approval_date_col: str = "最新批准日期",
#     drug_name_col: str = "通用名",
#     year: int = None,          # ✅ 例如 2024
#     quarter: str = "Q4"       # ✅ "Q1" / "Q2" / "Q3" / "Q4"
# ):
#     """
#     NMPA 专用（按自然季度筛选）：
#     1）读取【数据详情】sheet
#     2）按【指定年份 + 季度（Q1~Q4）】筛选批准药品
#     3）相同【通用名】→ 使用相同【序号】（不同规格共用）
#     """

#     if year is None:
#         raise ValueError("❌ 必须显式指定 year，例如 year=2024")

#     quarter = quarter.upper()
#     if quarter not in ["Q1", "Q2", "Q3", "Q4"]:
#         raise ValueError("❌ quarter 只能是：'Q1', 'Q2', 'Q3', 'Q4'")

#     # ===== ✅ 1️⃣ 读取数据 =====
#     df = pd.read_excel(input_path, sheet_name=sheet_name)
#     print("✅ NMPA 原始数据行数：", len(df))

#     # ===== ✅ 2️⃣ 检查关键列 =====
#     for col in [approval_date_col, drug_name_col]:
#         if col not in df.columns:
#             raise ValueError(
#                 f"❌ 找不到列：{col}，请检查 NMPA 表头。当前列：{list(df.columns)}"
#             )

#     # ===== ✅ 3️⃣ 时间转换 =====
#     df[approval_date_col] = pd.to_datetime(df[approval_date_col], errors="coerce")

#     # ===== ✅ 4️⃣ 定义季度起止日期 =====
#     if quarter == "Q1":
#         start_date = pd.Timestamp(year=year, month=1, day=1)
#         end_date   = pd.Timestamp(year=year, month=3, day=31)
#     elif quarter == "Q2":
#         start_date = pd.Timestamp(year=year, month=4, day=1)
#         end_date   = pd.Timestamp(year=year, month=6, day=30)
#     elif quarter == "Q3":
#         start_date = pd.Timestamp(year=year, month=7, day=1)
#         end_date   = pd.Timestamp(year=year, month=9, day=30)
#     else:  # Q4
#         start_date = pd.Timestamp(year=year, month=10, day=1)
#         end_date   = pd.Timestamp(year=year, month=12, day=31)

#     # ===== ✅ 5️⃣ 按季度筛选 =====
#     df_q = df[
#         (df[approval_date_col] >= start_date) &
#         (df[approval_date_col] <= end_date)
#     ].copy()

#     print(f"✅ 筛选区间：{start_date.date()} ~ {end_date.date()}")
#     print(f"✅ 该季度批准行数：", len(df_q))

#     # ===== ✅ 6️⃣ 相同通用名 → 同一序号 =====
#     unique_drugs = (
#         df_q[drug_name_col]
#         .dropna()
#         .drop_duplicates()
#         .reset_index(drop=True)
#     )

#     drug_to_id = {
#         name: idx + 1
#         for idx, name in unique_drugs.items()
#     }

#     df_q["序号"] = df_q[drug_name_col].map(drug_to_id)

#     print("✅ NMPA 按季度处理完成，添加序号后行数：", len(df_q))
#     # display(df_q.head())

#     return df_q

def step1_nmpa_filter_by_quarter(
    input_path: str,
    sheet_name: str = "数据详情",
    approval_date_col: str = "最新批准日期",
    drug_name_col: str = "通用名",
    dosage_col: str = "剂型",          # ⭐ 新增字段：用于去重
    year: int = None,
    quarter: str = "Q4"
):
    """
    NMPA 专用（按自然季度筛选）：
    1）筛选季度批准药品
    2）按【通用名 + "持证商(NMPA)"】分组 → 共用同一序号
    """

    if year is None:
        raise ValueError("❌ 必须显式指定 year，例如 year=2024")

    quarter = quarter.upper()
    if quarter not in ["Q1", "Q2", "Q3", "Q4"]:
        raise ValueError("❌ quarter 只能是：'Q1', 'Q2', 'Q3', 'Q4'")

    # ===== 1️⃣ 读取 =====
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    print("✅ NMPA 原始数据行数：", len(df))

    # ===== 2️⃣ 检查字段 =====
    for col in [approval_date_col, drug_name_col, dosage_col]:
        if col not in df.columns:
            raise ValueError(
                f"❌ 找不到列：{col}（当前列：{list(df.columns)}）"
            )

    # ===== 3️⃣ 时间格式处理 =====
    df[approval_date_col] = pd.to_datetime(df[approval_date_col], errors="coerce")

    # ===== 4️⃣ 计算季度起止 =====
    if quarter == "Q1":
        start_date = pd.Timestamp(year=year, month=1, day=1)
        end_date = pd.Timestamp(year=year, month=3, day=31)
    elif quarter == "Q2":
        start_date = pd.Timestamp(year=year, month=4, day=1)
        end_date = pd.Timestamp(year=year, month=6, day=30)
    elif quarter == "Q3":
        start_date = pd.Timestamp(year=year, month=7, day=1)
        end_date = pd.Timestamp(year=year, month=9, day=30)
    else:  # Q4
        start_date = pd.Timestamp(year=year, month=10, day=1)
        end_date = pd.Timestamp(year=year, month=12, day=31)

    # ===== 5️⃣ 按季度筛选 =====
    df_q = df[
        (df[approval_date_col] >= start_date) &
        (df[approval_date_col] <= end_date)
    ].copy()

    print(f"📌 筛选区间：{start_date.date()} ~ {end_date.date()}")
    print(f"📌 季度内批准记录数：{len(df_q)}")

    # ===== 6️⃣ ⭐ 按【通用名 + 剂型】去重生成序号 =====
    # unique_pairs = (
    #     df_q[[drug_name_col, dosage_col,"持证商(NMPA)"]]
    #     .dropna()
    #     .drop_duplicates()
    #     .reset_index(drop=True)
    # )

    # # 生成序号
    # drug_to_id = {
    #     (row[drug_name_col], row[dosage_col]): idx + 1
    #     for idx, row in unique_pairs.iterrows()
    # }

    # df_q["序号"] = df_q.apply(
    # lambda r: drug_to_id.get((r[drug_name_col], r[dosage_col])),
    # axis=1
    # )

    # print(f"✅ NMPA 按【通用名 + 剂型】添加序号后行数：{len(df_q)}")
    # ===== 6️⃣ ⭐ 按【通用名 + 剂型 + 持证商(NMPA)】去重，保留第一条 =====
    dedup_cols = [drug_name_col, dosage_col, "持证商(NMPA)"]

    before = len(df_q)

    df_q = (
        df_q.sort_values(approval_date_col)  # 如需要保持时间顺序
            .drop_duplicates(subset=dedup_cols, keep="first")
            .reset_index(drop=True)
    )

    after = len(df_q)
    print(f"✅ NMPA 去重完成：删除 {before - after} 条重复记录（基于 {dedup_cols}）")

    # ===== ✅ 添加【序号】列 =====
    if "序号" not in df_q.columns:
        df_q.insert(0, "序号", range(1, len(df_q) + 1))
        print(f"✅ 已为 NMPA 结果添加【序号】列，行数：{len(df_q)}")
    else:
        print("ℹ️ 检测到已有【序号】列，保留原有序号")

    return df_q
# def step1_fda_dedup_and_add_id(
#     input_path: str,
#     sheet_name: str = "目标药品",
# ):
#     """
#     FDA 专用（当前规则）：
#     1）读取【目标药品】sheet
#     2）不做任何去重
#     3）按原始顺序从上到下直接添加【序号】
#     """

#     df = pd.read_excel(input_path, sheet_name=sheet_name)

#     print("✅ FDA 原始数据行数：", len(df))
#     # display(df.head())

#     # ===== ✅ 直接按行号添加【序号】=====
#     if "序号" not in df.columns:
#         df.insert(0, "序号", range(1, len(df) + 1))
#     else:
#         print("ℹ️ FDA 表中已存在【序号】列，跳过自动生成")

#     print("✅ FDA 添加序号后行数：", len(df))
#     # display(df.head())

#     return df

def step1_fda_dedup_and_add_id(
    input_path: str,
    sheet_name: str = "目标药品",
    dedup_cols=["活性成分(中文)", "申请机构","剂型"]
):
    """
    FDA 专用（最新规则）：
    1）读取【目标药品】sheet
    2）按【活性成分（中文） + 持证商(NMPA)】去重（保留最后一条）
    3）按最终顺序添加【序号】
    """

    df = pd.read_excel(input_path, sheet_name=sheet_name)

    print("✅ FDA 原始数据行数：", len(df))

    # ===== 1️⃣ 检查必要字段是否存在 =====
    missing_cols = [c for c in dedup_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"❌ FDA 表缺少以下去重字段：{missing_cols}\n"
            f"当前表头：{list(df.columns)}"
        )

    # ===== 2️⃣ 去重（保留最后一条记录）=====
    df_dedup = df.drop_duplicates(subset=dedup_cols, keep="last").copy()

    print(f"🔁 FDA 按 {dedup_cols} 去重后行数：{len(df_dedup)}")

    # ===== 3️⃣ 添加序号 =====
    df_dedup.insert(0, "序号", range(1, len(df_dedup) + 1))

    print("✅ FDA 添加序号后行数：", len(df_dedup))

    return df_dedup

# def build_classify_mapping():
#     mapping_data = [
#         ["生物制品", "抗体", "BIO", "Antibody"],
#         ["化学药品", "其他", "SMD", "SMD"],
#         ["生物制品", "其他", "BIO", "BIO"],
#         ["生物制品", "疫苗", "BIO", "Vaccine"],
#         ["生物制品", "细胞疗法", "CGT", "CGT"],
#         ["中药", "中成药", "TCM", "TCM"],
#         ["生物制品", "基因疗法", "CGT", "CGT"],
#         ["化学药品", "多肽", "Polypeptide", "Polypeptide"],
#         ["化学药品", "核酸", "SMD", "RNA"],
#         ["生物制品", "多肽", "Polypeptide", "Polypeptide"],
#         ["中药", "中药单体", "TCM", "TCM"],
#         ["生物制品", "核酸", "BIO", "RNA"],
#     ]

#     df_map = pd.DataFrame(
#         mapping_data,
#         columns=["药品类别一", "药品类别二", "类别(粗分)", "详细列（细分）"]
#     )

#     return df_map
def build_classify_mapping_from_json():
    """
    ✅ 自动从程序同级目录读取 rules_config.json
    """
    config_path = os.path.join(get_base_dir(), "rules_config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ 找不到规则配置文件：{config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    mapping_data = config["classification_mapping"]
    df_map = pd.DataFrame(mapping_data)

    return df_map

def step2_add_class_and_save(
    df,
    df_map,
    output_classified_path: str
):
    df_with_class = df.merge(
        df_map,
        on=["药品类别一", "药品类别二"],
        how="left"
    )
    col = df_with_class["类别(粗分)"].astype(str).str.strip()
    missing = df_with_class[
    df_with_class["类别(粗分)"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
    ]

    
    
    # ✅ 未匹配检查
    # col = df_with_class["类别(粗分)"].astype(str).str.strip()

    # missing = df_with_class[
    #     col.isna() | col.eq("") | col.eq("nan") | col.eq("NaN") | col.eq("None")
    # ]
    # missing = df_with_class[
    #     df_with_class["类别(粗分)"]
    #     .astype(str)
    #     .str.strip()
    #     .isin(["", "nan", "NaN", "None"])
    # ]
    # missing = df_with_class[df_with_class["类别(粗分)"].isna()]
    if len(missing) > 0:
        print("⚠️ 发现未匹配分类的记录：")
        print(missing.shape)
        display(missing)
        # display(
        #     missing.drop_duplicates(subset=["药品类别一", "药品类别二"])
        # )
        # display(missing[["药品类别一", "药品类别二"]].drop_duplicates())
    else:
        print("✅ 所有记录已成功匹配分类")
    df_with_class.loc[col.isin(["", "nan", "NaN", "None"]), "类别(粗分)"] = "Others"
    # display(df_with_class.head())
    # ========== 给细分类补 Others ==========
    fine_col = df_with_class["详细列（细分）"].astype(str).str.strip()

    df_with_class.loc[
        fine_col.isin(["", "nan", "NaN", "None"]),
        "详细列（细分）"
    ] = "Others"
    # ======================================
    # ✅ 只保存这一份
    df_with_class.to_excel(output_classified_path, index=False)
    print(f"✅ 分类明细表已保存：{output_classified_path}")

    return df_with_class

# def step3_print_statistics(df):

#     # ===== ✅ 内部小工具：给统计表添加 Total 行 =====
#     def add_total_row(stat_df, name_col="类别", count_col="数量"):
#         total_value = stat_df[count_col].sum()
#         total_row = pd.DataFrame({
#             name_col: ["Total"],
#             count_col: [total_value]
#         })
#         stat_df_with_total = pd.concat([stat_df, total_row], ignore_index=True)
#         return stat_df_with_total

#     # ===============================
#     # ✅ 一、按【药品类别一】统计
#     # ===============================
#     print("✅ 一、按【药品类别一】统计：")
#     stat_cat1 = df["药品类别一"].value_counts().reset_index()
#     stat_cat1.columns = ["药品类别一", "数量"]

#     stat_cat1 = add_total_row(
#         stat_cat1,
#         name_col="药品类别一",
#         count_col="数量"
#     )

#     display(stat_cat1)

#     # ===============================
#     # ✅ 二、按【粗分类】统计
#     # ===============================
#     print("✅ 二、按【粗分类】统计：")
#     stat_coarse = df["类别(粗分)"].value_counts().reset_index()
#     stat_coarse.columns = ["类别(粗分)", "数量"]

#     stat_coarse = add_total_row(
#         stat_coarse,
#         name_col="类别(粗分)",
#         count_col="数量"
#     )

#     display(stat_coarse)

#     # ===============================
#     # ✅ 三、按【细分类】统计
#     # ===============================
#     print("✅ 三、按【细分类】统计：")
#     stat_fine = df["详细列（细分）"].value_counts().reset_index()
#     stat_fine.columns = ["详细列（细分）", "数量"]

#     stat_fine = add_total_row(
#         stat_fine,
#         name_col="详细列（细分）",
#         count_col="数量"
#     )

#     display(stat_fine)

#     return stat_cat1, stat_coarse, stat_fine

def step3_print_statistics(df, show: bool = True):

    def add_total_row(stat_df, name_col="类别", count_col="数量"):
        total_value = stat_df[count_col].sum()
        total_row = pd.DataFrame({
            name_col: ["Total"],
            count_col: [total_value]
        })
        return pd.concat([stat_df, total_row], ignore_index=True)

    stat_cat1 = None
    stat_coarse = None
    stat_fine = None

    # ===============================
    # ✅ 一、按【药品类别一】统计
    # ===============================
    if "药品类别一" in df.columns:
        stat_cat1 = df["药品类别一"].value_counts().reset_index()
        stat_cat1.columns = ["药品类别一", "数量"]
        stat_cat1 = add_total_row(stat_cat1, "药品类别一", "数量")

        if show:
            print("✅ 一、按【药品类别一】统计：")
            display(stat_cat1)
    else:
        if show:
            print("⚠️ 跳过【药品类别一】统计：当前 DataFrame 中不存在该列")

    # ===============================
    # ✅ 二、按【粗分类】统计
    # ===============================
    if "类别(粗分)" in df.columns:
        stat_coarse = df["类别(粗分)"].value_counts().reset_index()
        stat_coarse.columns = ["类别(粗分)", "数量"]
        stat_coarse = add_total_row(stat_coarse, "类别(粗分)", "数量")

        if show:
            print("✅ 二、按【粗分类】统计：")
            display(stat_coarse)
    else:
        if show:
            print("⚠️ 跳过【粗分类】统计：当前 DataFrame 中不存在列【类别(粗分)】")

    # ===============================
    # ✅ 三、按【细分类】统计
    # ===============================
    if "详细列（细分）" in df.columns:
        stat_fine = df["详细列（细分）"].value_counts().reset_index()
        stat_fine.columns = ["详细列（细分）", "数量"]
        stat_fine = add_total_row(stat_fine, "详细列（细分）", "数量")

        if show:
            print("✅ 三、按【细分类】统计：")
            display(stat_fine)
    else:
        if show:
            print("⚠️ 跳过【细分类】统计：当前 DataFrame 中不存在列【详细列（细分）】")

    return stat_cat1, stat_coarse, stat_fine

# def build_disease_area_mapping():
#     return {
#         "Oncology": "肿瘤",
#         "Hematology": "血液",
#         "Infectious": "感染",
#         "Respiratory": "呼吸",
#         "Gastrointestinal": "消化",
#         "Dermatology": "皮肤",
#         "Rare disease": "罕见疾病",
#         "Immunology": "免疫",
#         "Other": "其他"
#     }

def load_disease_area_mapping_from_json():
    config_path = os.path.join(get_base_dir(), "rules_config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ 找不到规则配置文件：{config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return config["disease_area_mapping"]

def step4_statistics_by_disease_area(df, disease_col: str = "参考疾病领域", show: bool = True):

    mapping = load_disease_area_mapping_from_json()

    if disease_col not in df.columns:
        if show:
            print(f"⚠️ 跳过疾病领域统计：找不到列【{disease_col}】")
        return None

    stat_rows = []

    for eng, zh in mapping.items():
        count = df[disease_col].astype(str).str.contains(zh, na=False).sum()
        stat_rows.append([eng, zh, count])

    stat_df = pd.DataFrame(
        stat_rows,
        columns=["疾病领域(英文)", "疾病领域(中文)", "数量"]
    )

    total_value = stat_df["数量"].sum()
    total_row = pd.DataFrame([["Total", "Total", total_value]],
                             columns=stat_df.columns)
    stat_df = pd.concat([stat_df, total_row], ignore_index=True)

    if show:
        print("✅ 按【参考疾病领域】统计结果：")
        display(stat_df)

    return stat_df

# def step5_statistics_by_target(df, target_col: str = "靶点", show: bool = True):

#     if target_col not in df.columns:
#         if show:
#             print(f"⚠️ 跳过靶点统计：当前 DataFrame 中不存在列【{target_col}】")
#         return (
#             pd.DataFrame(columns=["靶点", "数量"]),
#             pd.DataFrame(columns=["靶点", "数量"])
#         )

#     s = (
#         df[target_col]
#         .astype(str)
#         .fillna("")
#         .str.strip()
#     )

#     s = s[s != ""]

#     if s.empty:
#         if show:
#             print("⚠️ 靶点列为空或仅包含空值，返回空统计表。")
#         return (
#             pd.DataFrame(columns=["靶点", "数量"]),
#             pd.DataFrame(columns=["靶点", "数量"])
#         )

#     vc = s.value_counts()
#     detail_df = vc.reset_index()
#     detail_df.columns = ["靶点", "数量"]

#     top_k = 10
#     top_df = detail_df.head(top_k).copy()
#     others_count = detail_df["数量"].iloc[top_k:].sum()

#     summary_rows = []

#     for _, row in top_df.iterrows():
#         summary_rows.append([row["靶点"], row["数量"]])

#     if others_count > 0:
#         summary_rows.append(["others", others_count])

#     summary_df = pd.DataFrame(summary_rows, columns=["靶点", "数量"])

#     total_row = pd.DataFrame(
#         [["Total", summary_df["数量"].sum()]],
#         columns=["靶点", "数量"]
#     )
#     summary_df = pd.concat([summary_df, total_row], ignore_index=True)

#     if show:
#         print("✅ 按【靶点】统计结果（Top10 + others）：")
#         display(summary_df)

#     return detail_df, summary_df
def step5_statistics_by_target(df, target_col: str = "靶点", show: bool = True):

    if target_col not in df.columns:
        if show:
            print(f"⚠️ 跳过：不存在列【{target_col}】")
        empty = pd.DataFrame(columns=["靶点", "数量"])
        return empty, empty

    # 三列用于判断空值
    cols_check = [target_col, "药品类别一", "药品类别二"]

    for col in cols_check:
        if col not in df.columns:
            raise KeyError(f"❌ DataFrame 缺少必要列：{col}")

    # 统一清洗
    cleaned = df[cols_check].astype(str).apply(lambda c: c.str.strip())
    empty_vals = ["", "nan", "NaN", "None"]

    # === 判断三列是否全部为空 ===
    mask_all_empty = cleaned.apply(lambda row: all(v in empty_vals for v in row), axis=1)

    # -------------------------------
    # ✅（新增）打印总行数、有效靶点行数
    # -------------------------------
    total_rows = len(df)
    rows_no_target = mask_all_empty.sum()
    rows_valid = total_rows - rows_no_target

    if show:
        print("📊 靶点统计基础信息：")
        print(f"  • 总行数：{total_rows}")
        print(f"  • 三列皆为空 → 无有效靶点 的行数：{rows_no_target}")
        print(f"  • 进入靶点统计的有效行数：{rows_valid}")
        print("-" * 50)

    # 过滤有效
    # === 修复：靶点为空但行有效的也归入 Others ===

    valid_df = df[~mask_all_empty].copy()
    valid_df[target_col] = valid_df[target_col].astype(str).str.strip()
    valid_df.loc[
        valid_df[target_col].isin(["", "nan", "NaN", "None"]),
        target_col
    ] = "others"

    if valid_df.empty:
        if show:
            print("⚠️ 没有任何有效靶点信息，返回空表")
        empty = pd.DataFrame(columns=["靶点", "数量"])
        return empty, empty

    # 使用靶点列
    s = valid_df[target_col].astype(str).str.strip()
    s = s[~s.isin(empty_vals)]

    # value_counts
    vc = s.value_counts()
    detail_df = vc.rename_axis("靶点").reset_index(name="数量")

    # Top10 + others
    top_k = 10
    if len(detail_df) > top_k:
        top_df = detail_df.head(top_k)
        others_count = detail_df["数量"].iloc[top_k:].sum()
        summary_df = pd.concat(
            [top_df, pd.DataFrame([["others", others_count]], columns=["靶点", "数量"])]
        )
    else:
        summary_df = detail_df.copy()

    # 添加 Total 行
    summary_df.loc[len(summary_df)] = ["Total", summary_df["数量"].sum()]

    if show:
        display(summary_df)

    return detail_df, summary_df

# def step5_statistics_by_target(df, target_col: str = "靶点", show: bool = True):

#     # 1. 列不存在
#     if target_col not in df.columns:
#         if show:
#             print(f"⚠️ 跳过：不存在列【{target_col}】")
#         empty = pd.DataFrame(columns=["靶点", "数量"])
#         return empty, empty

#     # 2. 先 dropna，再转 str（顺序很重要！）
#     s = (
#         df[target_col]
#         .dropna()             # 去掉 NaN（关键步骤）
#         .astype(str)          # 转成字符串
#         .str.strip()          # 去掉两端空白
#     )

#     # 3. 去掉空字符串、"nan"、"None" 等脏值
#     s = s[~s.isin(["", "nan", "None", "NaN"])]
#     if s.empty:
#         if show:
#             print("⚠️ 靶点列为空，返回空结果")
#         empty = pd.DataFrame(columns=["靶点", "数量"])
#         return empty, empty

#     # 4. value_counts 统计
#     vc = s.value_counts(dropna=True)
#     detail_df = vc.rename_axis("靶点").reset_index(name="数量")

#     # 5. 生成 top10 + others
#     top_k = 10
#     if len(detail_df) > top_k:
#         top_df = detail_df.head(top_k)
#         others_count = detail_df["数量"].iloc[top_k:].sum()
#         summary_df = pd.concat(
#             [top_df, pd.DataFrame([["others", others_count]], columns=["靶点", "数量"])]
#         )
#     else:
#         summary_df = detail_df.copy()

#     # 6. Total 行
#     total = summary_df["数量"].sum()
#     summary_df.loc[len(summary_df)] = ["Total", total]

#     if show:
#         display(summary_df)

#     return detail_df, summary_df

def save_all_stats_to_one_sheet(
    output_file,
    stat_cat1,
    stat_coarse,
    stat_fine,
    stat_disease_area,
    summary_target,
    detail_target,
    sheet_name="所有统计汇总"
):
    """
    把 Step 3-5 的所有统计结果，按区块写入同一个 Sheet。
    ✅ 自动跳过 None 或空 DataFrame
    ✅ 使用 overlay 模式，避免重复写入时报错
    """

    import pandas as pd

    with pd.ExcelWriter(
        output_file,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="overlay"   # ✅ 允许多次写同一 Sheet
    ) as writer:

        start_row = 0

        def write_block(title, df_block, start_row):
            """
            ✅ 安全写入单个区块：
            - df_block 为 None 或空表 → 自动跳过
            - 返回新的 start_row
            """

            if df_block is None:
                print(f"⚠️ 跳过区块（None）：{title}")
                return start_row

            if isinstance(df_block, pd.DataFrame) and df_block.empty:
                print(f"⚠️ 跳过区块（空表）：{title}")
                return start_row

            # ===== 标题（单独一行）=====
            title_df = pd.DataFrame([[title]])
            title_df.to_excel(
                writer,
                sheet_name=sheet_name,
                startrow=start_row,
                startcol=0,
                index=False,
                header=False
            )

            # ===== 数据表 =====
            df_block.to_excel(
                writer,
                sheet_name=sheet_name,
                startrow=start_row + 2,  # 标题下面空一行
                startcol=0,
                index=False
            )

            # ✅ 返回下一个 block 的起始行
            return start_row + len(df_block) + 5

        # ===== ✅ 依次写入各个统计块（全部是安全写入）=====
        start_row = write_block("【统计一：药品类别一】", stat_cat1, start_row)
        start_row = write_block("【统计二：粗分类】", stat_coarse, start_row)
        start_row = write_block("【统计三：细分类】", stat_fine, start_row)
        start_row = write_block("【统计四：疾病领域】", stat_disease_area, start_row)
        start_row = write_block("【统计五：靶点 Top10 + Others】", summary_target, start_row)
        start_row = write_block("【统计六：靶点全量明细】", detail_target, start_row)

    print(f"✅ 所有可用的 Step 3–5 统计结果已合并保存到同一个 Sheet：{sheet_name}")


############### 多季度合并 ############3

def load_and_merge_by_sheet(
    q_files: list,        # ["Q1.xlsx", "Q2.xlsx", "Q3.xlsx", "Q4.xlsx"]
    sheet_keyword: str    # "FDA" / "NMPA" / "IND" / "NDA"
):
    """
    ✅ 终极稳健版（带“行数监控”+ 自动剔除空行）：
    1）自动查找“包含关键词”的 Sheet
    2）定位【药品类别二 / 药品类别一】作为真实表头
    3）识别 Q1–Q4 → 写入【季度来源】
    4）按不同 Sheet 类型进行列裁剪
    5）自动剔除“全为空”的行
    6）纵向合并
    ✅ 全流程打印“每一步的行数”
    """

    dfs = []

    for f in q_files:
        # ===== ✅ 1️⃣ 自动查找包含关键词的 sheet =====
        xl = pd.ExcelFile(f, engine="openpyxl")
        matched_sheets = [
            s for s in xl.sheet_names
            if sheet_keyword.lower() in s.lower()
        ]

        if len(matched_sheets) == 0:
            print(f"⚠️ 文件 {f} 中未找到包含关键词【{sheet_keyword}】的 Sheet，已跳过")
            continue

        sheet_name = matched_sheets[0]
        print(f"\n✅ 文件 {f} 使用 Sheet: {sheet_name}")

        # ===== ✅ 2️⃣ 无表头读取，用于“定位真实表头行” =====
        df_raw = pd.read_excel(
            f,
            sheet_name=sheet_name,
            engine="openpyxl",
            header=None
        )

        # 在前 10 行内查找“药品类别二”或“药品类别一”作为表头定位锚点
        header_row_idx = None
        search_limit = min(10, len(df_raw))

        for i in range(search_limit):
            row_vals = df_raw.iloc[i].astype(str).tolist()
            if any(v.strip() in ["药品类别二", "药品类别一"] for v in row_vals):
                header_row_idx = i
                break

        if header_row_idx is not None:
            if header_row_idx > 0:
                print(f"    ✅ 在第 {header_row_idx+1} 行检测到真实表头，已自动删除前 {header_row_idx} 行")

            new_header = df_raw.iloc[header_row_idx].astype(str).values
            df = df_raw.iloc[header_row_idx + 1:].copy()
            df.columns = new_header
            df = df.reset_index(drop=True)
        else:
            print(f"    ⚠️ 未在前 10 行中检测到【药品类别一/二】，退回默认读取方式")
            df = pd.read_excel(f, sheet_name=sheet_name, engine="openpyxl")

        # ✅✅✅ 第一次剔除“全为空”的行（表头修复后）
        before_drop = df.shape[0]
        df = df.dropna(how="all").reset_index(drop=True)
        after_drop = df.shape[0]
        print(f"    🧹 表头修复后：剔除空行 {before_drop - after_drop} 行")

        # ✅ 打印：修复表头后的“有效行数”
        print(f"    📌 表头修复后有效行数：{df.shape[0]}")

        # ===== ✅ 3️⃣ 标记季度来源 Q1-Q4 =====
        fname = os.path.basename(f).upper()

        if "Q1" in fname:
            quarter = "Q1"
        elif "Q2" in fname:
            quarter = "Q2"
        elif "Q3" in fname:
            quarter = "Q3"
        elif "Q4" in fname:
            quarter = "Q4"
        else:
            quarter = "未知季度"
            print(f"⚠️ 无法从文件名识别季度：{f}")

        df["季度来源"] = quarter

        # ✅ 打印：加季度后的“有效行数”
        print(f"    📌 添加季度来源后行数：{df.shape[0]}")

        # ===== ✅ 4️⃣ 按 Sheet 类型裁剪列（最终口径） =====
        keyword_upper = sheet_keyword.upper()

        if keyword_upper == "FDA":
            base_keep_cols = ["通用名", "剂型", "集团", "药品类别一", "药品类别二", "靶点", "季度来源"]

        elif keyword_upper == "NMPA":
            base_keep_cols = ["通用名", "药品类别一", "靶点", "季度来源"]

        elif keyword_upper in ["IND", "NDA"]:
            base_keep_cols = ["通用名", "药品类别一", "药品类别二", "靶点", "参考疾病领域", "季度来源"]

        else:
            base_keep_cols = []

        if base_keep_cols:
            keep_cols = [c for c in base_keep_cols if c in df.columns]
            missing = set(base_keep_cols) - set(keep_cols)
            if missing:
                print(f"⚠️ {sheet_keyword} 中缺失部分期望列：{missing}")

            df = df[keep_cols].copy()

        # ✅✅✅ 第二次剔除“裁剪后可能形成的空行”
        before_drop2 = df.shape[0]
        df = df.dropna(how="all").reset_index(drop=True)
        after_drop2 = df.shape[0]
        print(f"    🧹 裁剪后：剔除空行 {before_drop2 - after_drop2} 行")

        # ✅ 打印：裁剪后的“最终有效行数”
        print(f"    ✅ 裁剪后最终有效行数：{df.shape[0]}")

        dfs.append(df)

    if len(dfs) == 0:
        raise ValueError(f"❌ 所有文件中均未成功读取到【{sheet_keyword}】相关 Sheet")

    df_all = pd.concat(dfs, ignore_index=True)

    print(f"\n✅ 已合并 Sheet 关键词 = {sheet_keyword}")
    print(f"✅ 合并后总有效行数：{df_all.shape[0]}")

    return df_all


#################### 单个季度数据处理封装 ####################

#  NMPA
def run_nmpa_quarter_pipeline(
    input_file: str,
    output_file: str,
    year: int,
    quarter: str,
    sheet_name: str = "数据详情",
    approval_date_col: str = "最新批准日期",
    drug_name_col: str = "通用名",
    disease_col: str = "参考疾病领域",
    target_col: str = "靶点",
    summary_sheet_name: str = "所有统计汇总"
):
    """
    ✅ NMPA 最近一季度“全自动统计流水线”：
    1️⃣ 最近一季度批准 + 同药同序号
    2️⃣ 分类映射
    3️⃣ 保存分类明细表
    4️⃣ 药品类别一 / 粗分 / 细分 统计
    5️⃣ 参考疾病领域统计
    6️⃣ 靶点 Top10 + Others 统计
    7️⃣ 所有统计结果写入同一 Sheet

    ✅ 你只需要传：input_file, output_file, year, quarter
    """

    print("\n===============================")
    print(f"🚀 开始执行 NMPA {year} {quarter} 统计流水线")
    print("===============================\n")

    # ===== 1️⃣ 最近一季度批准 + 同药同序号 =====
    df_dedup = step1_nmpa_filter_by_quarter(
        input_path=input_file,
        sheet_name=sheet_name,
        approval_date_col=approval_date_col,
        drug_name_col=drug_name_col,
        year=year,
        quarter=quarter
    )

    # ===== 2️⃣ 构建分类规则 =====
    df_map = build_classify_mapping_from_json()

    # ===== 3️⃣ 加分类 & ✅ 保存分类明细表 =====
    df_with_class = step2_add_class_and_save(
        df=df_dedup,
        df_map=df_map,
        output_classified_path=output_file
    )

    # ===== 4️⃣ ✅ 分类统计（药品类别一 / 粗分 / 细分）=====
    stat_cat1, stat_coarse, stat_fine = step3_print_statistics(df_with_class)

    # ===== 5️⃣ ✅ 疾病领域统计 =====
    stat_disease_area = step4_statistics_by_disease_area(
        df_with_class,
        disease_col=disease_col
    )

    # ===== 6️⃣ ✅ 靶点 Top10 + Others =====
    detail_target, summary_target = step5_statistics_by_target(
        df_with_class,
        target_col=target_col
    )

    # ===== 7️⃣ ✅ 所有统计结果合并写入同一个 Sheet =====
    save_all_stats_to_one_sheet(
        output_file=output_file,
        stat_cat1=stat_cat1,
        stat_coarse=stat_coarse,
        stat_fine=stat_fine,
        stat_disease_area=stat_disease_area,
        summary_target=summary_target,
        detail_target=detail_target,
        sheet_name=summary_sheet_name
    )

    print("\n===============================")
    print("✅ NMPA 最近一季度统计流水线执行完成！")
    print(f"📁 结果文件：{output_file}")
    print("===============================\n")

    return {
    "df": df_with_class,
    "stat_cat1": stat_cat1,
    "stat_coarse":stat_coarse,
    "stat_fine":stat_fine,
    "stat_disease": stat_disease_area,
    "stat_target": summary_target
}


######## FDA
def run_fda_pipeline(
    input_file: str,
    output_file: str,
    sheet_name: str = "目标药品",
    target_col: str = "靶点",
    summary_sheet_name: str = "所有统计汇总"
):
    """
    ✅ FDA 全自动统计流水线：
    1️⃣ 目标药品去重 + 加序号
    2️⃣ 分类映射
    3️⃣ 保存分类明细表
    4️⃣ 药品类别一 / 粗分 / 细分 统计
    5️⃣ 靶点 Top10 + Others 统计
    6️⃣ 所有统计结果写入同一 Sheet

    ✅ FDA 不做疾病领域统计（自动传 None）
    """

    print("\n===============================")
    print("🚀 开始执行 FDA 统计流水线")
    print("===============================\n")

    # ===== 1️⃣ FDA：目标药品去重 + 加序号 =====
    df_dedup = step1_fda_dedup_and_add_id(
        input_path=input_file,
        sheet_name=sheet_name
    )

    # ===== 2️⃣ 构建分类规则 =====
    df_map = build_classify_mapping_from_json()

    # ===== 3️⃣ 加分类 & ✅ 保存分类明细表 =====
    df_with_class = step2_add_class_and_save(
        df=df_dedup,
        df_map=df_map,
        output_classified_path=output_file
    )

    # ===== 4️⃣ ✅ 分类统计（药品类别一 / 粗分 / 细分）=====
    stat_cat1, stat_coarse, stat_fine = step3_print_statistics(df_with_class)

    # ===== 5️⃣ ✅ 靶点 Top10 + Others =====
    detail_target, summary_target = step5_statistics_by_target(
        df_with_class,
        target_col=target_col
    )

    # ===== 6️⃣ ✅ 所有统计结果合并写入同一个 Sheet =====
    save_all_stats_to_one_sheet(
        output_file=output_file,
        stat_cat1=stat_cat1,
        stat_coarse=stat_coarse,
        stat_fine=stat_fine,
        stat_disease_area=None,      # ✅ FDA 无疾病领域
        summary_target=summary_target,
        detail_target=detail_target,
        sheet_name=summary_sheet_name
    )

    print("\n===============================")
    print("✅ FDA 统计流水线执行完成！")
    print(f"📁 结果文件：{output_file}")
    print("===============================\n")

    return {
    "df": df_with_class,
    "stat_cat1": stat_cat1,
    "stat_coarse":stat_coarse,
    "stat_fine":stat_fine,
    "stat_disease": None,
    "stat_target": summary_target
}

def run_ind_nda_pipeline(
    input_file: str,
    output_file: str,
    source: str,   # "IND" 或 "NDA"
    disease_col: str = "参考疾病领域",
    target_col: str = "靶点",
    summary_sheet_name: str = "所有统计汇总"
):
    """
    ✅ IND / NDA 通用全自动统计流水线：
    1️⃣ 去重（通用名 + 剂型 + 持证商，保留最新）
    2️⃣ 分类映射
    3️⃣ 保存分类明细表
    4️⃣ 药品类别一 / 粗分 / 细分 统计
    5️⃣ 参考疾病领域统计
    6️⃣ 靶点 Top10 + Others
    7️⃣ 所有统计结果写入同一 Sheet

    ✅ source 只能是："IND" 或 "NDA"
    """

    source = source.upper()
    if source not in ["IND", "NDA"]:
        raise ValueError("❌ source 只能是 'IND' 或 'NDA'")

    print("\n===============================")
    print(f"🚀 开始执行 {source} 统计流水线")
    print("===============================\n")

    if isinstance(input_file, str):
        # ===== 1️⃣ 去重（仅内存中） =====
        df_dedup = step1_dedup_only_keep_latest_NDA_IND(
            input_path=input_file
        )
    else:
        df_dedup=input_file

    # ===== 2️⃣ 构建分类规则 =====
    df_map = build_classify_mapping_from_json()

    # ===== 3️⃣ 加分类 & ✅ 保存分类明细表 =====
    df_with_class = step2_add_class_and_save(
        df=df_dedup,
        df_map=df_map,
        output_classified_path=output_file
    )

    # ===== 4️⃣ ✅ 分类统计（药品类别一 / 粗分 / 细分）=====
    stat_cat1, stat_coarse, stat_fine = step3_print_statistics(df_with_class)

    # ===== 5️⃣ ✅ 疾病领域统计 =====
    stat_disease_area = step4_statistics_by_disease_area(
        df_with_class,
        disease_col=disease_col
    )

    # ===== 6️⃣ ✅ 靶点 Top10 + Others =====
    detail_target, summary_target = step5_statistics_by_target(
        df_with_class,
        target_col=target_col
    )

    # ===== 7️⃣ ✅ 所有统计结果合并写入同一个 Sheet =====
    save_all_stats_to_one_sheet(
        output_file=output_file,
        stat_cat1=stat_cat1,
        stat_coarse=stat_coarse,
        stat_fine=stat_fine,
        stat_disease_area=stat_disease_area,
        summary_target=summary_target,
        detail_target=detail_target,
        sheet_name=summary_sheet_name
    )

    print("\n===============================")
    print(f"✅ {source} 统计流水线执行完成！")
    print(f"📁 结果文件：{output_file}")
    print("===============================\n")

    return {
    "df": df_with_class,
    "stat_cat1": stat_cat1,
    "stat_coarse":stat_coarse,
    "stat_fine":stat_fine,
    "stat_disease": stat_disease_area,
    "stat_target": summary_target
}



def align_and_export_to_self_template_by_json(
    template_json_path: str,         # ✅ 你保存的 template_columns.json
    output_excel_path: str,          # 新导出的结果 Excel
    df_nmpa: pd.DataFrame,
    df_fda: pd.DataFrame,
    df_ind: pd.DataFrame,
    df_nda: pd.DataFrame,
    stats_dict: dict                 # 每类对应的统计结果
):
    """
    ✅ 功能（JSON 驱动最终版）：
    1️⃣ 从 JSON 读取 4 个 Sheet 的【标准列结构】
    2️⃣ 自动对齐新数据列名
    3️⃣ 写回为 4 个子 Sheet
    4️⃣ 在每个 Sheet 下方追加对应统计表
    ✅ 特别规则：
       - 如果模板中有列【类型】，且中间 df 中有【类别(粗分)】列，
         则自动用【类别(粗分)】填充【类型】
    """

    # ===== ✅ 0️⃣ 读取 JSON 模板列配置 =====
    with open(template_json_path, "r", encoding="utf-8") as f:
        template_cols_map = json.load(f)

    sheet_map = {
        "NMPA approved drugs": df_nmpa,
        "FDA approved drugs": df_fda,
        "China IND": df_ind,
        "China NDA": df_nda,
    }

    # ✅ 确保输出目录存在
    save_dir = os.path.dirname(output_excel_path)
    if save_dir:
        os.makedirs(save_dir, exist_ok=True)

    with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
        for sheet_name, df_new in sheet_map.items():

            print(f"\n✅ 正在处理 Sheet：{sheet_name}")

            # ===== ✅ 1️⃣ 从 JSON 读取标准列结构 =====
            template_cols = template_cols_map.get(sheet_name)

            if not template_cols:
                print(f"⚠️ JSON 中未找到该 Sheet 的列模板：{sheet_name}，已跳过")
                continue

            print("    🔹 模板列名（来自 JSON）：", template_cols)

            # ===== ✅ 2️⃣ 按模板列构造对齐后的 DataFrame =====
            aligned_data = {}
            n_rows = len(df_new)

            for col in template_cols:
                if col in df_new.columns:
                    # 模板列名在新数据中也存在 → 直接用
                    aligned_data[col] = df_new[col].values

                elif col == "类型" and "类别(粗分)" in df_new.columns:
                    # ✅ 特殊规则：模板需要【类型】，用中间 df 的【类别(粗分)】来填
                    print("    🔁 列【类型】使用中间数据列【类别(粗分)】进行填充")
                    aligned_data[col] = df_new["类别(粗分)"].values

                else:
                    # 模板有，但新 df 没有，补空
                    aligned_data[col] = [pd.NA] * n_rows

            df_aligned = pd.DataFrame(aligned_data, columns=template_cols)

            print(f"    ✅ 列对齐完成，最终列数：{len(df_aligned.columns)}")
            print(f"    ✅ 数据行数：{len(df_aligned)}")

            # ===== ✅ 3️⃣ 写入主数据 =====
            df_aligned.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
                startrow=0
            )

            start_row = len(df_aligned) + 3  # 空两行再写统计

            # ===== ✅ 4️⃣ 追加统计表 =====
            stat_pack = stats_dict.get(sheet_name, {})

            for title, stat_df in stat_pack.items():
                if stat_df is None or stat_df.empty:
                    continue

                title_df = pd.DataFrame([[title]])
                title_df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    startrow=start_row,
                    index=False,
                    header=False
                )

                stat_df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    startrow=start_row + 2,
                    index=False
                )

                start_row += len(stat_df) + 4

            print(f"    ✅ {sheet_name} 写入完成")

    print("\n===============================")
    print("✅ 已完全按【JSON 模板结构】导出新版本")
    print(f"📁 输出文件：{output_excel_path}")
    print(f"📌 模板来源：{template_json_path}")
    print("===============================\n")