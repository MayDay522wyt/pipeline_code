import utils
import os
import sys

# ===============================
# ✅ 1️⃣ 获取 base_dir（兼容 .py & .exe）
# ===============================
if getattr(sys, "frozen", False):
    # PyInstaller 打包后
    base_dir = os.path.dirname(sys.executable)
else:
    # 普通 python 运行
    base_dir = os.path.dirname(os.path.abspath(__file__))

print(f"\n📁 当前程序目录 base_dir = {base_dir}\n")

# ===============================
# ✅ 2️⃣ 交互输入参数
# ===============================
print("📌 请输入本次处理信息：")

year = input("👉 请输入年份（如 2025）：").strip()
quarter = input("👉 请输入季度（Q1 / Q2 / Q3 / Q4）：").strip().upper()
operator = input("👉 请输入处理人姓名（如 Yueting）：").strip()

# ✅ 基本合法性校验
if quarter not in ["Q1", "Q2", "Q3", "Q4"]:
    raise ValueError("❌ 季度必须是 Q1 / Q2 / Q3 / Q4")

if not year.isdigit():
    raise ValueError("❌ 年份必须是数字，如 2025")

print("\n==============================")
print(f"✅ 本次参数确认：")
print(f"   年份：{year}")
print(f"   季度：{quarter}")
print(f"   处理人：{operator}")
print("==============================\n")

# ===============================
# ✅ 3️⃣ 构造路径（全部锁死在 base_dir）
# ===============================
quarter_folder = os.path.join(base_dir, year+"_"+quarter)
intermediate_dir = os.path.join(base_dir, f"{quarter}_intermediate")

# ✅ 汇总文件名：2025_Q4_处理人_自存.xlsx
final_output_filename = f"{year}_{quarter}_{operator}_自存.xlsx"
final_output_path = os.path.join(base_dir, final_output_filename)

template_json_path = os.path.join(base_dir, "template_columns.json")

print(f"📁 本次季度数据目录：{quarter_folder}")
print(f"📁 中间结果目录：{intermediate_dir}")
print(f"📄 最终汇总文件：{final_output_path}")
print(f"📄 JSON 模板路径：{template_json_path}")
print()

# ===============================
# ✅ 4️⃣ 运行四大监管流水线
# ===============================
results, stats_dict = utils.run_all_pipelines_and_save_intermediate(
    quarter_folder=quarter_folder,
    year=int(year),
    quarter=quarter,
    save_dir=intermediate_dir   # ✅ 强制锁死在 dist/Q4_intermediate
)

# ===============================
# ✅ 5️⃣ 生成最终“自存标准模板”总表
# ===============================
utils.align_and_export_to_self_template_by_json(
    template_json_path=template_json_path,
    output_excel_path=final_output_path,
    df_nmpa=results.get("NMPA"),
    df_fda=results.get("FDA"),
    df_ind=results.get("IND"),
    df_nda=results.get("NDA"),
    stats_dict=stats_dict
)

# ===============================
# ✅ 6️⃣ 结束提示
# ===============================
print("\n==============================")
print("✅ ✅ 所有流程执行完成！")
print(f"📁 中间结果目录：{intermediate_dir}")
print(f"📄 最终输出文件：{final_output_path}")
print("==============================\n")

input("✅ 按回车键退出程序...")