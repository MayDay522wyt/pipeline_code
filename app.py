import streamlit as st
import os
import sys
import shutil
import utils
import traceback
from datetime import datetime
import io
import zipfile

# ===============================
# ✅ 0️⃣ 获取 base_dir（兼容 .py & PyInstaller）
# ===============================
if getattr(sys, "frozen", False):
    base_dir = os.path.dirname(sys.executable)
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

# ===============================
# ✅ 小工具：打包目录为 zip bytes
# ===============================
def zip_dir_to_bytes(dir_path: str) -> bytes:
    """
    把整个目录打包成 zip，并返回 bytes（用于 st.download_button）
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(dir_path):
            for fn in files:
                abs_path = os.path.join(root, fn)
                # zip 内部相对路径（以目录名开头，方便用户解压后结构清晰）
                rel_path = os.path.relpath(abs_path, start=os.path.dirname(dir_path))
                zf.write(abs_path, arcname=rel_path)
    buf.seek(0)
    return buf.read()

# ===============================
# ✅ 1️⃣ Streamlit 页面设置
# ===============================
st.set_page_config(page_title="自动化数据处理工具", page_icon="📂", layout="centered")

st.title("📂 自动化数据处理工具")
st.caption(f"程序工作目录（base_dir）：{base_dir}")
st.markdown("---")

# ===============================
# ✅ 2️⃣ 侧边栏参数输入
# ===============================
st.sidebar.header("📝 参数设置")

year = st.sidebar.text_input("请输入年份（如 2025）", value="2025").strip()
quarter = st.sidebar.selectbox("请选择季度", ["Q1", "Q2", "Q3", "Q4"])
operator = st.sidebar.text_input("处理人姓名（如 Kate）", value="Kate").strip()

st.sidebar.markdown("---")
clear_quarter_folder = st.sidebar.checkbox(
    "上传前清空该季度原始数据目录（推荐）",
    value=True,
)
clear_intermediate_folder = st.sidebar.checkbox(
    "运行前清空中间结果目录（推荐）",
    value=True,
)
use_timestamp_output = st.sidebar.checkbox(
    "输出文件名增加时间戳（推荐）",
    value=True,
)

# ===============================
# ✅ 3️⃣ 基本校验
# ===============================
if not year.isdigit():
    st.error("❌ 年份必须是数字，如 2025")
    st.stop()

if quarter not in ["Q1", "Q2", "Q3", "Q4"]:
    st.error("❌ 季度必须是 Q1 / Q2 / Q3 / Q4")
    st.stop()

template_json_path = os.path.join(base_dir, "template_columns.json")
if not os.path.exists(template_json_path):
    st.error("❌ 错误：未在程序目录找到 'template_columns.json'。请把它放到程序同目录下。")
    st.stop()

# ===============================
# ✅ 4️⃣ 上传区
# ===============================
st.info(f"当前任务：{year}年 {quarter} - 处理人：{operator}")

uploaded_files = st.file_uploader(
    "📤 请上传本季度所有相关数据文件（支持多选）",
    accept_multiple_files=True
)

# ===============================
# ✅ 5️⃣ session_state 初始化（关键：防 rerun 丢结果）
# ===============================
if "done" not in st.session_state:
    st.session_state.done = False
if "final_excel_bytes" not in st.session_state:
    st.session_state.final_excel_bytes = None
if "final_excel_name" not in st.session_state:
    st.session_state.final_excel_name = None
if "intermediate_zip_bytes" not in st.session_state:
    st.session_state.intermediate_zip_bytes = None
if "intermediate_zip_name" not in st.session_state:
    st.session_state.intermediate_zip_name = None
if "final_output_path" not in st.session_state:
    st.session_state.final_output_path = None
if "intermediate_dir" not in st.session_state:
    st.session_state.intermediate_dir = None

# ===============================
# ✅ 6️⃣ 执行按钮
# ===============================
run_clicked = st.button("🚀 开始自动化处理", type="primary")

if run_clicked:
    if not uploaded_files:
        st.warning("⚠️ 请先上传文件！")
        st.stop()

    quarter_folder = os.path.join(base_dir, f"{year}_{quarter}")
    intermediate_dir = os.path.join(base_dir, f"{quarter}_intermediate")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_filename = f"{year}_{quarter}_{operator}_{ts}_自存.xlsx" if use_timestamp_output else f"{year}_{quarter}_{operator}_自存.xlsx"
    final_output_path = os.path.join(base_dir, final_filename)

    intermediate_zip_name = f"{year}_{quarter}_{operator}_{ts}_intermediate.zip" if use_timestamp_output else f"{year}_{quarter}_{operator}_intermediate.zip"

    with st.expander("📌 路径信息（点击展开）", expanded=True):
        st.write("📁 季度数据目录：", quarter_folder)
        st.write("📁 中间结果目录：", intermediate_dir)
        st.write("📄 最终输出文件：", final_output_path)
        st.write("📄 JSON 模板路径：", template_json_path)

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # A. 准备目录
        status_text.text("正在准备目录...")
        if clear_quarter_folder and os.path.isdir(quarter_folder):
            shutil.rmtree(quarter_folder)
        os.makedirs(quarter_folder, exist_ok=True)

        if clear_intermediate_folder and os.path.isdir(intermediate_dir):
            shutil.rmtree(intermediate_dir)
        os.makedirs(intermediate_dir, exist_ok=True)

        progress_bar.progress(5)

        # B. 保存上传文件
        status_text.text("正在保存上传文件到季度目录...")
        n = len(uploaded_files)
        for i, uf in enumerate(uploaded_files, start=1):
            file_path = os.path.join(quarter_folder, uf.name)
            with open(file_path, "wb") as f:
                f.write(uf.getbuffer())
            progress_bar.progress(5 + int(30 * i / max(1, n)))

        # C. 跑流水线
        status_text.text("正在运行监管流水线（生成中间文件）...")
        progress_bar.progress(40)
        results, stats_dict = utils.run_all_pipelines_and_save_intermediate(
            quarter_folder=quarter_folder,
            year=int(year),
            quarter=quarter,
            save_dir=intermediate_dir
        )
        progress_bar.progress(75)

        # D. 生成最终表
        status_text.text("正在生成最终汇总表（自存模板）...")
        utils.align_and_export_to_self_template_by_json(
            template_json_path=template_json_path,
            output_excel_path=final_output_path,
            df_nmpa=results.get("NMPA"),
            df_fda=results.get("FDA"),
            df_ind=results.get("IND"),
            df_nda=results.get("NDA"),
            stats_dict=stats_dict
        )
        progress_bar.progress(90)
        status_text.text("正在打包中间结果目录...")

        # E. 读取最终文件 bytes
        with open(final_output_path, "rb") as f:
            final_excel_bytes = f.read()

        # F. 打包中间目录为 zip bytes
        intermediate_zip_bytes = zip_dir_to_bytes(intermediate_dir)

        progress_bar.progress(100)
        status_text.text("处理完成！")

        # ✅ 关键：写入 session_state（防止点击下载后 rerun 丢结果）
        st.session_state.done = True
        st.session_state.final_excel_bytes = final_excel_bytes
        st.session_state.final_excel_name = final_filename
        st.session_state.intermediate_zip_bytes = intermediate_zip_bytes
        st.session_state.intermediate_zip_name = intermediate_zip_name
        st.session_state.final_output_path = final_output_path
        st.session_state.intermediate_dir = intermediate_dir

        st.success(f"✅ 处理成功！共处理 {len(uploaded_files)} 个文件。")

    except Exception as e:
        st.session_state.done = False
        st.error(f"❌ 发生错误：{e}")
        st.code(traceback.format_exc())

# ===============================
# ✅ 7️⃣ 结果区：无论 rerun 都稳定显示两个下载按钮
# ===============================
if st.session_state.done:
    st.markdown("---")
    st.subheader("📦 下载结果")

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            label=f"📥 下载最终 Excel：{st.session_state.final_excel_name}",
            data=st.session_state.final_excel_bytes,
            file_name=st.session_state.final_excel_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_final_excel"
        )

    with col2:
        st.download_button(
            label=f"📥 下载中间结果（ZIP）：{st.session_state.intermediate_zip_name}",
            data=st.session_state.intermediate_zip_bytes,
            file_name=st.session_state.intermediate_zip_name,
            mime="application/zip",
            key="download_intermediate_zip"
        )

    st.caption("✅ 本地也已保存：")
    st.code(st.session_state.final_output_path)
    st.caption("✅ 中间结果目录：")
    st.code(st.session_state.intermediate_dir)