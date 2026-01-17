import streamlit as st
import os
import shutil
import tempfile
import utils  # 引用你的工具库
import pandas as pd

# ===============================
# 页面基础设置
# ===============================
st.set_page_config(page_title="自动化数据处理工具", page_icon="📂")

st.title("📂 自动化数据处理工具")
st.markdown("---")

# ===============================
# 1️⃣ 侧边栏：参数设置
# ===============================
st.sidebar.header("📝 参数设置")

year = st.sidebar.text_input("请输入年份", value="2025")
quarter = st.sidebar.selectbox("请选择季度", ["Q1", "Q2", "Q3", "Q4"])
operator = st.sidebar.text_input("处理人姓名", value="Yueting")

# 检查模板文件是否存在
if not os.path.exists("template_columns.json"):
    st.error("❌ 错误：未在当前目录找到 'template_columns.json' 模板文件，请确保它已上传。")
    st.stop()

# ===============================
# 2️⃣ 主区域：文件上传
# ===============================
st.info(f"当前任务：{year}年 {quarter} - 处理人：{operator}")

uploaded_files = st.file_uploader(
    "📤 请上传本季度所有相关数据文件 (支持多选)", 
    accept_multiple_files=True
)

# ===============================
# 3️⃣ 执行逻辑
# ===============================
if st.button("🚀 开始自动化处理", type="primary"):
    if not uploaded_files:
        st.warning("⚠️ 请先上传文件！")
        st.stop()

    # 创建一个临时的进度条
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # --- A. 创建临时环境 ---
        # 创建一个临时目录来模拟你的本地文件夹结构
        with tempfile.TemporaryDirectory() as temp_dir:
            status_text.text("正在准备环境...")
            
            # 构造原来脚本需要的目录结构: temp_dir/2025_Q1
            folder_name = f"{year}_{quarter}"
            quarter_folder = os.path.join(temp_dir, folder_name)
            os.makedirs(quarter_folder, exist_ok=True)

            # 把用户上传的文件，保存到这个临时文件夹里
            for uploaded_file in uploaded_files:
                file_path = os.path.join(quarter_folder, uploaded_file.name)
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
            
            progress_bar.progress(30)
            status_text.text("文件已上传，正在运行监管流水线...")

            # --- B. 调用你的 utils 逻辑 ---
            # 设置中间结果目录
            intermediate_dir = os.path.join(temp_dir, f"{quarter}_intermediate")
            
            # 1. 运行四大监管流水线
            # 注意：这里直接调用你的 utils，路径传的是临时目录
            results, stats_dict = utils.run_all_pipelines_and_save_intermediate(
                quarter_folder=quarter_folder,
                year=int(year),
                quarter=quarter,
                save_dir=intermediate_dir
            )
            
            progress_bar.progress(70)
            status_text.text("流水线完成，正在生成最终汇总表...")

            # 2. 生成最终总表
            # 最终文件先保存到临时目录
            final_filename = f"{year}_{quarter}_{operator}_自存.xlsx"
            final_output_path = os.path.join(temp_dir, final_filename)
            
            # 注意：template_columns.json 就在当前运行目录下，直接传文件名即可
            utils.align_and_export_to_self_template_by_json(
                template_json_path="template_columns.json",
                output_excel_path=final_output_path,
                df_nmpa=results.get("NMPA"),
                df_fda=results.get("FDA"),
                df_ind=results.get("IND"),
                df_nda=results.get("NDA"),
                stats_dict=stats_dict
            )

            progress_bar.progress(100)
            status_text.text("处理完成！")

            # --- C. 读取生成的结果供下载 ---
            with open(final_output_path, "rb") as f:
                excel_data = f.read()

            st.success(f"✅ 处理成功！共处理 {len(uploaded_files)} 个文件。")
            
            # 显示下载按钮
            st.download_button(
                label=f"📥 下载最终文件：{final_filename}",
                data=excel_data,
                file_name=final_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # (可选) 如果你想让用户也能下载中间结果，可以把 intermediate_dir 打包成 zip 提供下载
            # 这里为了简单先只提供最终 Excel

    except Exception as e:
        st.error(f"❌ 发生错误：{e}")
        # 打印详细报错方便调试
        import traceback
        st.code(traceback.format_exc())