import streamlit as st
import streamlit.components.v1 as components
import os

# 페이지 기본 설정
st.set_page_config(
    layout="wide",
    page_title="Project HEARTBEAT | Live",
    page_icon="💓",
    initial_sidebar_state="collapsed"
)

# Streamlit 기본 UI 숨기기
st.markdown("""
    <style>
        .block-container {
            padding: 0 !important;
            max-width: 100% !important;
        }
        header[data-testid="stHeader"] {
            display: none !important;
        }
        .stDeployButton {
            display: none !important;
        }
        footer {
            display: none !important;
        }
        #MainMenu {
            visibility: hidden;
        }
    </style>
""", unsafe_allow_html=True)

# HTML 파일 로드 및 표시
try:
    with open("index.html", "r", encoding="utf-8") as file:
        html_content = file.read()
    
    # HTML 렌더링
    components.html(
        html_content,
        height=1200,
        scrolling=True
    )
    
except FileNotFoundError:
    st.error("❌ index.html 파일을 찾을 수 없습니다.")
    st.info("💡 GitHub 저장소에 index.html 파일이 올바르게 업로드되었는지 확인하세요.")
    
except Exception as e:
    st.error(f"❌ 파일 로딩 중 오류가 발생했습니다: {str(e)}")
