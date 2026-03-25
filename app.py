import streamlit as st
import streamlit.components.v1 as components

# 페이지 기본 설정
st.set_page_config(
    layout="wide",
    page_title="Project HEARTBEAT | Live",
    page_icon="💓",
    initial_sidebar_state="collapsed"
)

# Streamlit 기본 UI 숨기기 (깔끔한 HTML 표시를 위해)
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

# HTML 파일 읽어서 표시
try:
    # index.html 파일 읽기
    with open("index.html", "r", encoding="utf-8") as file:
        html_content = file.read()
    
    # HTML을 화면에 표시 (높이는 필요에 따라 조정 가능)
    components.html(
        html_content,
        height=1000,
        scrolling=True
    )
    
except FileNotFoundError:
    st.error("❌ index.html 파일을 찾을 수 없습니다.")
    st.info("💡 app.py와 index.html 파일이 같은 폴더에 있는지 확인하세요.")
    
except Exception as e:
    st.error(f"❌ 오류가 발생했습니다: {str(e)}")
    st.info("💡 파일 인코딩이나 내용을 확인해보세요.")
