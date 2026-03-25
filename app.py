<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Project HEARTBEAT | Live</title>
    
    <!-- 라이브러리 로드 (버전 고정) -->
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.10/dayjs.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.10/plugin/utc.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.10/plugin/timezone.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.39.0/dist/umd/supabase.min.js"></script>
    
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0e1117;
            color: #fafafa;
            margin: 0;
            padding: 20px;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: #262730;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #3a3a3a;
        }
        .status-box {
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 15px;
            font-weight: bold;
        }
        .status-success {
            background-color: #1a2e1a;
            color: #00ff7f;
            border: 1px solid #00ff7f;
        }
        .status-error {
            background-color: #2d1a1a;
            color: #ff6b6b;
            border: 1px solid #ff6b6b;
        }
        .status-loading {
            background-color: #1a1a2e;
            color: #4a9eff;
            border: 1px solid #4a9eff;
        }
        input {
            width: 100%;
            padding: 12px;
            margin: 10px 0;
            background: #0e1117;
            border: 1px solid #4a4a4a;
            color: white;
            border-radius: 4px;
            font-size: 16px;
        }
        button {
            width: 100%;
            padding: 12px;
            margin: 10px 0;
            background: #ff4b4b;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-weight: bold;
            font-size: 16px;
            transition: background 0.3s;
        }
        button:hover:not(:disabled) {
            background: #ff6b6b;
        }
        button:disabled {
            background: #555;
            cursor: not-allowed;
        }
        .card {
            background: #1e1e1e;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 6px;
            border: 1px solid #333;
            cursor: pointer;
            transition: all 0.3s;
        }
        .card:hover {
            background: #2a2a2a;
            border-color: #ff4b4b;
        }
        .loading {
            text-align: center;
            color: #aaa;
            padding: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>💓 HEARTBEAT Dashboard</h1>
            <div id="connection-status" class="status-box status-loading">
                연결 확인 중...
            </div>
            <div id="db-time" style="font-size: 0.9rem; color: #aaa; margin-bottom: 15px;"></div>
            <p style="margin-bottom: 15px;">충전소명, 주소, 사이트명 등으로 검색하세요.</p>
            <input type="text" id="search-input" placeholder="예: 서울 아파트, 노원 에버온...">
            <button id="search-btn" onclick="performSearch()">🔍 검색</button>
        </div>
        <div id="result-area"></div>
    </div>

    <script>
        // ========================================
        // 중복 실행 방지 및 전역 변수 설정
        // ========================================
        if (!window.heartbeatApp) {
            window.heartbeatApp = {
                initialized: false,
                supabaseClient: null,
                dbLastTime: null
            };

            // ========================================
            // 초기화 함수
            // ========================================
            async function initializeApp() {
                const statusDiv = document.getElementById('connection-status');
                const dbTimeDiv = document.getElementById('db-time');
                
                try {
                    statusDiv.textContent = "라이브러리 로딩 중...";
                    
                    // Day.js 플러그인 초기화
                    if (typeof dayjs !== 'undefined' && window.dayjs_plugin_utc) {
                        dayjs.extend(window.dayjs_plugin_utc);
                        dayjs.extend(window.dayjs_plugin_timezone);
                    }

                    // Supabase 설정
                    const SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co";
                    const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg";

                    statusDiv.textContent = "데이터베이스 연결 중...";

                    // 라이브러리 확인
                    if (!window.supabase) {
                        throw new Error("Supabase 라이브러리가 로드되지 않았습니다.");
                    }

                    // 클라이언트 생성 (중복 방지)
                    if (!window.heartbeatApp.supabaseClient) {
                        window.heartbeatApp.supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
                    }

                    // 연결 및 최신 시간 테스트
                    const { data, error } = await window.heartbeatApp.supabaseClient
                        .from('status_history')
                        .select('collected_at')
                        .order('collected_at', { ascending: false })
                        .limit(1);
                    
                    if (error) throw error;

                    // 성공 처리
                    if (data && data.length > 0) {
                        window.heartbeatApp.dbLastTime = dayjs(data[0].collected_at);
                        dbTimeDiv.textContent = `🕒 DB 최종 수신: ${window.heartbeatApp.dbLastTime.format('YYYY-MM-DD HH:mm')}`;
                    }

                    statusDiv.textContent = "✅ 연결 성공";
                    statusDiv.className = "status-box status-success";
                    
                    console.log('✅ HEARTBEAT 앱 초기화 완료');

                } catch (error) {
                    console.error('❌ 초기화 실패:', error);
                    statusDiv.textContent = `❌ 연결 실패: ${error.message}`;
                    statusDiv.className = "status-box status-error";
                }
            }

            // ========================================
            // 검색 함수
            // ========================================
            async function performSearch() {
                const keyword = document.getElementById('search-input').value.trim();
                const resultArea = document.getElementById('result-area');
                const searchBtn = document.getElementById('search-btn');
                
                if (!keyword) {
                    alert("검색어를 입력하세요");
                    return;
                }

                if (!window.heartbeatApp.supabaseClient) {
                    alert("데이터베이스가 연결되지 않았습니다.");
                    return;
                }

                // 로딩 상태
                searchBtn.disabled = true;
                searchBtn.textContent = "검색 중...";
                resultArea.innerHTML = '<div class="loading">🔄 검색 중...</div>';

                try {
                    // 키워드 토큰화
                    const tokens = keyword.split(/\s+/).filter(t => t.length > 0);
                    const primaryKeyword = tokens.reduce((a, b) => a.length >= b.length ? a : b);
                    const pattern = `%${primaryKeyword}%`;

                    // 기본 검색
                    const { data, error } = await window.heartbeatApp.supabaseClient
                        .from('charger_master')
                        .select('*')
                        .or(`station_name.ilike.${pattern},address1.ilike.${pattern},address_detail.ilike.${pattern},site_id.ilike.${pattern},station_id.ilike.${pattern}`)
                        .limit(100);

                    if (error) throw error;

                    if (!data || data.length === 0) {
                        resultArea.innerHTML = '<div style="padding:30px; text-align:center; color:#888;">검색 결과가 없습니다.</div>';
                        return;
                    }

                    // 추가 토큰 필터링
                    let filteredData = data;
                    const otherTokens = tokens.filter(t => t !== primaryKeyword);
                    
                    for (const token of otherTokens) {
                        filteredData = filteredData.filter(row => {
                            const searchText = [
                                row.station_name,
                                row.address1,
                                row.address_detail,
                                row.site_id,
                                row.station_id
                            ].join(' ').toLowerCase();
                            return searchText.includes(token.toLowerCase());
                        });
                    }

                    // 사이트별 그룹핑
                    const siteGroups = groupBySite(filteredData);

                    // 결과 렌더링
                    renderSearchResults(siteGroups);

                } catch (error) {
                    console.error('검색 오류:', error);
                    resultArea.innerHTML = `<div class="status-box status-error">검색 오류: ${error.message}</div>`;
                } finally {
                    searchBtn.disabled = false;
                    searchBtn.textContent = "🔍 검색";
                }
            }

            // ========================================
            // 사이트 그룹핑 함수
            // ========================================
            function groupBySite(data) {
                const groups = new Map();

                data.forEach(row => {
                    let key = row.site_id || row.station_id || 'unknown';
                    
                    if (!groups.has(key)) {
                        groups.set(key, {
                            key: key,
                            station_name: row.station_name || '',
                            address: row.address1 || '',
                            site_id: row.site_id,
                            station_id: row.station_id,
                            count: 0,
                            chargers: []
                        });
                    }

                    const group = groups.get(key);
                    group.count++;
                    group.chargers.push(row);
                });

                return Array.from(groups.values()).sort((a, b) => b.count - a.count);
            }

            // ========================================
            // 검색 결과 렌더링
            // ========================================
            function renderSearchResults(siteGroups) {
                const resultArea = document.getElementById('result-area');
                
                let html = `
                    <div style="margin-bottom: 20px; padding: 15px; background: #1a2e1a; border-radius: 8px; border: 1px solid #00ff7f;">
                        <strong style="color: #00ff7f;">✅ ${siteGroups.length}개 사이트 검색됨</strong>
                    </div>
                `;

                siteGroups.forEach((group, index) => {
                    const displayName = group.station_name || group.key;
                    const address = group.address || '';
                    
                    html += `
                        <div class="card" onclick="selectSite(${index})" data-site-index="${index}">
                            <div style="display: flex; justify-content: space-between; align-items: start;">
                                <div style="flex: 1;">
                                    <h3 style="margin: 0 0 8px 0; color: #ff4b4b; font-size: 1.2rem;">
                                        ${displayName}
                                    </h3>
                                    <div style="color: #ccc; margin-bottom: 5px; font-size: 0.95rem;">
                                        📍 ${address}
                                    </div>
                                    <div style="font-size: 0.8rem; color: #888;">
                                        🆔 ${group.site_id || group.station_id}
                                    </div>
                                </div>
                                <div style="background: #ff4b4b; color: white; padding: 8px 12px; border-radius: 20px; font-size: 0.8rem; font-weight: bold; white-space: nowrap; margin-left: 15px;">
                                    ${group.count}대
                                </div>
                            </div>
                        </div>
                    `;
                });

                resultArea.innerHTML = html;
                
                // 전역 변수에 저장 (사이트 선택용)
                window.heartbeatApp.currentSearchResults = siteGroups;
            }

            // ========================================
            // 사이트 선택 함수
            // ========================================
            window.selectSite = function(index) {
                const siteGroup = window.heartbeatApp.currentSearchResults[index];
                if (!siteGroup) return;

                alert(`선택된 사이트: ${siteGroup.station_name}\n충전기 ${siteGroup.count}대\n\n상세 대시보드 기능은 곧 추가될 예정입니다.`);
            };

            // ========================================
            // 앱 시작
            // ========================================
            document.addEventListener('DOMContentLoaded', () => {
                if (!window.heartbeatApp.initialized) {
                    initializeApp();
                    window.heartbeatApp.initialized = true;
                }
            });

            // 엔터키 검색 지원
            document.addEventListener('DOMContentLoaded', () => {
                const searchInput = document.getElementById('search-input');
                if (searchInput) {
                    searchInput.addEventListener('keypress', (e) => {
                        if (e.key === 'Enter') {
                            performSearch();
                        }
                    });
                }
            });

        } // window.heartbeatApp 체크 끝
    </script>
</body>
</html>
