// Quản lý trạng thái giao diện
let currentTab = 'dashboard';
let currentFilter = 'all';
let fullLogs = '';
let localCleared = false; // Xóa tạm thời phía client
let lastFetchedLogs = '';

// Khởi chạy ứng dụng
document.addEventListener('DOMContentLoaded', () => {
    // Bắt đầu lắng nghe thay đổi của form để cập nhật preview tin nhắn
    const form = document.getElementById('customOrderForm');
    if (form) {
        ['input', 'change'].forEach(evtType => {
            form.addEventListener(evtType, updateMessagePreview);
        });
        updateMessagePreview();
    }

    // Thiết lập vòng lặp tải logs mỗi 1 giây
    setInterval(loadLogs, 1000);
    loadLogs();
});

// Chuyển đổi giữa các Tab điều hướng
function switchTab(tabId) {
    currentTab = tabId;
    
    // Cập nhật trạng thái Active trên Sidebar
    document.querySelectorAll('.nav-item').forEach(btn => {
        btn.classList.remove('active');
    });
    const activeBtn = Array.from(document.querySelectorAll('.nav-item')).find(btn => 
        btn.getAttribute('onclick').includes(tabId)
    );
    if (activeBtn) activeBtn.classList.add('active');

    // Hiển thị section tab tương ứng
    document.querySelectorAll('.tab-section').forEach(sec => {
        sec.classList.remove('active');
    });
    const activeSection = document.getElementById(`tab-${tabId}`);
    if (activeSection) {
        activeSection.classList.add('active');
    }

    // Nếu chuyển sang tab Rebalance, vẽ lại liên kết sau khi DOM hiển thị
    if (tabId === 'rebalance') {
        setTimeout(parseAndRenderTopology, 100);
    }
}

// Cập nhật giao diện khi chọn radio card
function updateRadioCard(radioInput) {
    document.querySelectorAll('.radio-card').forEach(card => {
        card.classList.remove('active');
    });
    radioInput.closest('.radio-card').classList.add('active');
    updateMessagePreview();
}

// Cập nhật chuỗi preview bản tin gửi vào Kafka
function updateMessagePreview() {
    const orderId = document.getElementById('orderId')?.value || 'Order-Custom';
    const product = document.getElementById('productName')?.value || 'Product';
    const qty = document.getElementById('quantity')?.value || '1';
    const failureMode = document.querySelector('input[name="failureMode"]:checked')?.value || 'success';

    let msg = `${orderId},${product},${qty}`;
    if (failureMode === 'transient') {
        msg += '|fail-transient';
    } else if (failureMode === 'persistent') {
        msg += '|fail-persistent';
    }

    const previewEl = document.getElementById('demoMessagePreview');
    if (previewEl) {
        previewEl.textContent = msg;
    }
}

// Gửi đơn hàng nhanh từ trang Dashboard
async function quickSend(type) {
    try {
        let endpoint = "http://localhost:8081/send-success";
        if (type === 'failed') {
            endpoint = "http://localhost:8081/send-failed";
        }
        
        const res = await fetch(endpoint);
        const text = await res.text();
        showNotification(`Đã gửi: ${text}`, 'success');
        loadLogs();
    } catch (err) {
        showNotification('Không thể kết nối đến API Server!', 'error');
    }
}

// Gửi đơn hàng tùy biến từ Form cấu hình nâng cao
async function sendCustomOrder(event) {
    event.preventDefault();
    
    const orderId = document.getElementById('orderId').value;
    const product = document.getElementById('productName').value;
    const qty = document.getElementById('quantity').value;
    const failureMode = document.querySelector('input[name="failureMode"]:checked').value;

    try {
        const url = `http://localhost:8081/send-custom?id=${encodeURIComponent(orderId)}&product=${encodeURIComponent(product)}&qty=${encodeURIComponent(qty)}&mode=${encodeURIComponent(failureMode)}`;
        const res = await fetch(url);
        const text = await res.text();
        
        showNotification(`Đã gửi đơn tùy biến thành công!`, 'success');
        
        // Reset form và chuyển về tab Dashboard
        document.getElementById('customOrderForm').reset();
        document.querySelectorAll('.radio-card').forEach(card => card.classList.remove('active'));
        document.querySelector('input[value="success"]').closest('.radio-card').classList.add('active');
        updateMessagePreview();
        
        switchTab('dashboard');
    } catch (err) {
        showNotification('Lỗi khi gửi bản tin tùy biến sang API Server!', 'error');
    }
}

// Thiết lập bộ lọc logs hiển thị
function setLogFilter(filterType) {
    currentFilter = filterType;
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    const activeBtn = Array.from(document.querySelectorAll('.filter-btn')).find(btn => 
        btn.getAttribute('onclick').includes(filterType)
    );
    if (activeBtn) activeBtn.classList.add('active');
    
    renderLogs();
}

// Xóa log hiển thị tạm thời phía client
function clearLocalLogs() {
    localCleared = true;
    renderLogs();
    showNotification('Đã xóa tạm thời danh sách log hiển thị.', 'info');
}

// Tải nhật ký hệ thống từ API Server
async function loadLogs() {
    try {
        const response = await fetch("http://localhost:8081/logs");
        if (!response.ok) throw new Error("Cổng log lỗi");
        
        const text = await response.text();
        
        // Nếu có log mới
        if (text !== lastFetchedLogs) {
            lastFetchedLogs = text;
            fullLogs = text;
            
            // Cập nhật trạng thái API Online
            updateAPIStatus(true);
            
            // Render logs lên terminal
            renderLogs();
            
            // Tính toán lại các số liệu thống kê
            calculateStats(text);
            
            // Vẽ lại sơ đồ Rebalance
            parseAndRenderTopology();
        }
    } catch (err) {
        // Cập nhật trạng thái API Offline
        updateAPIStatus(false);
    }
}

// Cập nhật trạng thái hiển thị của API Server trên Sidebar
function updateAPIStatus(isOnline) {
    const pulse = document.getElementById('brokerStatusPulse');
    const text = document.getElementById('brokerStatusText');
    
    if (isOnline) {
        pulse.className = 'pulse-indicator online';
        text.textContent = 'API Server: ONLINE';
    } else {
        pulse.className = 'pulse-indicator offline';
        text.textContent = 'API Server: OFFLINE';
        document.getElementById('logs').textContent = "[OFFLINE] Không thể kết nối với API Server chạy trên http://localhost:8081\nHãy chắc chắn bạn đã chạy lớp DashboardServer Java.";
    }
}

// Hiển thị log ra terminal có lọc và phân màu sắc
function renderLogs() {
    if (localCleared) {
        document.getElementById("logs").innerHTML = `<span style="color: var(--text-dark)">-- Log đã được dọn sạch (Chờ dữ liệu log tiếp theo...) --</span>`;
        if (fullLogs !== '') {
            // Khi có log mới, bỏ cờ xóa
            localCleared = false;
        } else {
            return;
        }
    }

    if (!fullLogs) {
        document.getElementById("logs").textContent = "Không có dữ liệu nhật ký nào.";
        return;
    }

    const lines = fullLogs.trim().split('\n');
    let outputHTML = '';
    
    lines.forEach(line => {
        if (!line.trim()) return;

        // Áp dụng bộ lọc log
        if (currentFilter === 'success' && !line.includes('SUCCESS')) return;
        if (currentFilter === 'retry' && !line.includes('RETRY')) return;
        if (currentFilter === 'dlq' && !line.includes('DLQ')) return;
        if (currentFilter === 'rebalance' && !line.includes('REBALANCE')) return;

        // Phân màu sắc cho từng loại log
        let styledLine = line;
        if (line.includes('SUCCESS')) {
            styledLine = `<span style="color: var(--success)"><i class="fa-solid fa-circle-check"></i> ${line}</span>`;
        } else if (line.includes('RETRY')) {
            styledLine = `<span style="color: var(--warning)"><i class="fa-solid fa-arrows-rotate"></i> ${line}</span>`;
        } else if (line.includes('DLQ')) {
            styledLine = `<span style="color: var(--danger)"><i class="fa-solid fa-skull-crossbones"></i> ${line}</span>`;
        } else if (line.includes('REBALANCE')) {
            styledLine = `<span style="color: var(--primary)"><i class="fa-solid fa-diagram-project"></i> ${line}</span>`;
        } else if (line.includes('PRODUCER SENT')) {
            styledLine = `<span style="color: #38bdf8"><i class="fa-solid fa-paper-plane"></i> ${line}</span>`;
        }

        outputHTML += styledLine + '\n';
    });

    const logsContainer = document.getElementById("logs");
    if (logsContainer) {
        logsContainer.innerHTML = outputHTML || `<span style="color: var(--text-dark)">-- Không tìm thấy dòng nhật ký nào khớp bộ lọc --</span>`;
        // Tự động cuộn xuống cuối terminal
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }
}

// Thống kê số lượng bản tin
function calculateStats(logs) {
    // Đếm các mẫu trong logs
    const success = (logs.match(/SUCCESS/g) || []).length;
    const retry = (logs.match(/RETRY/g) || []).length;
    const dlq = (logs.match(/DLQ/g) || []).length;

    // Cập nhật giá trị số
    document.getElementById("successCount").innerText = success;
    document.getElementById("retryCount").innerText = retry;
    document.getElementById("dlqCount").innerText = dlq;

    // Tính toán tỷ lệ phần trăm để cập nhật thanh tiến trình
    const total = success + retry + dlq;
    if (total > 0) {
        document.getElementById("successProgress").style.width = `${(success / total) * 100}%`;
        document.getElementById("retryProgress").style.width = `${(retry / total) * 100}%`;
        document.getElementById("dlqProgress").style.width = `${(dlq / total) * 100}%`;
    } else {
        document.getElementById("successProgress").style.width = '0%';
        document.getElementById("retryProgress").style.width = '0%';
        document.getElementById("dlqProgress").style.width = '0%';
    }

    // Cập nhật trực quan Node Consumer trong quy trình Pipeline
    const consNode = document.getElementById('consumerNode');
    if (consNode) {
        if (dlq > 0) {
            consNode.className = 'step-circle danger-solid';
        } else if (retry > 0) {
            consNode.className = 'step-circle warning-solid';
        } else if (success > 0) {
            consNode.className = 'step-circle success-solid';
        } else {
            consNode.className = 'step-circle';
        }
    }
}

// Phân tích logs Rebalance và vẽ sơ đồ phân phối phân vùng
function parseAndRenderTopology() {
    if (!fullLogs) return;

    let assignments = {};      // consumerName -> Array of Partitions
    let activeConsumers = new Set();
    
    const lines = fullLogs.trim().split('\n');
    
    // Duyệt qua log từ cũ đến mới để cập nhật trạng thái phân vùng thực tế
    lines.forEach(line => {
        // Tìm dòng gán partition: "REBALANCE ASSIGN: ConsumerName -> [orders-0, orders-1]"
        let assignMatch = line.match(/REBALANCE ASSIGN:\s*([^\s-]+)\s*->\s*\[(.*?)\]/);
        if (assignMatch) {
            let consumerName = assignMatch[1];
            activeConsumers.add(consumerName);
            let partsStr = assignMatch[2].trim();
            let parts = partsStr ? partsStr.split(',').map(p => p.trim()) : [];
            
            // Quy tắc độc quyền: Một phân vùng chỉ được gán cho tối đa 1 consumer
            // Loại bỏ các phân vùng này khỏi các consumer khác nếu có
            parts.forEach(p => {
                for (let c in assignments) {
                    if (c !== consumerName) {
                        assignments[c] = assignments[c].filter(x => x !== p);
                    }
                }
            });
            
            assignments[consumerName] = parts;
        }

        // Tìm dòng thu hồi: "REBALANCE REMOVE: ConsumerName -> [orders-0]"
        let removeMatch = line.match(/REBALANCE REMOVE:\s*([^\s-]+)\s*->\s*\[(.*?)\]/);
        if (removeMatch) {
            let consumerName = removeMatch[1];
            let partsStr = removeMatch[2].trim();
            let parts = partsStr ? partsStr.split(',').map(p => p.trim()) : [];
            if (assignments[consumerName]) {
                assignments[consumerName] = assignments[consumerName].filter(x => !parts.includes(x));
            }
        }
    });

    // Cập nhật DOM của Active Consumers trong tab Rebalance
    const activeConsumerList = document.getElementById('activeConsumerList');
    if (!activeConsumerList) return;

    activeConsumerList.innerHTML = '';
    let hasConsumers = false;

    for (let consumerName in assignments) {
        // Chỉ giữ lại consumer nếu nó có partition hoặc được đánh dấu active gần đây
        if (assignments[consumerName].length === 0 && !activeConsumers.has(consumerName)) {
            continue;
        }
        hasConsumers = true;
        
        const partBadges = assignments[consumerName].map(p => {
            const pNum = p.replace('orders-', 'P-');
            return `<span class="badge badge-primary" style="margin-top: 5px; margin-right: 5px;">${pNum}</span>`;
        }).join('');

        activeConsumerList.innerHTML += `
            <div class="consumer-node" id="c-${consumerName}">
                <div class="c-icon"><i class="fa-solid fa-user-gear"></i></div>
                <div class="c-info">
                    <span class="c-name">${consumerName}</span>
                    <span class="c-meta">${assignments[consumerName].length > 0 ? 'Được gán phân vùng' : 'Đang rảnh (Idle)'}</span>
                    <div class="c-partitions" style="margin-top: 4px;">${partBadges || '<span style="color: var(--text-dark); font-size: 11px;">Không có phân vùng</span>'}</div>
                </div>
            </div>
        `;
    }

    if (!hasConsumers) {
        activeConsumerList.innerHTML = `
            <div class="no-consumers">
                <i class="fa-solid fa-circle-exclamation"></i>
                <p>Chưa phát hiện sự kiện Rebalance nào trong log.</p>
                <span>Hãy khởi chạy ít nhất một Consumer và để ý log REBALANCE ASSIGN.</span>
            </div>
        `;
        // Clear SVG lines
        const svg = document.getElementById('topology-svg');
        if (svg) svg.innerHTML = '';
    } else {
        // Vẽ lại các đường nối SVG
        setTimeout(() => drawConnections(assignments), 50);
    }
}

// Vẽ các kết nối nối từ Partition Node sang Consumer Node
function drawConnections(assignments) {
    const svg = document.getElementById('topology-svg');
    if (!svg) return;
    
    svg.innerHTML = ''; // Xóa sạch các đường nối cũ
    
    const mapContainer = document.querySelector('.topology-map');
    if (!mapContainer) return;
    const mapRect = mapContainer.getBoundingClientRect();
    
    for (let consumerName in assignments) {
        const consumerEl = document.getElementById(`c-${consumerName}`);
        if (!consumerEl) continue;
        
        const cRect = consumerEl.getBoundingClientRect();
        // Lấy tọa độ y trung tâm của thẻ Consumer Node relative với Map Container
        const cy = (cRect.top + cRect.height / 2) - mapRect.top;
        
        assignments[consumerName].forEach(partitionName => {
            // Loại bỏ dấu ngoặc vuông nếu có trong tên
            const cleanPartName = partitionName.replace(/\[|\]/g, '').trim();
            const partNodeId = `p-${cleanPartName}`;
            const partEl = document.getElementById(partNodeId);
            if (!partEl) return;
            
            const pRect = partEl.getBoundingClientRect();
            // Lấy tọa độ y trung tâm của Partition Node relative với Map Container
            const py = (pRect.top + pRect.height / 2) - mapRect.top;
            
            // Vẽ đường cong Bezier nối từ x=0 (trái - partition) sang x=150 (phải - consumer)
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            const d = `M 0 ${py} C 75 ${py}, 75 ${cy}, 150 ${cy}`;
            path.setAttribute('d', d);
            path.setAttribute('stroke', '#6366f1');
            path.setAttribute('stroke-width', '3');
            path.setAttribute('fill', 'none');
            path.setAttribute('style', 'filter: drop-shadow(0 0 4px rgba(99, 102, 241, 0.45)); transition: all 0.3s ease;');
            
            svg.appendChild(path);
        });
    }
}

// Trực quan hóa thông báo đẩy (Notification)
function showNotification(message, type = 'info') {
    // Tạo notification element
    const notif = document.createElement('div');
    notif.style.position = 'fixed';
    notif.style.bottom = '24px';
    notif.style.right = '24px';
    notif.style.padding = '14px 20px';
    notif.style.borderRadius = '8px';
    notif.style.color = '#fff';
    notif.style.fontSize = '14px';
    notif.style.fontWeight = '500';
    notif.style.zIndex = '9999';
    notif.style.display = 'flex';
    notif.style.alignItems = 'center';
    notif.style.gap = '10px';
    notif.style.boxShadow = '0 10px 30px rgba(0,0,0,0.3)';
    notif.style.animation = 'fadeIn 0.3s ease-out';
    notif.style.backdropFilter = 'blur(10px)';

    let icon = '<i class="fa-solid fa-circle-info"></i>';
    if (type === 'success') {
        notif.style.backgroundColor = 'rgba(16, 185, 129, 0.85)';
        notif.style.border = '1px solid rgba(16, 185, 129, 0.3)';
        icon = '<i class="fa-solid fa-circle-check"></i>';
    } else if (type === 'error') {
        notif.style.backgroundColor = 'rgba(239, 68, 68, 0.85)';
        notif.style.border = '1px solid rgba(239, 68, 68, 0.3)';
        icon = '<i class="fa-solid fa-circle-exclamation"></i>';
    } else {
        notif.style.backgroundColor = 'rgba(99, 102, 241, 0.85)';
        notif.style.border = '1px solid rgba(99, 102, 241, 0.3)';
    }

    notif.innerHTML = `${icon} <span>${message}</span>`;
    document.body.appendChild(notif);

    // Tự động biến mất sau 3 giây
    setTimeout(() => {
        notif.style.animation = 'fadeIn 0.3s ease-out reverse';
        setTimeout(() => notif.remove(), 300);
    }, 3000);
}

// Bắt sự kiện thay đổi kích thước cửa sổ để vẽ lại đường nối SVG
window.addEventListener('resize', () => {
    if (currentTab === 'rebalance') {
        parseAndRenderTopology();
    }
});