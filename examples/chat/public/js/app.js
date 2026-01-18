// PAGI-Channels Chat Client

let ws;
let username;
let room;

// DOM Elements
const loginScreen = document.getElementById('login-screen');
const chatScreen = document.getElementById('chat-screen');
const loginForm = document.getElementById('login-form');
const messageForm = document.getElementById('message-form');
const messagesDiv = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const usersList = document.getElementById('users-list');
const userCount = document.getElementById('user-count');
const currentRoom = document.getElementById('current-room');
const connectionStatus = document.getElementById('connection-status');

// Login form submit
loginForm.addEventListener('submit', (e) => {
    e.preventDefault();
    username = document.getElementById('username').value.trim() || 'anonymous';
    room = document.getElementById('room').value.trim() || 'general';
    connect();
});

// Message form submit
messageForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = messageInput.value.trim();
    if (text && ws && ws.readyState === WebSocket.OPEN) {
        // Send to server
        ws.send(JSON.stringify({ text }));
        // Show own message locally
        addMessage(username, text);
        messageInput.value = '';
    }
});

function connect() {
    loginScreen.classList.add('hidden');
    chatScreen.classList.remove('hidden');
    currentRoom.textContent = '#' + room;

    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${location.host}/ws/chat?user=${encodeURIComponent(username)}&room=${encodeURIComponent(room)}`;

    ws = new WebSocket(url);

    ws.onopen = () => {
        setStatus('connected', 'Connected');
        addSystemMessage('Connected to room: ' + room);
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMessage(data);
    };

    ws.onclose = () => {
        setStatus('disconnected', 'Disconnected');
        addSystemMessage('Disconnected from server');
    };

    ws.onerror = () => {
        setStatus('disconnected', 'Connection error');
    };
}

function handleMessage(data) {
    switch (data.type) {
        case 'users':
            // Initial user list
            updateUsersList(data.users);
            break;

        case 'chat.message':
            addMessage(data.user, data.text);
            break;

        case 'user_joined':
            addSystemMessage(data.user + ' joined', 'join');
            // Add to users list
            addUser(data.user);
            break;

        case 'user_left':
            addSystemMessage(data.user + ' left', 'leave');
            // Remove from users list
            removeUser(data.user);
            break;
    }
}

function addMessage(author, text) {
    const div = document.createElement('div');
    div.className = 'message';

    const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    div.innerHTML = `
        <div class="meta">
            <span class="author">${escapeHtml(author)}</span>
            <span class="time">${time}</span>
        </div>
        <div class="text">${escapeHtml(text)}</div>
    `;

    messagesDiv.appendChild(div);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
}

function addSystemMessage(text, type = '') {
    const div = document.createElement('div');
    div.className = 'message system ' + type;
    div.textContent = text;
    messagesDiv.appendChild(div);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
}

function updateUsersList(users) {
    usersList.innerHTML = '';
    users.forEach(u => {
        const li = document.createElement('li');
        li.textContent = u.user;
        li.dataset.user = u.user;
        usersList.appendChild(li);
    });
    userCount.textContent = users.length;
}

function addUser(name) {
    // Check if already exists
    if (usersList.querySelector(`[data-user="${name}"]`)) return;

    const li = document.createElement('li');
    li.textContent = name;
    li.dataset.user = name;
    usersList.appendChild(li);
    userCount.textContent = usersList.children.length;
}

function removeUser(name) {
    const li = usersList.querySelector(`[data-user="${name}"]`);
    if (li) {
        li.remove();
        userCount.textContent = usersList.children.length;
    }
}

function setStatus(state, text) {
    connectionStatus.className = 'status ' + state;
    connectionStatus.querySelector('span:last-child').textContent = text;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
