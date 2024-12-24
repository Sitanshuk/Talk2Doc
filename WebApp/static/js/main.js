function sendMessage() {
    var message = document.getElementById('user-input').value;
    fetch('/chatbot', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({message: message}),
    })
    .then(response => response.json())
    .then(data => {
        displayMessage(data.response);
    });
}

function displayMessage(message) {
    var chatbox = document.getElementById('chatbox');
    chatbox.innerHTML += '<p>' + message + '</p>';
}