// main.js

document.addEventListener('DOMContentLoaded', function() {
    const form = document.querySelector('form');
    if (form) {
        form.addEventListener('submit', function(event) {
            // event.preventDefault(); // Prevent form submission
            const userInput = form.querySelector('input[name="user_input"]').value;
            // Send user input to server using fetch
            fetch('/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: 'user_input=' + encodeURIComponent(userInput)
            })
            .then(response => response.text())
            .then(html => {
                // Update the page with the returned HTML
                document.body.innerHTML = html;
                // Re-attach event listener to the new form
                document.addEventListener('DOMContentLoaded', arguments.callee);
            })
            .catch(error => console.error('Error:', error));
        });
    }
});