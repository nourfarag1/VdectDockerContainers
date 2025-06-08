document.addEventListener('DOMContentLoaded', function() {
    const incidentFeed = document.getElementById('incident-feed');
    const logContainer = document.getElementById('log-container');

    // Function to update the incident feed
    function updateIncidents(events) {
        incidentFeed.innerHTML = ''; // Clear existing incidents
        events.forEach(event => {
            const card = document.createElement('div');
            card.className = 'incident-card';

            // Thumbnail
            const thumbnail = document.createElement('img');
            thumbnail.className = 'thumbnail';
            thumbnail.src = event.thumbnail_url || '/static/placeholder.png'; // Use a placeholder if no thumbnail

            // Details
            const details = document.createElement('div');
            details.className = 'incident-details';

            const title = document.createElement('h3');
            title.textContent = `Violence Detected: ${event.camera_id}`;
            
            const timestamp = document.createElement('p');
            timestamp.textContent = `Time: ${new Date(event.timestamp).toLocaleString()}`;
            
            const video = document.createElement('p');
            video.textContent = `Video: ${event.video_filename}`;

            // Download Button
            const downloadLink = document.createElement('a');
            downloadLink.className = 'download-btn';
            downloadLink.href = event.video_url;
            downloadLink.textContent = 'Download Clip';
            downloadLink.target = '_blank'; // Open in new tab

            details.appendChild(title);
            details.appendChild(timestamp);
            details.appendChild(video);
            details.appendChild(downloadLink);

            card.appendChild(thumbnail);
            card.appendChild(details);

            incidentFeed.prepend(card); // Add new incidents to the top
        });
    }

    // Function to update the logs
    function updateLogs(logs) {
        logContainer.innerHTML = ''; // Clear existing logs
        logs.forEach(log => {
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry';
            logEntry.textContent = log;
            logContainer.prepend(logEntry); // Add new logs to the top
        });
    }

    // Fetch data from the API periodically
    async function fetchData() {
        try {
            const response = await fetch('/api/data');
            if (!response.ok) {
                console.error('Failed to fetch data from API');
                return;
            }
            const data = await response.json();
            updateIncidents(data.events);
            updateLogs(data.logs);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    }

    // Initial fetch and then poll every 3 seconds
    fetchData();
    setInterval(fetchData, 3000);
}); 