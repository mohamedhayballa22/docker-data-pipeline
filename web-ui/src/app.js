document.addEventListener('DOMContentLoaded', function() {
    const statusEl = document.getElementById('status');
    const dataBodyEl = document.getElementById('dataBody');
    const refreshBtn = document.getElementById('refreshBtn');
    
    // API URL
    const API_URL = 'http://localhost:8000/data';
    
    // Function to fetch data from API
    async function fetchData() {
        setStatus('loading', 'Fetching data from API...');
        
        try {
            const response = await fetch(API_URL);
            
            if (!response.ok) {
                throw new Error(`API returned ${response.status} ${response.statusText}`);
            }
            
            const data = await response.json();
            
            // Display the data
            displayData(data);
            setStatus('success', `Successfully loaded ${data.count} items`);
            
        } catch (error) {
            console.error('Error fetching data:', error);
            setStatus('error', `Error: ${error.message}`);
            dataBodyEl.innerHTML = `<tr><td colspan="4">No data available. The pipeline might still be processing.</td></tr>`;
        }
    }
    
    // Function to display data in the table
    function displayData(data) {
        if (!data || !data.items || data.items.length === 0) {
            dataBodyEl.innerHTML = '<tr><td colspan="4">No data available</td></tr>';
            return;
        }
        
        const rows = data.items.map(item => `
            <tr>
                <td>${item.id}</td>
                <td>${escapeHtml(item.title)}</td>
                <td>${item.wordCount}</td>
                <td>${escapeHtml(item.summary)}</td>
            </tr>
        `).join('');
        
        dataBodyEl.innerHTML = rows;
    }
    
    // Helper function to set status message
    function setStatus(type, message) {
        statusEl.className = `status ${type}`;
        statusEl.textContent = message;
    }
    
    // Helper function to escape HTML
    function escapeHtml(unsafe) {
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
    
    // Add event listener for refresh button
    refreshBtn.addEventListener('click', fetchData);
    
    // Initial data fetch
    fetchData();
});