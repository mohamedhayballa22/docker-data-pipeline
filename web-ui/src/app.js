document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const jobsContainer = document.getElementById('jobsContainer');
    const refreshBtn = document.getElementById('refreshBtn');
    const refreshBtnText = document.getElementById('refreshBtnText');
    const loadingSpinner = document.getElementById('loadingSpinner');
    const pipelineSuccess = document.getElementById('pipelineSuccess');
    const pipelineError = document.getElementById('pipelineError');
    const progressContainer = document.getElementById('progressContainer');
    const progressBar = document.getElementById('progressBar');
    const progressDescription = document.getElementById('progressDescription');

    // State
    let socket = null;
    const websocketUrl = 'ws://localhost:8000/ws';

    // Initialization
    fetchJobs();
    connectWebSocket();
    refreshBtn.addEventListener('click', triggerPipeline);

    // WebSocket Functions
    function connectWebSocket() {
        console.log('Attempting to connect WebSocket...');
        socket = new WebSocket(websocketUrl);

        socket.onopen = () => {
            console.log('WebSocket connection established.');
            pipelineError.classList.add('d-none');
        };

        socket.onmessage = (event) => {
            try {
                const message = JSON.parse(event.data);
                console.log('WebSocket message received:', message);

                if (message.type === 'status_update' && message.data) {
                    const { percentage, description, status } = message.data;

                    if (!progressContainer.classList.contains('d-none')) {
                        updateProgressBar(percentage, description);

                        if (percentage === 100.0 || status === 'COMPLETE') {
                            console.log('Pipeline completed via WebSocket message.');
                            fetchJobs().then(() => {
                                showSuccessMessage();
                                resetUIState();
                            }).catch(error => {
                                console.error('Failed to fetch jobs after pipeline completion:', error);
                                showErrorMessage('Failed to load updated jobs after pipeline completion.');
                                resetUIState();
                            });
                        }
                    }
                } else if (message.type === 'initial_state') {
                    console.log('Received initial state:', message.jobs);
                } else {
                    console.warn('Received unknown message type:', message.type);
                }
            } catch (error) {
                console.error('Error processing WebSocket message:', error);
                console.error('Received data:', event.data);
            }
        };

        socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            showErrorMessage('WebSocket connection error. Real-time updates may be unavailable.');
            if (!progressContainer.classList.contains('d-none')) {
               resetUIState();
            }
        };

        socket.onclose = (event) => {
            console.log('WebSocket connection closed:', event.code, event.reason);
            if (!progressContainer.classList.contains('d-none') && event.code !== 1000) {
               showErrorMessage('WebSocket connection lost unexpectedly.');
               resetUIState();
            }
        };
    }

    // API & Data Handling Functions
    async function fetchJobs() {
        console.log('Fetching jobs data...');
        jobsContainer.innerHTML = `
            <div class="col-12 text-center py-5">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="mt-2">Loading jobs...</p>
            </div>
        `;
        try {
            const response = await fetch('http://127.0.0.1:8000/data');

            if (!response.ok) {
                throw new Error(`Failed to fetch jobs (${response.status} ${response.statusText})`);
            }

            const jobs = await response.json();
            displayJobs(jobs);
            console.log('Jobs fetched and displayed successfully.');
        } catch (error) {
            console.error('Error fetching jobs:', error);
            jobsContainer.innerHTML = `
                <div class="col-12 text-center">
                    <div class="alert alert-danger">
                        ${error.message || 'Failed to load jobs. Please try again later.'}
                    </div>
                </div>
            `;
            throw error;
        }
    }

    function displayJobs(jobs) {
        if (!jobs || jobs.length === 0) {
            jobsContainer.innerHTML = `
                <div class="col-12 text-center">
                    <div class="alert alert-info">
                        No jobs found. Click "Refresh Jobs" to fetch new data.
                    </div>
                </div>
            `;
            return;
        }

        jobs.sort((a, b) => new Date(b.date_posted) - new Date(a.date_posted));

        let jobsHTML = '';
        jobs.forEach(job => {
            const postedDate = new Date(job.date_posted).toLocaleDateString();
            const scrapedDate = new Date(job.date_scraped).toLocaleString();

            const skillsHTML = (job.skills || [])
                .map(skillObj => `<span class="badge bg-secondary skill-badge">${skillObj.skill}</span>`)
                .join('');

            let statusBadgeClass = 'bg-secondary';
            if (job.progress === 'active') statusBadgeClass = 'bg-success';
            else if (job.progress === 'pending') statusBadgeClass = 'bg-warning text-dark';
            else if (job.progress === 'rejected') statusBadgeClass = 'bg-danger';

            jobsHTML += `
                <div class="col-md-6 col-lg-4 mb-4 d-flex align-items-stretch">
                    <div class="card job-card w-100">
                        <div class="card-body d-flex flex-column">
                            <h5 class="card-title">${job.title || 'N/A'}</h5>
                            <h6 class="card-subtitle mb-2 text-muted">${job.company_name || 'N/A'}</h6>
                            <p class="job-location mb-1">
                                <i class="bi bi-geo-alt-fill"></i> ${job.location || 'N/A'}
                            </p>
                            <div class="mb-2">
                                <span class="badge ${statusBadgeClass}">${job.progress || 'unknown'}</span>
                            </div>
                            <div class="mb-2 skills-container">
                                ${skillsHTML || '<span class="text-muted small">No skills listed</span>'}
                            </div>
                            <p class="job-date small text-muted mb-1">Posted: ${postedDate}</p>
                            <p class="job-date small text-muted mb-3">Scraped: ${scrapedDate}</p>
                            <div class="mt-auto">
                                <a href="${job.job_url}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-primary job-url">View Original Job</a>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        jobsContainer.innerHTML = jobsHTML;
    }

    async function triggerPipeline() {
        console.log('Triggering pipeline...');
        refreshBtn.disabled = true;
        refreshBtnText.textContent = 'Pipeline Running';
        loadingSpinner.classList.remove('d-none');
        pipelineSuccess.classList.add('d-none');
        pipelineError.classList.add('d-none');

        progressContainer.classList.remove('d-none');
        updateProgressBar(0, 'Initializing pipeline...');

        try {
            if (!socket || socket.readyState !== WebSocket.OPEN) {
                console.log('WebSocket not open. Attempting to reconnect before triggering...');
                connectWebSocket();
                await new Promise(resolve => setTimeout(resolve, 1000));
                if (!socket || socket.readyState !== WebSocket.OPEN) {
                   throw new Error('WebSocket connection failed. Cannot trigger pipeline.');
                }
            }

            const response = await fetch('http://127.0.0.1:8000/trigger-job-pipeline', {
                method: 'POST'
            });

            if (!response.ok) {
                const errorData = await response.text();
                throw new Error(`Pipeline trigger failed: ${response.status} ${response.statusText}. ${errorData}`);
            }

            console.log('Pipeline trigger request sent successfully.');

        } catch (error) {
            console.error('Error triggering pipeline:', error);
            showErrorMessage(error.message || 'Failed to trigger the pipeline. Please check console and try again.');
            resetUIState();
        }
    }

    // UI Update Functions
    function updateProgressBar(percentage, description) {
        const clampedPercentage = Math.max(0, Math.min(100, percentage));
        progressBar.style.width = `${clampedPercentage}%`;
        progressBar.textContent = `${clampedPercentage.toFixed(0)}%`;
        progressBar.setAttribute('aria-valuenow', clampedPercentage);
        progressDescription.textContent = description || '';
        if (clampedPercentage < 100) {
            progressBar.classList.add('progress-bar-animated', 'progress-bar-striped');
            progressBar.classList.remove('bg-success');
        } else {
            progressBar.classList.remove('progress-bar-animated', 'progress-bar-striped');
            progressBar.classList.add('bg-success');
        }
    }

    function resetUIState() {
        console.log('Resetting UI state.');
        refreshBtn.disabled = false;
        refreshBtnText.textContent = 'Refresh Jobs';
        loadingSpinner.classList.add('d-none');
        progressContainer.classList.add('d-none');
        updateProgressBar(0, '');
    }

    function showSuccessMessage() {
        pipelineSuccess.classList.remove('d-none');
        pipelineError.classList.add('d-none');
        setTimeout(() => {
            pipelineSuccess.classList.add('d-none');
        }, 5000);
    }

    function showErrorMessage(message) {
        pipelineError.textContent = message;
        pipelineError.classList.remove('d-none');
        pipelineSuccess.classList.add('d-none');
    }

});