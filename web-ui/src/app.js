document.addEventListener('DOMContentLoaded', () => {
    const jobsContainer = document.getElementById('jobsContainer');
    const openPipelineModalBtn = document.getElementById('openPipelineModalBtn');
    const runPipelineBtn = document.getElementById('runPipelineBtn');
    const loadingSpinner = document.getElementById('loadingSpinner');
    const pipelineSuccess = document.getElementById('pipelineSuccess');
    const pipelineError = document.getElementById('pipelineError');
    const progressContainer = document.getElementById('progressContainer');
    const progressBar = document.getElementById('progressBar');
    const progressDescription = document.getElementById('progressDescription');

    const pipelineConfigModalEl = document.getElementById('pipelineConfigModal');
    const pipelineConfigForm = document.getElementById('pipelineConfigForm');
    const locationInput = document.getElementById('location');
    const maxJobsInput = document.getElementById('maxJobs');
    const newJobTitleInput = document.getElementById('newJobTitle');
    const addJobTitleBtn = document.getElementById('addJobTitleBtn');
    const jobTitlesList = document.getElementById('jobTitlesList');
    const jobTitlesHidden = document.getElementById('jobTitlesHidden');
    const submitPipelineConfigBtn = document.getElementById('submitPipelineConfigBtn');
    const submitBtnText = document.getElementById('submitBtnText');
    const submitSpinner = document.getElementById('submitSpinner');
    const modalAlertContainer = document.getElementById('modalAlertContainer');

    const pipelineConfigModal = new bootstrap.Modal(pipelineConfigModalEl);

    let socket = null;
    const websocketUrl = `ws://${window.location.hostname}:8000/ws`;
    const apiBaseUrl = `http://${window.location.hostname}:8000`;

    function getAddedJobTitles() {
        return Array.from(jobTitlesList.querySelectorAll('.job-title-text')).map(span => span.textContent);
    }

    function updateJobTitlesHidden() {
        const titles = getAddedJobTitles();
        jobTitlesHidden.value = titles.length > 0 ? 'filled' : '';
        console.log('Updated hidden job titles:', jobTitlesHidden.value);
    }

    function addJobTitleToList(title) {
        title = title.trim();
        if (!title) return false;

        const existingTitles = getAddedJobTitles().map(t => t.toLowerCase());
        if (existingTitles.includes(title.toLowerCase())) {
            console.warn(`Job title "${title}" already exists.`);
            return false;
        }

        const listItem = document.createElement('li');
        listItem.className = 'list-group-item d-flex justify-content-between align-items-center';

        const titleSpan = document.createElement('span');
        titleSpan.className = 'job-title-text';
        titleSpan.textContent = title;

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'btn btn-danger btn-sm remove-job-title-btn py-0 px-1 lh-1';
        removeBtn.innerHTML = '×';
        removeBtn.ariaLabel = 'Remove job title';
        removeBtn.onclick = () => {
            listItem.remove();
            updateJobTitlesHidden();
        };

        listItem.appendChild(titleSpan);
        listItem.appendChild(removeBtn);
        jobTitlesList.appendChild(listItem);
        updateJobTitlesHidden();
        return true;
    }

    function handleAddJobTitle() {
        const title = newJobTitleInput.value.trim();
        if (title) {
            const added = addJobTitleToList(title);
            if (added) {
                newJobTitleInput.value = '';
                clearModalAlert();
            } else {
                showModalAlert('Job title already added or is empty.', 'warning');
            }
        }
        newJobTitleInput.focus();
    }

    function connectWebSocket() {
        console.log('Attempting to connect WebSocket...');
        if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
            console.log('WebSocket already open or connecting.');
            return;
        }

        socket = new WebSocket(websocketUrl);

        socket.onopen = () => {
            console.log('WebSocket connection established.');
            clearMainAlerts();
        };

        socket.onmessage = (event) => {
            try {
                const message = JSON.parse(event.data);
                console.log('WebSocket message received:', message);

                if (message.type === 'status_update' && message.data) {
                    handleStatusUpdate(message.data);
                } else if (message.type === 'initial_state') {
                    console.log('Received initial state:', message.jobs);
                } else if (message.type === 'pipeline_error' && message.data) {
                     console.error('Pipeline error received via WebSocket:', message.data.error);
                     showErrorMessage(`Pipeline Error: ${message.data.error}`);
                     resetUIStateAfterPipelineEnd();
                } else {
                    console.warn('Received unknown message type:', message.type);
                }
            } catch (error) {
                console.error('Error processing WebSocket message:', error, 'Data:', event.data);
                if (!progressContainer.classList.contains('d-none')) return;
                showErrorMessage('Error processing update from server.');
            }
        };

        socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            if (!progressContainer.classList.contains('d-none') && progressBar.getAttribute('aria-valuenow') !== '100') {
                 resetUIStateAfterPipelineEnd();
            }
            showErrorMessage('WebSocket connection error. Real-time updates unavailable.');
        };

        socket.onclose = (event) => {
            console.log('WebSocket connection closed:', event.code, event.reason);
            if (event.code !== 1000 && progressBar.getAttribute('aria-valuenow') !== '100') {
                 showErrorMessage('WebSocket connection lost. Real-time updates stopped.');
                 if (!progressContainer.classList.contains('d-none')) {
                    resetUIStateAfterPipelineEnd();
                 }
            }
            socket = null;
        };
    }

    function handleStatusUpdate({ percentage, description, status }) {
         if (progressContainer.classList.contains('d-none')) {
             console.warn("Received status update while progress bar was hidden. Showing progress bar.");
             showProgressBar();
             setMainButtonState(true);
         }

        updateProgressBar(percentage, description);

        if (status === 'COMPLETE' || percentage >= 100) {
            console.log('Pipeline completed via WebSocket message.');
            fetchJobs().then(() => {
                showSuccessMessage();
                resetUIStateAfterPipelineEnd();
            }).catch(error => {
                console.error('Failed to fetch jobs after pipeline completion:', error);
                showErrorMessage('Pipeline finished, but failed to load updated jobs.');
                resetUIStateAfterPipelineEnd();
            });
        } else if (status === 'FAILED') {
            console.error('Pipeline failed via WebSocket message:', description);
            showErrorMessage(`Pipeline Failed: ${description || 'Unknown error'}`);
            resetUIStateAfterPipelineEnd();
        }
    }

    async function fetchJobs() {
        console.log('Fetching jobs data...');
        setJobsLoading(true);
        clearMainAlerts();

        try {
            const response = await fetch(`${apiBaseUrl}/data`);

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
                    <div class="alert alert-danger" role="alert">
                        ${error.message || 'Failed to load jobs. Please try again later.'}
                    </div>
                </div>
            `;
        } finally {
            setJobsLoading(false);
        }
    }

    function displayJobs(jobs) {
        if (!Array.isArray(jobs)) {
             console.error("Received non-array data for jobs:", jobs);
             jobsContainer.innerHTML = `
                 <div class="col-12 text-center">
                     <div class="alert alert-warning" role="alert">
                         Received invalid job data format from server.
                     </div>
                 </div>
             `;
             return;
         }

        if (jobs.length === 0) {
            jobsContainer.innerHTML = `
                <div class="col-12 text-center">
                    <div class="alert alert-info" role="alert">
                        No jobs found. Click "Refresh Jobs" to configure and run the pipeline.
                    </div>
                </div>
            `;
            return;
        }

        jobs.sort((a, b) => {
            const dateA = a.date_posted ? new Date(a.date_posted) : 0;
            const dateB = b.date_posted ? new Date(b.date_posted) : 0;
            return dateB - dateA;
        });


        let jobsHTML = '';
        jobs.forEach(job => {
            const postedDate = job.date_posted ? new Date(job.date_posted).toLocaleDateString() : 'N/A';
            const scrapedDate = job.date_scraped ? new Date(job.date_scraped).toLocaleString() : 'N/A';

            const skillsHTML = (Array.isArray(job.skills) ? job.skills : [])
                .map(skillObj => `<span class="badge bg-secondary skill-badge">${skillObj.skill || 'Unknown Skill'}</span>`)
                .join('');

            let statusBadgeClass = 'bg-secondary';
            const progress = job.progress || 'unknown';
            if (progress === 'active') statusBadgeClass = 'bg-success';
            else if (progress === 'pending') statusBadgeClass = 'bg-warning text-dark';
            else if (progress === 'rejected') statusBadgeClass = 'bg-danger';

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
                                <span class="badge ${statusBadgeClass}">${progress}</span>
                            </div>
                            <div class="mb-2 skills-container">
                                ${skillsHTML || '<span class="text-muted small">No skills listed</span>'}
                            </div>
                            <p class="job-date small text-muted mb-1">Posted: ${postedDate}</p>
                            <p class="job-date small text-muted mb-3">Scraped: ${scrapedDate}</p>
                            <div class="mt-auto">
                                <a href="${job.job_url || '#'}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-primary job-url ${!job.job_url ? 'disabled' : ''}">View Original Job</a>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        jobsContainer.innerHTML = jobsHTML;
    }


    function resetModalForm() {
        console.log('Resetting modal form.');
        pipelineConfigForm.reset();
        jobTitlesList.innerHTML = '';
        updateJobTitlesHidden();
        clearModalAlert();
        setModalSubmitButtonState(false);
        pipelineConfigForm.classList.remove('was-validated');
    }

    function showModalAlert(message, type = 'danger') {
        modalAlertContainer.innerHTML = `
            <div class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        `;
    }

    function clearModalAlert() {
        modalAlertContainer.innerHTML = '';
    }

    async function handleFormSubmit(event) {
        event.preventDefault();
        event.stopPropagation();

        clearModalAlert();
        pipelineConfigForm.classList.add('was-validated');

        const jobTitles = getAddedJobTitles();
        jobTitlesHidden.value = jobTitles.length > 0 ? 'filled' : '';

        if (!pipelineConfigForm.checkValidity()) {
            if (jobTitles.length === 0) {
                 showModalAlert('Please add at least one job title.', 'warning');
                 newJobTitleInput.focus();
             } else {
                showModalAlert('Please fill in all required fields correctly.', 'warning');
            }
            return;
        }

        setModalSubmitButtonState(true);

        const formData = {
            job_titles: jobTitles.join(','),
            location: locationInput.value.trim(),
            time_filter: document.querySelector('input[name="timeFilter"]:checked').value,
            max_jobs: maxJobsInput.value,
        };

        console.log('Triggering pipeline with data:', formData);

        try {
            if (!socket || socket.readyState !== WebSocket.OPEN) {
                console.log('WebSocket not open. Attempting to connect/reconnect...');
                connectWebSocket();
                await new Promise(resolve => setTimeout(resolve, 1500));
                if (!socket || socket.readyState !== WebSocket.OPEN) {
                   throw new Error('WebSocket connection failed. Cannot trigger pipeline.');
                }
                 console.log('WebSocket reconnected successfully.');
            }

            const response = await fetch(`${apiBaseUrl}/trigger-job-pipeline`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify(formData)
            });

             const responseBody = await response.text();

            if (!response.ok) {
                let errorMsg = `Pipeline trigger failed: ${response.status} ${response.statusText}.`;
                try {
                    const errorData = JSON.parse(responseBody);
                    errorMsg += ` ${errorData.detail || ''}`;
                } catch (e) {
                     errorMsg += ` ${responseBody}`;
                }
                 throw new Error(errorMsg);
            }

            console.log('Pipeline trigger request sent successfully.', responseBody);
            pipelineConfigModal.hide();
            showProgressBar();
            setMainButtonState(true);
            clearMainAlerts();
            updateProgressBar(0, 'Pipeline initiated...');

        } catch (error) {
            console.error('Error triggering pipeline:', error);
            showModalAlert(error.message || 'An unexpected error occurred while triggering the pipeline.');
            setModalSubmitButtonState(false);
        }
    }

    function setJobsLoading(isLoading) {
        if (isLoading) {
            jobsContainer.innerHTML = `
                <div class="col-12 text-center py-5">
                    <div class="spinner-border" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2">Loading jobs...</p>
                </div>
            `;
        }
    }

    function showProgressBar() {
         progressContainer.classList.remove('d-none');
         updateProgressBar(0, 'Initializing...');
    }

    function updateProgressBar(percentage, description) {
        const clampedPercentage = Math.max(0, Math.min(100, percentage));
        progressBar.style.width = `${clampedPercentage}%`;
        progressBar.textContent = `${clampedPercentage.toFixed(0)}%`;
        progressBar.setAttribute('aria-valuenow', clampedPercentage);
        progressDescription.textContent = description || '';

        progressBar.classList.remove('bg-success', 'progress-bar-animated', 'progress-bar-striped');

        if (clampedPercentage < 100) {
            progressBar.classList.add('progress-bar-animated', 'progress-bar-striped');
        } else {
            progressBar.classList.add('bg-success');
        }
    }

    function setMainButtonState(isLoading) {
         openPipelineModalBtn.disabled = isLoading;
         if (isLoading) {
             runPipelineBtn.textContent = 'Pipeline Running';
             loadingSpinner.classList.remove('d-none');
         } else {
             runPipelineBtn.textContent = 'Refresh Jobs';
             loadingSpinner.classList.add('d-none');
         }
     }

     function setModalSubmitButtonState(isSubmitting) {
         submitPipelineConfigBtn.disabled = isSubmitting;
         if (isSubmitting) {
             submitBtnText.textContent = 'Submitting...';
             submitSpinner.classList.remove('d-none');
         } else {
             submitBtnText.textContent = 'Run Pipeline';
             submitSpinner.classList.add('d-none');
         }
     }

    function resetUIStateAfterPipelineEnd() {
        console.log('Resetting main UI state after pipeline end.');
        setMainButtonState(false);
        progressContainer.classList.add('d-none');
        updateProgressBar(0, '');
    }

    function showSuccessMessage() {
        clearMainAlerts();
        pipelineSuccess.classList.remove('d-none');
        setTimeout(() => {
           if (pipelineSuccess) pipelineSuccess.classList.add('d-none');
        }, 5000);
    }

    function showErrorMessage(message) {
        clearMainAlerts();
        pipelineError.textContent = message;
        pipelineError.classList.remove('d-none');
     }

    function clearMainAlerts() {
         pipelineSuccess.classList.add('d-none');
         pipelineError.classList.add('d-none');
         pipelineError.textContent = '';
     }

     // Initialization
    fetchJobs();
    connectWebSocket();
    addJobTitleBtn.addEventListener('click', handleAddJobTitle);
    newJobTitleInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            handleAddJobTitle();
        }
    });
    pipelineConfigForm.addEventListener('submit', handleFormSubmit);
    pipelineConfigModalEl.addEventListener('hidden.bs.modal', resetModalForm);

    const defaultJobTitle = "Data engineer";
    console.log(`Adding default job title: ${defaultJobTitle}`);
    addJobTitleToList(defaultJobTitle);

});