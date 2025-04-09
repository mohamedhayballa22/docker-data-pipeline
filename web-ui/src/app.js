document.addEventListener('DOMContentLoaded', () => {
    const jobsContainer = document.getElementById('jobsContainer');
    const refreshBtn = document.getElementById('refreshBtn');
    const refreshBtnText = document.getElementById('refreshBtnText');
    const loadingSpinner = document.getElementById('loadingSpinner');
    const pipelineAlert = document.getElementById('pipelineAlert');
    const pipelineSuccess = document.getElementById('pipelineSuccess');
    const pipelineError = document.getElementById('pipelineError');

    // Load jobs on page load
    fetchJobs();

    // Set up refresh button
    refreshBtn.addEventListener('click', triggerPipeline);

    async function fetchJobs() {
        try {
            const response = await fetch('http://127.0.0.1:8000/data');
            
            if (!response.ok) {
                throw new Error('Failed to fetch jobs');
            }
            
            const jobs = await response.json();
            displayJobs(jobs);
        } catch (error) {
            console.error('Error fetching jobs:', error);
            jobsContainer.innerHTML = `
                <div class="col-12 text-center">
                    <div class="alert alert-danger">
                        Failed to load jobs. Please try again later.
                    </div>
                </div>
            `;
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

        // Sort jobs by date_posted (newest first)
        jobs.sort((a, b) => new Date(b.date_posted) - new Date(a.date_posted));

        let jobsHTML = '';
        
        jobs.forEach(job => {
            const postedDate = new Date(job.date_posted).toLocaleDateString();
            const scrapedDate = new Date(job.date_scraped).toLocaleString();
            
            // Create skills badges HTML
            const skillsHTML = job.skills.map(skillObj => 
                `<span class="badge bg-secondary skill-badge">${skillObj.skill}</span>`
            ).join('');

            // Determine status badge color
            let statusBadgeClass = 'bg-secondary';
            if (job.progress === 'active') {
                statusBadgeClass = 'bg-success';
            } else if (job.progress === 'pending') {
                statusBadgeClass = 'bg-warning';
            } else if (job.progress === 'rejected') {
                statusBadgeClass = 'bg-danger';
            }

            jobsHTML += `
                <div class="col-md-6 col-lg-4 mb-4">
                    <div class="card job-card">
                        <div class="card-body">
                            <h5 class="card-title">${job.title}</h5>
                            <h6 class="card-subtitle mb-2 text-muted">${job.company_name}</h6>
                            <p class="job-location">
                                <i class="bi bi-geo-alt"></i> ${job.location}
                            </p>
                            <div class="mb-3">
                                <span class="badge ${statusBadgeClass}">${job.progress}</span>
                            </div>
                            <div class="mb-3">
                                ${skillsHTML}
                            </div>
                            <p class="job-date">Posted: ${postedDate}</p>
                            <p class="job-date">Scraped: ${scrapedDate}</p>
                            <a href="${job.job_url}" target="_blank" class="btn btn-sm btn-outline-primary job-url">View Job</a>
                        </div>
                    </div>
                </div>
            `;
        });
        
        jobsContainer.innerHTML = jobsHTML;
    }

    async function triggerPipeline() {
        // Update UI to show pipeline is running
        refreshBtn.disabled = true;
        refreshBtnText.textContent = 'Pipeline Running';
        loadingSpinner.classList.remove('d-none');
        pipelineAlert.classList.remove('d-none');
        pipelineSuccess.classList.add('d-none');
        pipelineError.classList.add('d-none');

        try {
            const response = await fetch('http://127.0.0.1:8000/trigger-job-pipeline', {
                method: 'POST'
            });

            if (!response.ok) {
                throw new Error('Pipeline trigger failed');
            }

            // Set a polling interval to check if pipeline has completed
            const pollingInterval = setInterval(async () => {
                try {
                    // Make a request to the data endpoint to see if new data is available
                    const checkResponse = await fetch('http://127.0.0.1:8000/data');
                    
                    if (checkResponse.ok) {
                        // Update the jobs display
                        const jobs = await checkResponse.json();
                        displayJobs(jobs);
                        
                        // Show success message
                        pipelineAlert.classList.add('d-none');
                        pipelineSuccess.classList.remove('d-none');
                        
                        // Reset button state
                        refreshBtn.disabled = false;
                        refreshBtnText.textContent = 'Refresh Jobs';
                        loadingSpinner.classList.add('d-none');
                        
                        // Clear the polling interval
                        clearInterval(pollingInterval);
                        
                        // Hide success message after 5 seconds
                        setTimeout(() => {
                            pipelineSuccess.classList.add('d-none');
                        }, 5000);
                    }
                } catch (error) {
                    console.error('Error checking pipeline status:', error);
                }
            }, 5000); // Check every 5 seconds
            
            // Set a timeout to stop polling after 2 minutes
            setTimeout(() => {
                clearInterval(pollingInterval);
                if (refreshBtn.disabled) {
                    // If button is still disabled, pipeline has taken too long
                    refreshBtn.disabled = false;
                    refreshBtnText.textContent = 'Refresh Jobs';
                    loadingSpinner.classList.add('d-none');
                    pipelineAlert.classList.add('d-none');
                    
                    // Show a timeout message
                    pipelineError.textContent = "Pipeline is taking longer than expected. You can check back later.";
                    pipelineError.classList.remove('d-none');
                }
            }, 120000); // 2 minutes timeout

        } catch (error) {
            console.error('Error triggering pipeline:', error);
            
            // Reset UI and show error
            refreshBtn.disabled = false;
            refreshBtnText.textContent = 'Refresh Jobs';
            loadingSpinner.classList.add('d-none');
            pipelineAlert.classList.add('d-none');
            
            pipelineError.textContent = "Failed to trigger the pipeline. Please try again.";
            pipelineError.classList.remove('d-none');
        }
    }
});