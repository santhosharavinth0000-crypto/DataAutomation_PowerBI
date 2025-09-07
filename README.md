# DataAutomation_PowerBI
Automated data extraction from shared folders, transformation using Python, and daily refreshed Power BI dashboard with email alerts.v
# Data Automation & Power BI Dashboard

## Project Overview
This project automates the workflow of daily reporting:
- Extracting Excel reports from a shared folder using Python.
- Cleaning, transforming, and combining the data.
- Feeding the processed dataset into a **Power BI dashboard**.
- Scheduling a **daily refresh** and automatically sending an **email notification** to management.

This eliminates manual work, ensures data consistency, and speeds up reporting.

--

## Tools & Technologies
- **Python** → pandas, openpyxl, smtplib, schedule  
- **Power BI** → dashboard design, DAX measures, auto refresh  
- **Windows Task Scheduler** → automation  
- **Email (SMTP)** → daily notifications  

---
## 📊 Dashboard Preview
![Dashboard Screenshot](screenshot.png)

git add screenshot.png README.md
git commit -m "Added Power BI dashboard screenshot"
git push


## Project Structure
```plaintext
📂 DataAutomation_PowerBI
 ├── 📂 python_scripts
 │     ├── extract_data.py        # Extracts Excel files from shared folder
 │     ├── transform_data.py      # Cleans & merges reports
 │     └── requirements.txt       # Python dependencies
 ├── 📂 powerbi_dashboard
 │     ├── dashboard.pbix         # Power BI dashboard
 │     └── dataset_sample.csv     # Sample dataset (dummy data for demo)
 ├── 📂 automation
 │     ├── schedule_task.md       # How to set auto-refresh
 │     └── email_alerts.py        # Script to send summary email
 ├── screenshot.png               # Dashboard preview
 └── README.md
