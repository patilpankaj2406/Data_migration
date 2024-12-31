# Data Migration Project: MongoDB & Azure to Wasabi

## Overview

This project focuses on migrating JSON data from MongoDB and blob data from Azure Storage to **Wasabi**. The goal of this migration is to **reduce storage costs** while maintaining efficiency and data integrity. The migration process is automated using a Python application powered by Azure Functions, triggered on a time schedule.

---

## Features

- **Automated Migration**: Scheduled execution via Azure Time Trigger.
- **Cost-Effective**: Moves data to Wasabi for reduced storage costs.
- **Efficient Data Handling**: Concurrent processing for JSON and blob data.
- **Reliable**: Ensures data integrity with error handling and logging.

---

## Architecture

1. **Source**:
   - JSON data from MongoDB.
   - Blob data from Azure Storage.

2. **Destination**:
   - All data is migrated to Wasabi.

3. **Automation**:
   - Azure Functions with a time-trigger schedule for periodic execution.

4. **Language**:
   - Python-based solution leveraging libraries such as `pymongo`, `azure-storage-blob`, and `boto3`.

---

## Prerequisites

### Tools and Services
- Python 3.8+
- Azure Function App
- MongoDB (as the source database)
- Azure Storage (as the blob storage service)
- Wasabi (as the destination storage)

### Libraries
Install the required dependencies using:
```bash
pip install -r requirements.txt
