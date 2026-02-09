# Browser Upload Interface for Global Sky Model

## What Was Added

A simple browser-based interface to upload sky survey CSV files using the existing `/upload-sky-survey-batch` API endpoint.

### Files Added

1. **`src/ska_sdp_global_sky_model/api/app/static/upload.html`**
   - Simple, responsive HTML upload interface
   - Drag-and-drop file selection
   - Multiple CSV file uploads
   - Real-time upload status monitoring
   - Works with existing API endpoints

2. **Modified `src/ska_sdp_global_sky_model/api/app/main.py`**
   - Added `Path` import from pathlib
   - Added `FileResponse` import from fastapi.responses
   - Added `GET /` endpoint to serve the HTML interface

## Usage

1. Start the API server
2. Navigate to `http://localhost:8000/` in your browser
3. Select catalog type (GLEAM, RACS, RCAL, or GENERIC)
4. Drag and drop CSV files or click to browse
5. Click "Upload Files"
6. Monitor the upload status in real-time

The interface uses the existing upload endpoint and doesn't require any backend changes beyond serving the HTML file.

## API Endpoints Used

- **`POST /upload-sky-survey-batch`** - Existing endpoint for file uploads
- **`GET /upload-sky-survey-status/{upload_id}`** - Existing endpoint for status checks
- **`GET /`** - New endpoint to serve the HTML interface

## No Validation Added

As requested, no additional validation logic was added. The interface simply provides browser access to the existing upload functionality that was already implemented.
