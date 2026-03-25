# Hass Petroleum Logistics Monitor

A modern Flask dashboard for **Hass Petroleum Mombasa** to monitor fleet logistics, upload reports, and review outbound/inbound/mileage tables with mandatory formula columns.

## Features

- Secure login screen before dashboard access.
- Branded dashboard with Hass logo support (`img/hass_logo.png`).
- Live monitor table for Hass fleet units from `get_units.json`.
- Report center with CSV/Excel upload.
- Dedicated tabs for Live Monitor, Reports, Outbound, Inbound, and Mileage.
- Mandatory outbound/inbound formula columns carried from notebook logic.

## Data Sources

- `get_units.json` is used as the live vehicle source.
- Notebook logic inspiration comes from `hass_clean.ipynb` (vehicle summary, route tabs, mileage views).

## Local Installation

1. Open a terminal in this folder.
2. Create and activate a virtual environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run the app locally:

```bash
python app.py
```

## Login Details

Default credentials:

- **Username:** `admin`
- **Password:** `Hass@2026`

You can override them using environment variables:

- `HASS_APP_USERNAME`
- `HASS_APP_PASSWORD`

Example:

```bash
set HASS_APP_USERNAME=operations
set HASS_APP_PASSWORD=StrongPassword123!
python app.py
```

## Vercel Deployment

This project can be deployed to Vercel using Python runtime.

1. Push the project to GitHub.
2. Import the repo in Vercel.
3. Set environment variables:
   - `HASS_APP_USERNAME`
   - `HASS_APP_PASSWORD`
   - `APP_SECRET_KEY`
4. Deploy.

## Notes

- The home page content references Hass Group public company information from their official website: [hasspetroleum.com](https://hasspetroleum.com/).
- Uploaded report files are processed in-memory on the Flask server.

## Mandatory Report Formula Columns

The app now enforces these mandatory computed columns in report tables:

- **Outbound**
  - `Average Time to Destination`
  - `Transit to Malaba`
  - `Transit to Nimule`
  - `Transit to Destination`
  - `Depot Time Spent`
  - `Malaba Time Spent`
  - `Nimule Time Spent`
  - `Juba Time Spent`
- **Inbound**
  - `Average Time to Depot`
  - `Transit to Nimule`
  - `Transit to Malaba`
  - `Transit to Depot`
  - `Juba Time Spent`
  - `Nimule Time Spent`
  - `Malaba Time Spent`
  - `Depot Time Spent`

