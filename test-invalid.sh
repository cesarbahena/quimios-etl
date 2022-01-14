#!/bin/bash
echo "Setting up invalid date test..."
sed -i 's/01\/02\/2023 11:00:00 AM/INVALID_DATE/' consulta.html
echo "Running invalid data test..."
lims-scraper
echo "Restoring valid data..."
sed -i 's/INVALID_DATE/01\/02\/2023 11:00:00 AM/' consulta.html
echo "Test completed, HTML restored."