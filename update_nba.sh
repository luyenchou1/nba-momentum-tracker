#!/bin/bash
# Daily NBA data update script
# Add to crontab: 0 7 * * * /path/to/update_nba.sh

cd "$(dirname "$0")"
echo "Starting NBA data update at $(date)"

python3 scrape_nba.py
if [ $? -ne 0 ]; then
    echo "ERROR: Scraper failed at $(date)" >> update.log
    exit 1
fi

python3 export_chart_data.py
if [ $? -ne 0 ]; then
    echo "ERROR: Export failed at $(date)" >> update.log
    exit 1
fi

echo "NBA data updated successfully at $(date)" >> update.log
