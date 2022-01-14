#!/usr/bin/env python3
"""
End-to-end test runner that uses SQLite instead of PostgreSQL
"""
import os
import sys
sys.path.insert(0, 'src')
from lims_etl.scraper import LIMSConfig, Scraper, prepare_sample_data
from lims_etl.database import DatabaseManager

# Override environment to use SQLite
os.environ['DB_HOST'] = 'sqlite'
os.environ['DB_NAME'] = '///test_e2e_full.db'

class TestLIMSConfig(LIMSConfig):
    """Test config that uses SQLite instead of PostgreSQL"""
    def __init__(self):
        # Copy parent attributes without calling __init__
        import json
        
        # Basic config
        self.username = 'demo_user'
        self.password = 'demo_pass'
        self.max_fails = 30
        self.sleep_time = 2
        self.test_clients = [101, 102]
        
        from datetime import datetime, timedelta
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2020, 1, 1)
        
        # Chrome options
        from selenium import webdriver
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        
        # Load selectors
        with open('selectors.json', 'r') as f:
            self.selectors = json.load(f)
            
        # Use SQLite instead of PostgreSQL
        self.db_manager = DatabaseManager('sqlite:///test_e2e_full.db')
        self.db_manager.create_tables()

def main():
    print("Starting end-to-end test...")
    config = TestLIMSConfig()
    
    print("Testing single client scraper...")
    try:
        with Scraper(101, config) as scraper:
            print(f"Chrome driver started: {scraper.driver is not None}")
            
            # Test login
            login_success = scraper.login()
            print(f"Login successful: {login_success}")
            
            # Test navigation
            nav_success = scraper.navigate_to_client()
            print(f"Navigation successful: {nav_success}")
            
            # Test page scanning
            samples_found = scraper.scan_page()
            print(f"Samples found on page: {samples_found}")
            
            # Test data conversion and saving
            if scraper.data and any(scraper.data.values()):
                sample_records = prepare_sample_data(scraper.data)
                saved_count = config.db_manager.save_samples(sample_records)
                print(f"Samples saved to database: {saved_count}")
                
                total_in_db = config.db_manager.get_sample_count()
                print(f"Total samples in database: {total_in_db}")
            else:
                print("No data collected")
                
        print("End-to-end test completed successfully!")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()