import requests
import re
import os
import configparser
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

def setup_logging():
    """Configure and return logger"""
    logging.basicConfig(
        filename='update_log.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'
    )
    return logging.getLogger()

#--------------------------------------------------------------------------------------------------------------------------

def set_working_directory():
    """Set the working directory for the script"""
    # os.chdir("/")
    # working_dir = "/opt/splunk/etc/apps/CousinsProperties/local"    
    # os.chdir(working_dir)
    os.getcwd()
    pass

#--------------------------------------------------------------------------------------------------------------------------

def read_config(config_file):
    """Read and return the config parser object"""
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        return config
    except Exception as e:
        logger.error(f"Error reading config file {config_file}: {e}")
        raise

#--------------------------------------------------------------------------------------------------------------------------

def get_credentials_from_config(config, section_name):
    """Get credentials from config object"""
    try:
        if not config.has_section(section_name):
            logger.warning(f"Missing section: {section_name}")
            return None
        
        auth_user = config.get(section_name, 'auth_user')
        auth_password = config.get(section_name, 'auth_password')
        return auth_user, auth_password
    except configparser.NoOptionError:
        logger.warning(f"Missing credentials for section: {section_name}")
        return None

#--------------------------------------------------------------------------------------------------------------------------

def update_endpoint(config, config_file, section, new_endpoint):
    """Update endpoint in config file"""
    try:
        config.set(section, 'endpoint', new_endpoint)
        logger.info(f"Updated endpoint for section: {section} to {new_endpoint}")

        with open(config_file, 'w') as configfile:
            config.write(configfile)
    except Exception as e:
        logger.error(f"Error updating endpoint in {config_file}, section {section}: {e}")
        raise

#--------------------------------------------------------------------------------------------------------------------------

def get_score(property_id, year, month, section_name, config, config_file, max_years_to_check=1):
    """
    Get Energy Star score for a property
    Returns a tuple of (success, year, month) where success is a boolean
    """
    # Skip weather endpoints or other non-property sections
    if section_name == "rest://WeatherTest3":
        logger.info(f"Skipping section {section_name}.")
        return False, None, None
        
    years_checked = 0
    
    logger.info(f"Checking score - Property ID: {property_id}")

    while years_checked <= max_years_to_check:
        url = f"https://portfoliomanager.energystar.gov/ws/property/{property_id}/metrics?year={year}&month={month}"
        headers = {"PM-Metrics": "score"}
        
        credentials = get_credentials_from_config(config, section_name)
        if credentials is None:
            logger.warning(f"Skipping section {section_name} due to missing credentials.")
            return False, None, None
            
        auth_user, auth_password = credentials
        auth = HTTPBasicAuth(auth_user, auth_password)
        try:
            response = requests.get(url, auth=auth, headers=headers)
            response.raise_for_status()  # Raise exception for non-200 status codes
            
            soup = BeautifulSoup(response.text, "xml")
            score_text = soup.text.strip()
            
            if not score_text:
                # Move to previous month and continue
                logger.info(f"No score found for Property ID: {property_id}, Year: {year}, Month: {month}. Trying previous month.")
                if month == 1:
                    year -= 1
                    month = 12
                    years_checked += 1
                else:
                    month -= 1
                
                if years_checked > max_years_to_check:
                    logger.info(f"No score found for Property ID: {property_id} after checking {max_years_to_check} previous years.")
                    return False, None, None
            else:
                # Score found, update endpoint
                new_endpoint = f"https://portfoliomanager.energystar.gov/ws/property/{property_id}/metrics?year={year}&month={month}"
                logger.info(f"Score found for Property ID: {property_id}, Year: {year}, Month: {month}, Score: {score_text}")
                
                # Change polling interval to "0 0 5 31 2 ?" first
                config.set(section_name, "polling_interval", "999999")
                
                # Update endpoint
                config.set(section_name, 'endpoint', new_endpoint)
                config.set(section_name, "polling_interval", "0 0 5 31 2 ?")
                
                # Write all changes to file
                with open(config_file, 'w') as configfile:
                    config.write(configfile)
                
                logger.info(f"Updated endpoint for section: {section_name} to {new_endpoint}")
                logger.info("--------------------------------------------------------------------------------------------------------------------------")
                return True, year, month
                
        except requests.exceptions.RequestException as e:
            # Handle HTTP error
            logger.warning(f"HTTP error for Property ID {property_id}, Year: {year}, Month: {month}: {e}. Trying previous month.")
            if month == 1:
                year -= 1
                month = 12
                years_checked += 1
            else:
                month -= 1
                
            if years_checked > max_years_to_check:
                logger.info(f"No valid response found for Property ID: {property_id} after checking {max_years_to_check} previous years.")
                return False, None, None
                
        except Exception as e:
            # Handle other errors
            logger.warning(f"Unexpected error for Property ID {property_id}, Year: {year}, Month: {month}: {e}. Trying previous month.")
            if month == 1:
                year -= 1
                month = 12
                years_checked += 1
            else:
                month -= 1
                
            if years_checked > max_years_to_check:
                logger.info(f"No valid response found for Property ID: {property_id} after checking {max_years_to_check} previous years.")
                return False, None, None
    
    # Should only reach here if max_years_to_check is 0 and no score was found in the current month
    return False, None, None

#--------------------------------------------------------------------------------------------------------------------------

def extract_property_id(endpoint_value):
    """Extract property ID from endpoint URL"""
    pattern = r'/property/(\d+)/metrics'
    matching = re.search(pattern, endpoint_value)
    if matching:
        return matching.group(1)
    return None

#--------------------------------------------------------------------------------------------------------------------------

def main():
    """Main function to update all endpoints"""
    config_file = "inputs.conf"
    
    # Setup
    global logger
    logger = setup_logging()
    set_working_directory()
    
    try:
        # Read configuration
        config = read_config(config_file)
        sections = config.sections()
        
        if not sections:
            logger.warning(f"No sections found in {config_file}")
            return
            
        # Set all polling intervals to "999999" first
        modified = False
        for section in sections:
            if section != "rest://WeatherTest3" and config.has_option(section, 'polling_interval'):
                current_interval = config.get(section, 'polling_interval')
                if current_interval != "999999":
                    config.set(section, 'polling_interval', "999999")
                    modified = True
        
        if modified:
            with open(config_file, 'w') as configfile:
                config.write(configfile)
            
            # Reload config after writing
            config = read_config(config_file)
            
        # Current date
        current_year = datetime.today().year
        current_month = datetime.today().month
        
        # Process each section
        updated_count = 0
        skipped_count = 0
        error_count = 0
        
        for section in sections:
            # Skip the problematic weather section
            if section == "rest://WeatherTest3":
                logger.info(f"Skipping section {section} as it's a weather endpoint.")
                skipped_count += 1
                continue
                
            if not config.has_option(section, 'endpoint'):
                logger.warning(f"Section {section} has no endpoint option. Skipping.")
                skipped_count += 1
                continue
                
            endpoint = config.get(section, 'endpoint')
            property_id = extract_property_id(endpoint)
            
            if not property_id:
                logger.warning(f"Could not extract property ID from endpoint in section {section}. Skipping.")
                skipped_count += 1
                continue
                
            success, _, _ = get_score(property_id, current_year, current_month, section, config, config_file)
            if success:
                updated_count += 1
            elif not success:
                error_count += 1
            else:
                skipped_count += 1
                
        logger.info(f"Processing complete. Updated: {updated_count}, Error: {error_count} Skipped: {skipped_count}")
        logger.info("--------------------------------------------------------------------------------------------------------------------------")
        
        print(f"Processing complete. Updated: {updated_count}, Error: {error_count} Skipped: {skipped_count}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        print("Error occured, Please check log file.")
        error_count += 1
        print(f"Error: {e}")

#--------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()