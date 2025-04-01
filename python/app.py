"""
Snowflake to Ellie Transfer Tool - Main Application

This Streamlit application provides a user interface for transferring database schema
information from Snowflake to Ellie.ai. It allows users to select schemas, customize
model creation options, and manage connection settings.

Author: Your Name
License: MIT
"""

import streamlit as st
import yaml
import os
import random
import json
import re
from pathlib import Path
import snowflake.connector
from ellie import (
    snowflake_connect,
    snowflake_export,
    ellie_connect,
    ellie_model_import
)

# Set page config
st.set_page_config(
    page_title="Snowflake to Ellie Transfer",
    page_icon="❄️",
    layout="wide"
)

# Initialize session state
if 'connected_to_snowflake' not in st.session_state:
    st.session_state.connected_to_snowflake = False
if 'connected_to_ellie' not in st.session_state:
    st.session_state.connected_to_ellie = False

def load_config():
    """
    Load configuration from the default config file.
    
    Returns:
        dict: Configuration dictionary with Snowflake and Ellie settings
    """
    config_path = Path("config/default_config.yaml")
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {
        'snowflake': {
            'account': '', 'user': '', 'password': '',
            'warehouse': 'COMPUTE_WH', 'database': '', 'role': '',
            'connection_mode': 'standard', 'custom_url': ''
        },
        'ellie': {
            'organization': 'your-organization.ellie.ai',
            'token': '', 'api_version': 'v1',
            'folder_id': ''
        }
    }

def save_config(config):
    """
    Save configuration to the default config file.
    
    Args:
        config (dict): Configuration dictionary to save
    """
    os.makedirs('config', exist_ok=True)
    with open('config/default_config.yaml', 'w') as f:
        yaml.dump(config, f)

def extract_account_from_url(url):
    """
    Extract the account identifier from a Snowflake URL.
    
    Args:
        url (str): Snowflake URL (e.g., https://nn73358.eu-north-1.aws.snowflakecomputing.com)
        
    Returns:
        str: Account identifier (e.g., nn73358.eu-north-1.aws)
    """
    # First, check if it's a URL format
    url_pattern = r'https?://([^\.]+\.[^\.]+\.[^\.]+)\.snowflakecomputing\.com'
    match = re.match(url_pattern, url)
    if match:
        return match.group(1)
    
    # If not a URL format, return as is (could be just the account ID)
    return url

def connect_to_snowflake(settings):
    """
    Establish a connection to Snowflake using the provided settings.
    
    Args:
        settings (dict): Snowflake connection settings
        
    Returns:
        snowflake.connector.connection.SnowflakeConnection or None: Snowflake connection object or None if connection fails
    """
    try:
        # If using a custom URL for privatelink, use that instead of the account
        if settings.get('connection_mode') == 'privatelink' and settings.get('custom_url'):
            # For privatelink connections, use the custom URL
            conn = snowflake.connector.connect(
                account=settings['custom_url'],
                user=settings['user'],
                password=settings['password'],
                warehouse=settings['warehouse'],
                database=settings['database'],
                role=settings['role']
            )
        else:
            # For standard connections, use the account (which might be a full URL)
            account_param = extract_account_from_url(settings['account'])
            conn = snowflake.connector.connect(
                account=account_param,
                user=settings['user'],
                password=settings['password'],
                warehouse=settings['warehouse'],
                database=settings['database'],
                role=settings['role']
            )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

# Load existing config
config = load_config()

# Main UI
st.title("❄️ Snowflake to Ellie Transfer")

# Sidebar for connections
with st.sidebar:
    st.header("Settings")
    
    # Snowflake Settings
    st.subheader("Snowflake Configuration")
    snowflake_config = config['snowflake']
    
    # Connection mode selector
    connection_mode = st.radio(
        "Connection Mode",
        ["Standard", "PrivateLink"],
        index=0 if snowflake_config.get('connection_mode', 'standard') == 'standard' else 1,
        help="Standard: Regular Snowflake account. PrivateLink: Use privatelink with VPN/private network."
    )
    snowflake_config['connection_mode'] = connection_mode.lower()
    
    # Show appropriate fields based on connection mode
    if connection_mode == "Standard":
        snowflake_config['account'] = st.text_input(
            "Account URL or ID", 
            snowflake_config.get('account', ''),
            help="Your Snowflake account identifier or full URL (e.g., 'xy12345.eu-west-1.aws' or 'https://xy12345.eu-west-1.aws.snowflakecomputing.com')"
        )
    else:
        snowflake_config['custom_url'] = st.text_input(
            "PrivateLink URL", 
            snowflake_config.get('custom_url', ''),
            help="Your Snowflake privatelink URL (e.g., 'youraccount-privatelink.snowflakecomputing.com')"
        )
    
    # Common Snowflake settings
    snowflake_config['user'] = st.text_input("User", snowflake_config.get('user', ''))
    snowflake_config['password'] = st.text_input("Password", snowflake_config.get('password', ''), type="password")
    snowflake_config['warehouse'] = st.text_input("Warehouse", snowflake_config.get('warehouse', 'COMPUTE_WH'))
    snowflake_config['database'] = st.text_input("Database", snowflake_config.get('database', ''))
    snowflake_config['role'] = st.text_input("Role", snowflake_config.get('role', ''))

    # Ellie Settings
    st.subheader("Ellie Configuration")
    ellie_config = config['ellie']
    org_value = ellie_config.get('organization', '')
    ellie_config['organization'] = st.text_input("Organization", org_value)
    ellie_config['token'] = st.text_input("Token", ellie_config.get('token', ''), type="password")
    ellie_config['api_version'] = st.text_input("API Version", ellie_config.get('api_version', 'v1'))
    ellie_config['folder_id'] = st.text_input("Default Folder ID", ellie_config.get('folder_id', ''))

    if st.button("Save Settings"):
        # Ensure organization URL has https:// prefix
        if ellie_config['organization'] and not ellie_config['organization'].startswith(('http://', 'https://')):
            ellie_config['organization'] = 'https://' + ellie_config['organization']
            
        save_config(config)
        st.success("Settings saved successfully!")

    if st.button("Connect"):
        try:
            # Validate connection settings
            if connection_mode == "Standard" and not snowflake_config.get('account'):
                st.error("Snowflake Account URL or ID is required for Standard connection mode.")
                raise ValueError("Snowflake Account URL or ID is required")
                
            if connection_mode == "PrivateLink" and not snowflake_config.get('custom_url'):
                st.error("PrivateLink URL is required for PrivateLink connection mode.")
                raise ValueError("PrivateLink URL is required")
            
            # Ensure organization URL has https:// prefix
            if ellie_config['organization'] and not ellie_config['organization'].startswith(('http://', 'https://')):
                ellie_config['organization'] = 'https://' + ellie_config['organization']
                
            # Connect to Snowflake
            snowflake_connect(snowflake_config)
            st.session_state.connected_to_snowflake = True
            
            # Connect to Ellie
            ellie_connect(ellie_config)
            st.session_state.connected_to_ellie = True
            
            st.success("Connected to both services!")
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")

# Main content
if st.session_state.connected_to_snowflake and st.session_state.connected_to_ellie:
    st.header("Data Transfer")
    
    # Get available schemas
    try:
        conn = connect_to_snowflake(snowflake_config)
        if conn:
            cursor = conn.cursor()
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            # Schema selection
            selected_schema = st.selectbox("Select Schema", schemas)
            
            if selected_schema:
                # Model configuration
                st.subheader("Model Configuration")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Model name input (default to schema name)
                    model_name = st.text_input("Model Name", selected_schema)
                    # Add note about physical model
                    st.caption("Models will be imported as physical models")
                    
                    # Include views option
                    include_views = st.checkbox("Include Views", value=True, 
                                             help="If checked, both tables and views will be included. If unchecked, only tables will be included.")
                
                with col2:
                    # Folder ID input (using the default from config if available)
                    folder_id = st.text_input(
                        "Folder ID", 
                        ellie_config.get('folder_id', ''),
                        help="The ID of the folder where the model will be created (must be a number)"
                    )
                    
                    # Debug mode
                    debug_mode = st.checkbox("Debug Mode (Show API responses)")
                
                # Info about foreign key detection
                st.info("""
                The application will automatically detect and extract relationships from your database's foreign key constraints.
                Ensure your tables have proper foreign key constraints defined to create relationships in the model.
                """)
                
                # Transfer button
                if st.button("Transfer to Ellie"):
                    if not folder_id:
                        st.error("Folder ID is required. Please enter a valid Folder ID.")
                    else:
                        try:
                            # Try to convert folder_id to an integer
                            folder_id_int = int(folder_id)
                            
                            with st.spinner("Transferring data..."):
                                try:
                                    # Ensure organization URL has https:// prefix again (in case it was changed)
                                    if ellie_config['organization'] and not ellie_config['organization'].startswith(('http://', 'https://')):
                                        ellie_config['organization'] = 'https://' + ellie_config['organization']
                                        ellie_connect(ellie_config)
                                        
                                    # Export data from selected schema, passing the include_views preference
                                    data = snowflake_export([selected_schema], include_views)
                                    
                                    # Add the folderId to the model data as an integer
                                    data['model']['folderId'] = folder_id_int
                                    
                                    if debug_mode:
                                        st.subheader("API Request Data:")
                                        entity_count = len(data['model'].get('entities', []))
                                        relationship_count = len(data['model'].get('relationships', []))
                                        st.write(f"Found {entity_count} entities and {relationship_count} relationships based on foreign key constraints")
                                        st.code(json.dumps(data, indent=2), language="json")
                                    
                                    # Import to Ellie as physical model
                                    model_level = "physical"
                                    response = ellie_model_import(model_name, data, model_level)
                                    
                                    if debug_mode:
                                        st.subheader(f"API Response:")
                                        st.write(f"Status Code: {response.status_code}")
                                        try:
                                            response_json = response.json()
                                            st.json(response_json)
                                        except:
                                            st.text(response.text)
                                    
                                    # Check response status
                                    if response.status_code >= 200 and response.status_code < 300:
                                        entity_count = len(data['model'].get('entities', []))
                                        relationship_count = len(data['model'].get('relationships', []))
                                        
                                        entity_type = "tables/views" if include_views else "tables"
                                        success_msg = f"Physical model successfully created: {model_name} with {entity_count} {entity_type}"
                                        
                                        if relationship_count > 0:
                                            success_msg += f" and {relationship_count} relationships"
                                        else:
                                            success_msg += " (no relationships found in schema)"
                                        
                                        st.success(success_msg)
                                        
                                        # Get model ID from response and create direct URL
                                        try:
                                            response_json = response.json()
                                            
                                            # Extract model ID from response - Ellie API returns 'id' field
                                            model_id = None
                                            if 'id' in response_json:
                                                model_id = response_json['id']
                                            elif 'modelId' in response_json:
                                                model_id = response_json['modelId']
                                            
                                            if model_id:
                                                # Create direct link to the model
                                                model_url = f"{ellie_config['organization']}/models/{model_level}/{model_id}"
                                                st.markdown(f"### [Open your model: {model_name}]({model_url})")
                                                
                                                # Show model ID for reference
                                                st.caption(f"Model ID: {model_id}")
                                            else:
                                                # Fallback to search URL if model ID is not found
                                                model_url = f"{ellie_config['organization']}/models?search={model_name}"
                                                st.markdown(f"### [Search for your model: {model_name}]({model_url})")
                                                
                                                if debug_mode:
                                                    st.warning("Model ID not found in API response. Response format:")
                                                    st.json(response_json)
                                        except Exception as e:
                                            # Fallback to search URL if we can't parse the response
                                            model_url = f"{ellie_config['organization']}/models?search={model_name}"
                                            st.markdown(f"### [Search for your model: {model_name}]({model_url})")
                                            
                                            if debug_mode:
                                                st.warning(f"Error extracting model ID: {str(e)}. Response text:")
                                                st.text(response.text)
                                        
                                        # Show info if no relationships were found
                                        if relationship_count == 0:
                                            st.warning("""
                                            No relationships were found in your schema. This could be because:
                                            1. Your tables don't have explicit foreign key constraints defined
                                            2. The constraints couldn't be detected by our queries
                                            
                                            You can manually add relationships in the Ellie interface after the model is created.
                                            """)
                                    else:
                                        st.error(f"Failed to create model: {model_name} - Status {response.status_code}")
                                        st.subheader("Error details:")
                                        st.text(response.text)
                                        
                                        # Troubleshooting guidance
                                        st.warning("""
                                        Troubleshooting steps:
                                        1. Verify your Ellie token is valid
                                        2. Check that your organization URL is correct
                                        3. Make sure the Folder ID is correct and you have permission to create models in it
                                        4. Check for any data format issues (see API Request Data in Debug Mode)
                                        """)
                                        
                                except Exception as e:
                                    st.error(f"Transfer failed: {str(e)}")
                                    st.exception(e)
                        except ValueError:
                            st.error("Folder ID must be a number. Please enter a valid numeric Folder ID.")
    except Exception as e:
        st.error(f"Error: {str(e)}")
else:
    st.info("Please connect to Snowflake and Ellie using the sidebar settings first.") 