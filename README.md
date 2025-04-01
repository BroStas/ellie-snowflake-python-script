# Snowflake to Ellie Transfer Tool

A Streamlit-based application for transferring database schema information from Snowflake to Ellie.ai, an enterprise data modeling platform.

## Features

- **Easy Configuration**: Simple UI for managing Snowflake and Ellie connections
- **Schema Selection**: Browse and select schemas from your Snowflake database
- **View Management**: Choose whether to include views in addition to tables
- **Relationship Detection**: Automatically detect relationships from foreign key constraints
- **Customization**: Specify model names and target folders in Ellie
- **PrivateLink Support**: Connect to Snowflake via PrivateLink when using VPN
- **Debug Mode**: See detailed API requests and responses

## Quick Start

### Prerequisites

- Python 3.7+
- Snowflake account with access credentials
- Ellie.ai account with API token

### Installation

1. Extract the zip file to a directory of your choice:
   ```bash
   unzip snowflake-to-ellie.zip
   cd snowflake-to-ellie
   ```

2. Run the installation script:
   ```bash
   chmod +x install.sh
   ./install.sh
   ```

### Running the Application

1. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Navigate to the python directory:
   ```bash
   cd python
   ```

3. Start the Streamlit app:
   ```bash
   streamlit run app.py
   ```

4. Open your browser at `http://localhost:8501`

## Using the Application

1. Fill in your credentials in the sidebar and click "Connect"
2. Select a schema from the dropdown
3. Enter a model name (defaults to schema name)
4. Enter the folder ID where the model should be created in Ellie
5. Choose whether to include views using the checkbox
6. Click "Transfer to Ellie" to start the process
7. When finished, click the link to view your model in Ellie

## Troubleshooting

### Common Issues

1. **Connection problems**:
   - Verify your Snowflake credentials
   - Check your Ellie API token is valid and has appropriate permissions

2. **Model creation fails**:
   - Ensure the Folder ID is correct and you have permission to create models in it
   - Check if the schema has tables/views (empty schemas will be skipped)

3. **No relationships in created model**:
   - Your tables might not have explicit foreign key constraints
   - Try enabling Debug Mode to see what data is being sent to Ellie

### For PrivateLink Users

If connecting to Snowflake via PrivateLink:
1. Select "PrivateLink" in the connection mode options
2. Enter your custom PrivateLink URL
3. Make sure your machine has access to the private network

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- Built with [Streamlit](https://streamlit.io/)
- Uses [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- Integrates with [Ellie.ai](https://ellie.ai/) 