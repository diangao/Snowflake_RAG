# Welcome to [FurWell](https://furwell.streamlit.app/)! 
- To access the app, follow this [link](https://furwell.streamlit.app/)!

- Contributors:
- [Angela Tao](https://github.com/XinranTaoAngela):xinran.tao2001@gmail.com
- [Dian Gao](https://github.com/diangao):
- [Xinwei Song](https://github.com/XinweiSong1018):song.xinwe@northeastern.edu

- A Streamlit-based chatbot that uses Snowflake's capabilities to provide intelligent responses about animal health data.

## Features

- Interactive chat interface using Streamlit
- Integration with Snowflake for data storage and retrieval
- Context-aware responses using chat history
- Category-based filtering of responses
- Debug mode for development

## Prerequisites

- Python 3.11+
- Snowflake account with appropriate permissions
- Required Python packages (see `requirements.txt`)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create and activate a virtual environment:
```bash
python -m venv venv

# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
- Copy `.env.example` to `.env`
- Update the values in `.env` with your Snowflake credentials

5. Run the application:
```bash
streamlit run streamlit_bot.py
```

## Usage

1. Select a category from the sidebar to filter responses
2. Type your question in the chat input
3. The bot will:
   - Search relevant documents
   - Consider chat history for context
   - Provide a response based on the available information

Debug mode can be enabled in the sidebar to see:
- Session state
- Chat history summary
- Search results

## Project Structure

```
.
├── streamlit_bot.py    # Main application file
├── requirements.txt    # Python dependencies
├── .env.example       # Example environment variables
├── .env              # Local environment variables (not in git)
└── .gitignore        # Git ignore rules
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 