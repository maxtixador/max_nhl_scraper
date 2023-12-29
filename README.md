# Max NHL Scraper

The `max_nhl_scraper` package is a Python tool for scraping NHL (National Hockey League) data. It allows users to easily extract detailed game data, player statistics, and other relevant information from the NHL's official sources.

## Installation

You can install `max_nhl_scraper` directly from PyPI:

```bash
pip install max_nhl_scraper
```

## Usage

Here's a simple example of how to use max_nhl_scraper:

```python
from max_nhl_scraper.nhl.main import scrape_game
from max_nhl_scraper.nhl.utility.functions import *



# Example: Scrape data for a specific game or player
data = scrape_game(game_id=2023020005) #Replace <2023020005> with the actual game ID you want to scrape data for.
```

## Requirements

nhl_scraper requires the following Python libraries:

- beautifulsoup4==4.12.2
- numpy==1.26.2
- pandas==2.1.4
- Requests==2.31.0
These dependencies should be automatically installed when you install the package via pip.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions to max_nhl_scraper are welcome! Please feel free to submit pull requests or open issues to discuss potential improvements or report bugs.

## Contact

If you have any questions or suggestions, please contact me at maxtixador@gmail.com.

## Acknowledgments

Special thanks to all contributors (just me for now) and users of max_nhl_scraper.