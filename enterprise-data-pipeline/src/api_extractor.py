"""
API Data Extractor
Extracts cryptocurrency data from CoinGecko API with retry logic and error handling
"""

import requests
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIExtractor:
    """
    Extractor for cryptocurrency data from CoinGecko API
    Implements retry logic, rate limiting, and error handling
    """
    
    def __init__(self, config: Dict):
        """
        Initialize API Extractor
        
        Args:
            config: Configuration dictionary containing API settings
        """
        self.config = config
        self.base_url = config['api']['coingecko']['base_url']
        self.timeout = config['api']['coingecko']['timeout']
        self.retry_attempts = config['api']['coingecko']['retry_attempts']
        self.retry_delay = config['api']['coingecko']['retry_delay']
        self.rate_limit = config['api']['coingecko']['rate_limit_per_minute']
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Enterprise-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 60 / self.rate_limit
        
        logger.info("APIExtractor initialized successfully")
    
    def _rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout)),
        reraise=True
    )
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request with retry logic
        
        Args:
            endpoint: API endpoint to call
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Making request to: {url}")
        
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info(f"Request successful: {response.status_code}")
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            if response.status_code == 429:  # Rate limit exceeded
                logger.warning("Rate limit exceeded, backing off...")
                time.sleep(60)
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def extract_crypto_markets(self, page: int = 1, per_page: int = 100) -> List[Dict]:
        """
        Extract cryptocurrency market data
        
        Args:
            page: Page number to fetch
            per_page: Number of results per page
            
        Returns:
            List of cryptocurrency market data
        """
        logger.info(f"Extracting crypto markets data (page={page}, per_page={per_page})")
        
        endpoint = self.config['api']['coingecko']['endpoints']['markets']
        params = {
            'vs_currency': self.config['api']['coingecko']['params']['vs_currency'],
            'order': self.config['api']['coingecko']['params']['order'],
            'per_page': per_page,
            'page': page,
            'sparkline': False,
            'price_change_percentage': '24h,7d,30d'
        }
        
        try:
            data = self._make_request(endpoint, params)
            
            # Add extraction metadata
            extraction_time = datetime.utcnow().isoformat()
            for record in data:
                record['extracted_at'] = extraction_time
                record['source'] = 'coingecko_api'
                record['extraction_page'] = page
            
            logger.info(f"Successfully extracted {len(data)} records")
            return data
            
        except Exception as e:
            logger.error(f"Failed to extract crypto markets: {e}")
            raise
    
    def extract_trending_coins(self) -> Dict:
        """
        Extract trending cryptocurrency data
        
        Returns:
            Trending coins data
        """
        logger.info("Extracting trending coins data")
        
        endpoint = self.config['api']['coingecko']['endpoints']['trending']
        
        try:
            data = self._make_request(endpoint)
            
            # Add metadata
            data['extracted_at'] = datetime.utcnow().isoformat()
            data['source'] = 'coingecko_api'
            
            logger.info(f"Successfully extracted trending data")
            return data
            
        except Exception as e:
            logger.error(f"Failed to extract trending coins: {e}")
            raise
    
    def extract_multiple_pages(self, num_pages: int = 3, per_page: int = 100) -> List[Dict]:
        """
        Extract multiple pages of crypto market data
        
        Args:
            num_pages: Number of pages to extract
            per_page: Records per page
            
        Returns:
            Combined list of all records
        """
        logger.info(f"Extracting {num_pages} pages of data")
        
        all_data = []
        
        for page in range(1, num_pages + 1):
            try:
                page_data = self.extract_crypto_markets(page=page, per_page=per_page)
                all_data.extend(page_data)
                logger.info(f"Page {page}/{num_pages} completed")
                
            except Exception as e:
                logger.error(f"Failed to extract page {page}: {e}")
                # Continue with next page even if one fails
                continue
        
        logger.info(f"Total records extracted: {len(all_data)}")
        return all_data
    
    def save_to_json(self, data: List[Dict], output_path: str):
        """
        Save extracted data to JSON file
        
        Args:
            data: Data to save
            output_path: Path to output file
        """
        logger.info(f"Saving data to {output_path}")
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Data saved successfully: {len(data)} records")
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise


def main():
    """Main execution function"""
    import yaml
    
    # Load configuration
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize extractor
    extractor = APIExtractor(config)
    
    # Extract data
    data = extractor.extract_multiple_pages(num_pages=2, per_page=50)
    
    # Save to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'../logs/crypto_data_{timestamp}.json'
    extractor.save_to_json(data, output_path)
    
    print(f"✅ Extraction completed: {len(data)} records")


if __name__ == "__main__":
    main()
