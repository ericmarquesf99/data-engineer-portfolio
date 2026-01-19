"""
Unit Tests for API Extractor
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from datetime import datetime
import json

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from api_extractor import APIExtractor


@pytest.fixture
def mock_config():
    """Mock configuration for testing"""
    return {
        'api': {
            'coingecko': {
                'base_url': 'https://api.coingecko.com/api/v3',
                'endpoints': {
                    'markets': '/coins/markets',
                    'trending': '/search/trending'
                },
                'params': {
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc'
                },
                'rate_limit_per_minute': 50,
                'timeout': 30,
                'retry_attempts': 3,
                'retry_delay': 5
            }
        }
    }


@pytest.fixture
def mock_api_response():
    """Mock API response data"""
    return [
        {
            'id': 'bitcoin',
            'symbol': 'btc',
            'name': 'Bitcoin',
            'current_price': 45000.0,
            'market_cap': 850000000000,
            'total_volume': 25000000000
        },
        {
            'id': 'ethereum',
            'symbol': 'eth',
            'name': 'Ethereum',
            'current_price': 2500.0,
            'market_cap': 300000000000,
            'total_volume': 15000000000
        }
    ]


class TestAPIExtractor:
    """Test suite for APIExtractor class"""
    
    def test_initialization(self, mock_config):
        """Test extractor initialization"""
        extractor = APIExtractor(mock_config)
        
        assert extractor.base_url == 'https://api.coingecko.com/api/v3'
        assert extractor.timeout == 30
        assert extractor.retry_attempts == 3
        assert extractor.rate_limit == 50
    
    @patch('api_extractor.requests.Session.get')
    def test_successful_api_call(self, mock_get, mock_config, mock_api_response):
        """Test successful API call"""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        # Execute
        extractor = APIExtractor(mock_config)
        result = extractor.extract_crypto_markets(page=1, per_page=2)
        
        # Assert
        assert len(result) == 2
        assert result[0]['id'] == 'bitcoin'
        assert 'extracted_at' in result[0]
        assert 'source' in result[0]
    
    @patch('api_extractor.requests.Session.get')
    def test_rate_limiting(self, mock_get, mock_config, mock_api_response):
        """Test rate limiting between requests"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(mock_config)
        
        # Make two requests
        start_time = datetime.now()
        extractor.extract_crypto_markets(page=1, per_page=2)
        extractor.extract_crypto_markets(page=2, per_page=2)
        end_time = datetime.now()
        
        # Check that time elapsed respects rate limit
        elapsed = (end_time - start_time).total_seconds()
        min_expected = extractor.min_request_interval
        
        # Some tolerance for test execution time
        assert elapsed >= (min_expected * 0.8)
    
    @patch('api_extractor.requests.Session.get')
    def test_retry_on_failure(self, mock_get, mock_config):
        """Test retry logic on failed requests"""
        # First two calls fail, third succeeds
        mock_get.side_effect = [
            requests.exceptions.Timeout(),
            requests.exceptions.RequestException(),
            Mock(status_code=200, json=lambda: [])
        ]
        
        extractor = APIExtractor(mock_config)
        result = extractor.extract_crypto_markets()
        
        # Should succeed after retries
        assert result == []
        assert mock_get.call_count == 3
    
    @patch('api_extractor.requests.Session.get')
    def test_http_error_handling(self, mock_get, mock_config):
        """Test handling of HTTP errors"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(mock_config)
        
        with pytest.raises(requests.exceptions.HTTPError):
            extractor.extract_crypto_markets()
    
    @patch('api_extractor.requests.Session.get')
    def test_rate_limit_exceeded(self, mock_get, mock_config):
        """Test handling of rate limit exceeded (429)"""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(mock_config)
        
        with pytest.raises(requests.exceptions.HTTPError):
            extractor.extract_crypto_markets()
    
    def test_save_to_json(self, mock_config, mock_api_response, tmp_path):
        """Test saving data to JSON file"""
        extractor = APIExtractor(mock_config)
        
        # Create temporary file path
        output_path = tmp_path / "test_output.json"
        
        # Save data
        extractor.save_to_json(mock_api_response, str(output_path))
        
        # Verify file exists and contains correct data
        assert output_path.exists()
        
        with open(output_path, 'r') as f:
            saved_data = json.load(f)
        
        assert len(saved_data) == 2
        assert saved_data[0]['id'] == 'bitcoin'
    
    @patch('api_extractor.requests.Session.get')
    def test_extract_multiple_pages(self, mock_get, mock_config, mock_api_response):
        """Test extracting multiple pages"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(mock_config)
        result = extractor.extract_multiple_pages(num_pages=3, per_page=2)
        
        # Should have data from 3 pages
        assert len(result) == 6  # 3 pages * 2 records each
        assert mock_get.call_count == 3
    
    @patch('api_extractor.requests.Session.get')
    def test_metadata_enrichment(self, mock_get, mock_config, mock_api_response):
        """Test that metadata is added to extracted records"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(mock_config)
        result = extractor.extract_crypto_markets(page=2, per_page=50)
        
        # Check metadata fields
        for record in result:
            assert 'extracted_at' in record
            assert 'source' in record
            assert record['source'] == 'coingecko_api'
            assert 'extraction_page' in record
            assert record['extraction_page'] == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
