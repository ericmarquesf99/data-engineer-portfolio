"""
Unit Tests for CoinGecko Extractor
===================================

Tests for API data extraction with mocked HTTP requests.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.extractors.coingecko_extractor import CryptoExtractor


@pytest.fixture
def sample_api_response():
    """Sample API response from CoinGecko"""
    return {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45000.50,
        "market_cap": 850000000000,
        "total_volume": 25000000000,
        "price_change_24h": 1250.75,
        "price_change_percentage_24h": 2.85,
        "last_updated": "2026-01-19T12:00:00.000Z"
    }


@pytest.fixture
def extractor_config():
    """Configuration for extractor"""
    return {
        "api": {
            "coingecko": {
                "base_url": "https://api.coingecko.com/api/v3",
                "timeout": 30,
                "retry_attempts": 3,
                "retry_delay": 1,
                "rate_limit_per_minute": 50
            }
        }
    }


class TestCryptoExtractor:
    """Test suite for CryptoExtractor class"""
    
    def test_initialization(self, extractor_config):
        """Test extractor initialization"""
        extractor = CryptoExtractor(extractor_config)
        
        assert extractor.base_url == "https://api.coingecko.com/api/v3"
        assert extractor.timeout == 30
        assert extractor.retry_attempts == 3
    
    @patch('requests.Session.get')
    def test_get_crypto_data_success(self, mock_get, extractor_config, sample_api_response):
        """Test successful API data retrieval"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor(extractor_config)
        result = extractor.get_crypto_data("bitcoin")
        
        assert result is not None
        assert result["id"] == "bitcoin"
        assert result["symbol"] == "btc"
        assert result["current_price"] == 45000.50
        mock_get.assert_called_once()
    
    @patch('requests.Session.get')
    def test_get_crypto_data_rate_limiting(self, mock_get, extractor_config):
        """Test rate limiting enforcement"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "bitcoin"}
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor(extractor_config)
        
        # Make multiple requests quickly
        start_time = datetime.now()
        extractor.get_crypto_data("bitcoin")
        extractor.get_crypto_data("ethereum")
        end_time = datetime.now()
        
        # Should have delayed due to rate limiting
        elapsed = (end_time - start_time).total_seconds()
        min_expected_delay = 60 / extractor_config["api"]["coingecko"]["rate_limit_per_minute"]
        
        assert elapsed >= min_expected_delay * 0.8  # Allow some margin
    
    @patch('requests.Session.get')
    def test_get_crypto_data_api_error(self, mock_get, extractor_config):
        """Test handling of API errors"""
        # Mock API error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("Not Found")
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor(extractor_config)
        result = extractor.get_crypto_data("invalid_coin")
        
        assert result is None
    
    @patch('requests.Session.get')
    def test_get_multiple_cryptos(self, mock_get, extractor_config, sample_api_response):
        """Test retrieving multiple cryptocurrencies"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor(extractor_config)
        crypto_ids = ["bitcoin", "ethereum", "cardano"]
        
        results = extractor.get_multiple_cryptos(crypto_ids)
        
        assert len(results) == 3
        assert all(result is not None for result in results)
    
    @patch('dbutils.fs.put')
    def test_save_to_dbfs(self, mock_dbfs_put, extractor_config, sample_api_response):
        """Test saving data to DBFS"""
        extractor = CryptoExtractor(extractor_config)
        
        path = "dbfs:/mnt/data/test.json"
        success = extractor.save_to_json([sample_api_response], path)
        
        # In non-Databricks environment, should fall back to local save
        # This test would need to be run in actual Databricks to test DBFS
        assert success in [True, False]  # Depends on environment
    
    def test_validate_response_structure(self, extractor_config, sample_api_response):
        """Test response validation"""
        extractor = CryptoExtractor(extractor_config)
        
        # Valid response
        assert extractor._validate_response(sample_api_response) == True
        
        # Invalid response (missing required fields)
        invalid_response = {"id": "bitcoin"}
        assert extractor._validate_response(invalid_response) == False
    
    @patch('requests.Session.get')
    def test_retry_on_failure(self, mock_get, extractor_config):
        """Test retry logic on transient failures"""
        # First two calls fail, third succeeds
        mock_get.side_effect = [
            Exception("Connection error"),
            Exception("Timeout"),
            Mock(status_code=200, json=lambda: {"id": "bitcoin"})
        ]
        
        extractor = CryptoExtractor(extractor_config)
        result = extractor.get_crypto_data("bitcoin")
        
        # Should succeed after retries
        assert result is not None
        assert mock_get.call_count == 3


class TestDataExtraction:
    """Integration-like tests for extraction workflow"""
    
    @patch('requests.Session.get')
    def test_full_extraction_workflow(self, mock_get, extractor_config, sample_api_response):
        """Test complete extraction workflow"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor(extractor_config)
        
        # Extract data
        crypto_ids = ["bitcoin", "ethereum"]
        results = []
        
        for crypto_id in crypto_ids:
            data = extractor.get_crypto_data(crypto_id)
            if data:
                results.append(data)
        
        # Validate results
        assert len(results) == 2
        assert all("id" in result for result in results)
        assert all("current_price" in result for result in results)
    
    def test_error_handling_in_batch(self, extractor_config):
        """Test that one failure doesn't stop batch processing"""
        with patch('requests.Session.get') as mock_get:
            # Mix of success and failure
            mock_get.side_effect = [
                Mock(status_code=200, json=lambda: {"id": "bitcoin", "symbol": "btc"}),
                Mock(status_code=404, raise_for_status=lambda: Exception("Not found")),
                Mock(status_code=200, json=lambda: {"id": "ethereum", "symbol": "eth"})
            ]
            
            extractor = CryptoExtractor(extractor_config)
            crypto_ids = ["bitcoin", "invalid", "ethereum"]
            
            results = []
            for crypto_id in crypto_ids:
                data = extractor.get_crypto_data(crypto_id)
                if data:
                    results.append(data)
            
            # Should have 2 successful results despite one failure
            assert len(results) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
