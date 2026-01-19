"""
Structured Logging Configuration
=================================

JSON-based structured logging for production observability.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logs
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class StructuredLogger:
    """
    Structured logger with JSON output and contextual metadata
    
    Usage:
        logger = StructuredLogger("extraction")
        logger.log_event("api_call_started", {"endpoint": "/coins/bitcoin"})
        logger.log_event("api_call_completed", {"duration": 1.5}, level="INFO")
    """
    
    def __init__(
        self,
        name: str,
        log_file: Optional[str] = None,
        level: str = "INFO"
    ):
        """
        Initialize structured logger
        
        Args:
            name: Logger name (typically module/component name)
            log_file: Optional file path for logging (defaults to logs/{name}.json)
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Console handler with JSON formatter
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(console_handler)
        
        # File handler if specified
        if log_file:
            log_path = Path(log_file)
        else:
            log_path = Path("logs") / f"{name}.json"
        
        # Create logs directory if it doesn't exist
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(file_handler)
        
        self.run_id: Optional[str] = None
        self.context: Dict[str, Any] = {}
    
    def set_run_id(self, run_id: str):
        """
        Set run ID for tracking pipeline execution
        
        Args:
            run_id: Unique identifier for pipeline run
        """
        self.run_id = run_id
        self.context["run_id"] = run_id
    
    def add_context(self, **kwargs):
        """
        Add contextual metadata to all subsequent logs
        
        Args:
            **kwargs: Key-value pairs to add to context
        """
        self.context.update(kwargs)
    
    def log_event(
        self,
        event_name: str,
        data: Optional[Dict[str, Any]] = None,
        level: str = "INFO"
    ):
        """
        Log a structured event
        
        Args:
            event_name: Name of the event (e.g., "extraction_started")
            data: Additional event data (metrics, parameters, etc.)
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        log_data = {
            "event": event_name,
            **self.context,
        }
        
        if data:
            log_data.update(data)
        
        # Create log record with extra data
        log_record = self.logger.makeRecord(
            self.logger.name,
            getattr(logging, level.upper()),
            "(structured)",
            0,
            event_name,
            (),
            None
        )
        log_record.extra_data = log_data
        
        self.logger.handle(log_record)
    
    def log_metric(
        self,
        metric_name: str,
        value: float,
        unit: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Log a metric value
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Optional unit (e.g., "seconds", "records", "bytes")
            tags: Optional tags for metric categorization
        """
        metric_data = {
            "metric_type": "gauge",
            "metric_name": metric_name,
            "value": value,
        }
        
        if unit:
            metric_data["unit"] = unit
        
        if tags:
            metric_data["tags"] = tags
        
        self.log_event("metric", metric_data, level="INFO")
    
    def log_error(
        self,
        error_name: str,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Log an error with exception details
        
        Args:
            error_name: Name/category of the error
            error: Exception object
            context: Additional context about the error
        """
        error_data = {
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        
        if context:
            error_data.update(context)
        
        self.log_event(error_name, error_data, level="ERROR")
    
    def log_duration(
        self,
        operation: str,
        duration_seconds: float,
        success: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log operation duration
        
        Args:
            operation: Name of the operation
            duration_seconds: Duration in seconds
            success: Whether operation succeeded
            metadata: Additional operation metadata
        """
        duration_data = {
            "operation": operation,
            "duration_seconds": duration_seconds,
            "success": success,
        }
        
        if metadata:
            duration_data.update(metadata)
        
        level = "INFO" if success else "WARNING"
        self.log_event("operation_completed", duration_data, level=level)


def get_logger(name: str, level: str = "INFO") -> StructuredLogger:
    """
    Factory function to get a structured logger
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        StructuredLogger instance
    """
    return StructuredLogger(name, level=level)
