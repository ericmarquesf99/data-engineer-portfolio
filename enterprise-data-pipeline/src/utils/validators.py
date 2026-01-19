"""
Data Validators
===============

Validation utilities for data quality checks.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


def validate_dataframe(
    df: DataFrame,
    required_columns: List[str],
    non_null_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Validate Spark DataFrame schema and data quality
    
    Args:
        df: Spark DataFrame to validate
        required_columns: List of columns that must be present
        non_null_columns: List of columns that cannot have nulls
        
    Returns:
        Validation results dictionary
        
    Raises:
        ValidationError: If critical validations fail
    """
    results = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "stats": {}
    }
    
    # Check required columns
    df_columns = set(df.columns)
    missing_columns = set(required_columns) - df_columns
    
    if missing_columns:
        results["valid"] = False
        results["errors"].append(f"Missing required columns: {missing_columns}")
    
    # Check for null values in non-null columns
    if non_null_columns:
        for col in non_null_columns:
            if col in df_columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    results["warnings"].append(
                        f"Column '{col}' has {null_count} null values"
                    )
    
    # Collect basic statistics
    results["stats"]["total_rows"] = df.count()
    results["stats"]["total_columns"] = len(df.columns)
    
    return results


def check_data_quality(
    df: DataFrame,
    quality_rules: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks
    
    Args:
        df: Spark DataFrame to check
        quality_rules: Dictionary of quality rules to apply
        
    Returns:
        Quality check results
    """
    results = {
        "passed": True,
        "checks": {},
        "summary": {}
    }
    
    total_rows = df.count()
    results["summary"]["total_rows"] = total_rows
    
    # Completeness checks
    if "required_columns" in quality_rules:
        for col in quality_rules["required_columns"]:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                
                results["checks"][f"{col}_completeness"] = {
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 2),
                    "passed": null_percentage < quality_rules.get("max_null_percentage", 5)
                }
                
                if null_percentage >= quality_rules.get("max_null_percentage", 5):
                    results["passed"] = False
    
    # Uniqueness checks
    if "unique_columns" in quality_rules:
        for col in quality_rules["unique_columns"]:
            if col in df.columns:
                total = df.count()
                distinct = df.select(col).distinct().count()
                duplicate_count = total - distinct
                
                results["checks"][f"{col}_uniqueness"] = {
                    "total": total,
                    "distinct": distinct,
                    "duplicates": duplicate_count,
                    "passed": duplicate_count == 0
                }
                
                if duplicate_count > 0:
                    results["passed"] = False
    
    # Range checks
    if "numeric_ranges" in quality_rules:
        for col, range_spec in quality_rules["numeric_ranges"].items():
            if col in df.columns:
                min_val = range_spec.get("min")
                max_val = range_spec.get("max")
                
                out_of_range = df.filter(
                    (F.col(col) < min_val) | (F.col(col) > max_val)
                ).count()
                
                results["checks"][f"{col}_range"] = {
                    "out_of_range_count": out_of_range,
                    "expected_range": f"[{min_val}, {max_val}]",
                    "passed": out_of_range == 0
                }
                
                if out_of_range > 0:
                    results["passed"] = False
    
    # Freshness check
    if "freshness_column" in quality_rules:
        col = quality_rules["freshness_column"]
        max_age_hours = quality_rules.get("max_age_hours", 24)
        
        if col in df.columns:
            from datetime import datetime, timedelta
            
            max_timestamp = df.agg(F.max(col)).collect()[0][0]
            if max_timestamp:
                age_hours = (datetime.now() - max_timestamp).total_seconds() / 3600
                
                results["checks"]["data_freshness"] = {
                    "latest_timestamp": max_timestamp.isoformat(),
                    "age_hours": round(age_hours, 2),
                    "max_age_hours": max_age_hours,
                    "passed": age_hours <= max_age_hours
                }
                
                if age_hours > max_age_hours:
                    results["passed"] = False
    
    return results


def validate_schema(
    df: DataFrame,
    expected_schema: Dict[str, str]
) -> Dict[str, Any]:
    """
    Validate DataFrame schema against expected schema
    
    Args:
        df: Spark DataFrame
        expected_schema: Dictionary mapping column names to expected data types
        
    Returns:
        Schema validation results
    """
    results = {
        "valid": True,
        "mismatches": []
    }
    
    actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
    
    for col, expected_type in expected_schema.items():
        if col not in actual_schema:
            results["valid"] = False
            results["mismatches"].append({
                "column": col,
                "issue": "missing",
                "expected": expected_type,
                "actual": None
            })
        elif expected_type not in actual_schema[col]:
            results["valid"] = False
            results["mismatches"].append({
                "column": col,
                "issue": "type_mismatch",
                "expected": expected_type,
                "actual": actual_schema[col]
            })
    
    return results


def detect_anomalies(
    df: pd.DataFrame,
    numeric_columns: List[str],
    z_threshold: float = 3.0
) -> Dict[str, Any]:
    """
    Detect anomalies in numeric columns using Z-score method
    
    Args:
        df: Pandas DataFrame
        numeric_columns: List of numeric columns to check
        z_threshold: Z-score threshold for anomaly detection
        
    Returns:
        Anomaly detection results
    """
    anomalies = {}
    
    for col in numeric_columns:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            mean = df[col].mean()
            std = df[col].std()
            
            if std > 0:
                z_scores = (df[col] - mean) / std
                anomaly_mask = abs(z_scores) > z_threshold
                anomaly_count = anomaly_mask.sum()
                
                anomalies[col] = {
                    "anomaly_count": int(anomaly_count),
                    "anomaly_percentage": round((anomaly_count / len(df)) * 100, 2),
                    "mean": float(mean),
                    "std": float(std),
                    "z_threshold": z_threshold
                }
                
                if anomaly_count > 0:
                    anomalies[col]["anomaly_values"] = df[anomaly_mask][col].tolist()[:10]  # First 10
    
    return anomalies
