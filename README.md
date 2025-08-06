# Customer360 Analytics: Enterprise Data Engineering Pipeline

## Abstract
This project implements a comprehensive Customer 360-degree analytics platform utilizing modern data engineering methodologies. The system processes multi-modal customer interaction data through a scalable Apache Spark-based ETL pipeline, enabling advanced customer segmentation, behavioral analysis, and business intelligence capabilities.

## Architecture Overview
![Data Pipeline Architecture](https://github.com/hien2706/Customer360/blob/main/pictures/ETL_pipeline_new.png)

The solution employs a distributed data processing architecture with the following technology stack:

### Core Technologies
- **Data Processing Engine**: Apache Spark (PySpark) 4.0+
- **Data Storage**: Azure MySQL Database
- **Business Intelligence**: Microsoft Power BI
- **Data Formats**: Parquet (columnar), JSON (semi-structured), CSV (tabular)
- **Programming Language**: Python 3.x

### System Requirements
- Apache Spark 4.0.0+ with Hadoop 3.x
- Python 3.8+ with PySpark, pandas, openpyxl libraries
- MySQL Connector/J 8.0.30+
- Minimum 6GB RAM allocation for Spark driver

## Methodology

### Data Engineering Pipeline

#### 1. Data Extraction Phase
The system ingests heterogeneous data sources through Apache Spark's distributed file reading capabilities:

**Data Sources:**
- **Behavioral Data**: Parquet files containing user search patterns and temporal interactions
- **Transactional Data**: JSON files with customer engagement metrics and device usage statistics

**Data Volume**: Multi-gigabyte datasets with millions of customer interaction records spanning temporal ranges (YYYYMMDD format for content logs, YYYYMM format for search logs).

#### 2. Data Transformation Phase

##### 2.1 Customer Interaction Analytics
**Input**: Semi-structured JSON files containing customer engagement data
**Processing Script**: `ETL_script.py`

**Transformation Pipeline:**
1. **Content Categorization**: Application-based content mapping using rule-based classification
   ```
   CHANNEL, DSHD, KPLUS → Truyen_hinh (Television)
   VOD, FIMS → Phim_truyen (Movies)
   SPORT → The_thao (Sports)
   RELAX → Giai_tri (Entertainment)
   CHILD → Thieu_nhi (Children)
   ```

2. **Device Usage Aggregation**: Calculation of unique device fingerprints per customer contract using `countDistinct()` operations

3. **Content Preference Analysis**: Statistical analysis to identify primary viewing categories using `greatest()` function across content types

4. **Customer Taste Profiling**: Multi-dimensional preference vector creation using `concat_ws()` for non-zero viewing categories

5. **Activity Score Calculation**: Temporal engagement measurement based on unique active days per customer
   ```
   Activity Levels:
   1-7 days: "very low"    │ 8-14 days: "low"
   15-21 days: "moderate"  │ 22-28 days: "high"  
   29-31 days: "very high"
   ```

6. **Customer Segmentation**: Advanced statistical segmentation using Interquartile Range (IQR) analysis with `percentile_approx()` function
   ```
   Segments:
   • Leaving: Low activity + Duration < Q1
   • Need Attention: Low activity + Duration < Median
   • Normal: Moderate activity + Duration < Median
   • Potential: Moderate activity + Duration ≥ Median
   • Loyal: High activity + Duration > Q1
   • VIP: Very high activity + Duration > Q1
   ```

**Data Quality Enhancement:**
- Null value filtering and data validation
- Duplicate record elimination
- Temporal filtering with configurable date ranges

**Performance Optimizations:**
- DataFrame caching using `.cache()` for iterative operations
- Column-wise operations minimizing data shuffling
- Lazy evaluation leveraging Spark's Catalyst optimizer

##### 2.2 Search Behavior Analytics
**Input**: Columnar Parquet files with user search query logs
**Processing Integration**: Incorporated within `ETL_script.py`

**Advanced Processing Pipeline:**

1. **Temporal Data Parsing**: Robust datetime handling with malformed data tolerance
   ```python
   # Regex-based validation for datetime strings
   when(col("datetime").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}"), 
        to_date(col("datetime"))).otherwise(None)
   ```

2. **Search Pattern Analysis**: Window function-based ranking to identify dominant search keywords per user
   ```sql
   -- Equivalent SQL operation
   ROW_NUMBER() OVER (PARTITION BY month, user_id ORDER BY search_count DESC)
   ```

3. **Keyword Categorization**: External mapping table integration with left joins for semantic classification

4. **Trend Analysis**: Comparative analysis across temporal periods (Month 6 vs Month 7)
   - **Trending_Type**: Binary classification (Changed/Unchanged)
   - **Previous**: State transition tracking with concatenated change vectors

5. **Data Quality Assurance**:
   - Null value elimination using `isNotNull()` filters
   - Temporal range validation (months 6-7)
   - Result set limitation for processing efficiency (250 records)
  
#### 3. Data Integration & Consolidation

**Unified Customer View Creation:**
- **Join Strategy**: Index-based joining using `monotonically_increasing_id()` for dataset alignment
- **Schema Harmonization**: Column standardization and duplicate attribute removal
- **Dimensional Modeling**: Star schema preparation for OLAP operations

#### 4. Data Loading & Persistence

**Multi-Target Output Strategy:**
- **Intermediate Storage**: CSV format with single partition using `.repartition(1)` for consolidated output
- **Production Database**: Azure MySQL integration via JDBC connector with batch loading
- **Data Validation**: Automatic success markers (`_SUCCESS` files) and checksum verification

### Data Visualizations

#### Before/After Transformation Analysis


## Business Intelligence & Analytics

### Advanced Dashboard Implementation
Comprehensive analytical dashboards built using Microsoft Power BI, featuring:

- **Customer Segmentation Analysis**: Multi-dimensional customer classification with drill-down capabilities
- **Behavioral Trend Analysis**: Temporal pattern recognition and forecasting models
- **Content Performance Metrics**: Content category performance and user engagement analytics
- **Device Usage Analytics**: Multi-device usage patterns and cross-platform behavior analysis

**Interactive Dashboard:**


**Detailed Analytics Report:**


## Technical Implementation

### Execution Instructions

1. **Environment Setup**
   ```bash
   # Install required dependencies
   pip install pyspark pandas openpyxl
   
   # Configure Spark environment
   export SPARK_HOME=/path/to/spark-4.0.0-bin-hadoop3
   export PYSPARK_PYTHON=python3
   ```

2. **Pipeline Execution**
   ```bash
   cd ETL_scripts/
   python ETL_script.py
   ```

3. **Configuration Parameters**
   - Log content path: Directory containing JSON files
   - Log search path: Directory containing Parquet files
   - Output path: Destination for processed CSV files
   - Mapping file: Excel/CSV file for keyword categorization
   - Date ranges: YYYYMMDD format for content, YYYYMM for search data

### Performance Characteristics

- **Scalability**: Horizontal scaling across Spark cluster nodes
- **Memory Efficiency**: Optimized through DataFrame caching and lazy evaluation
- **Fault Tolerance**: Built-in Spark resilience with automatic task retry mechanisms
- **Data Quality**: Comprehensive validation and error handling for malformed data

### Key Innovations

1. **Multilingual Data Handling**: Robust processing of Persian/Arabic numerals in datetime fields
2. **Dynamic Schema Adaptation**: Flexible handling of varying JSON structures
3. **Statistical Customer Segmentation**: IQR-based quantitative customer classification
4. **Real-time Data Quality Monitoring**: Automated data validation and quality metrics

## Future Enhancements

- **Machine Learning Integration**: Predictive analytics for customer churn and lifetime value
- **Real-time Streaming**: Apache Kafka integration for continuous data processing
- **Advanced Analytics**: Time-series forecasting and anomaly detection
- **Cloud Migration**: Full Azure ecosystem integration with Data Factory and Synapse Analytics

## Contributing

This project follows enterprise data engineering best practices. For contributions, please ensure:
- Comprehensive unit testing coverage
- Documentation of data lineage and transformations
- Performance benchmarking for large-scale datasets
- Compliance with data governance and privacy regulations

---
*Last Updated: August 2025*  
*Project Type: Enterprise Data Engineering Pipeline*  
*Classification: Customer Analytics & Business Intelligence*
