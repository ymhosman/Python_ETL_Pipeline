# Python_ETL_Pipeline
This project implements an ETL pipeline using Python. Data is extracted from two sources: MySQL app transaction data and a CSV restaurant reference file. Quality rules and enrichment are applied to the data, and rejected rows are omitted and placed in a reject file. The pipeline produces an aggregated summary (CSV) and the aforementioned reject file consisting of rows that failed hard-error rules.

## Architecture Overview

The project consists of 1 main module (`main.py`) and 4 sub-modules: `extract.py`, `transform.py`, `load.py`, and `config.py`. The extract module reads. The transform module has 4 functions; transform cleans the data and handles null values, along with adding soft and hard error flags. The reject function produces two flows, a cleaned output along with a DataFrame of rejected rows along with flags. Aggregate then produces a final insights by aggregating values from the clean flow.

## Source Description
### Source A – Transactions (MySQL)
- **Table:** `source_sales`
- **Fields:** `sale_id`, `restaurant_code`, `quantity`, `order_price`, `sale_date`, `customer_rating`, `discount_applied`, `time_to_deliver`
- **Characteristics:** 50 rows of order data, includes intentional errors (negative/zero quantities, negative prices, high prices) to test quality rules.

### Source B – Reference (CSV)
- **File:** `restaurant_reference.csv`
- **Fields:** `restaurant_code`, `restaurant_name`, `category`, `chain`, `own_driver`, `delivery_fee`, `adv_delivery_time`, `location`
- **Characteristics:** Static lookup table with 6 restaurants. Restaurants R106 and R107 are not present in to simulate missing references.
## Quality Rules

#### Hard Errors (rows go to `reject_flow`)

| Rule | Condition | `rejection_reason` |
|------|-----------|--------------------|
| ERR‑01 | `order_price < 0` | `"Invalid order price"` |
| ERR‑02 | `quantity <= 0` | `"Invalid item quantity; "` |

#### Warning Rules (rows stay in `clean_flow` with flag)

| Rule | Condition | Message added to `data_quality_flag` |
|------|-----------|--------------------------------------|
| WRN‑01 | `order_price > 3000` | `"High price alert; "` |
| WRN‑02 | `category == null` or `"Unknown"` | `"Missing restuarant reference"` |

## Aggregation

The `clean_flow` is aggregated by `restaurant_code` to produce a summary dataset.

| Output Column | Function | Input Column |
|---------------|----------|--------------|
| `total_revenue` | `sum` | `total_price` |
| `avg_rating` | `avg` | `customer_rating` |
| `total_quantity` | `sum` | `quantity` |
| `transaction_count` | `count` | `sale_id` |
| `delivered_on_time_pct` | `lambda x: (x == 'Yes').sum()` | `delivered_on_time` |

The aggregated output is written to a CSV file (specified by `OUTPUT_AGG`).

## Run Instructions
1. Clone the repo
`git clone <your-repo-url>`
`cd <your-repo-folder>`

2. Create and activate virtual environment
`python -m venv venv`
`venv\Scripts\activate`

3. Install dependencies
`pip install -r requirements.txt`

4. Configure input and output paths in `config.py`

5. Run the pipeline
`python main.py`

## Assumptions and trade-offs

## Scale question
