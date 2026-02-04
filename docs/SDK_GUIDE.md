# Lightdash Python SDK Guide

This guide covers the query builder, filtering system, and other features of the Lightdash Python SDK.

## Quick Start

```python
from lightdash import Client

client = Client(base_url="https://app.lightdash.cloud", token="your-token")
model = client.get_model("your_project_uuid", "your_model_name")

# Build and execute a query
result = (
    model.query()
    .metrics(model.metrics.revenue, model.metrics.profit)
    .dimensions(model.dimensions.country)
    .filter(model.dimensions.status == "active")
    .sort(model.metrics.revenue.desc())
    .limit(100)
    .execute()
)

df = result.to_df()
```

---

## Query Builder

The SDK provides two patterns for building queries: **single-call** and **chainable builder**.

### Single-Call Pattern

Pass all parameters at once—useful for simple queries:

```python
query = model.query(
    metrics=[model.metrics.revenue, model.metrics.profit],
    dimensions=[model.dimensions.country],
    filters=model.dimensions.status == "active",
    sort=model.metrics.revenue.desc(),
    limit=100
)
result = query.execute()
```

### Chainable Builder Pattern

Build queries incrementally with method chaining—each method returns a new immutable `Query` object:

```python
query = (
    model.query()
    .metrics(model.metrics.revenue)
    .dimensions(model.dimensions.country, model.dimensions.date)
    .filter(model.dimensions.status == "active")
    .sort(model.metrics.revenue.desc())
    .limit(100)
)
```

**Key characteristics:**

- **Immutable**: Each method returns a new Query object (safe for reuse)
- **Lazy evaluation**: API calls only happen when `.execute()` is called
- **Order-independent**: Methods can be called in any order
- **Composable**: Create base queries and extend them

```python
# Create a reusable base query
base = model.query().metrics(model.metrics.revenue).dimensions(model.dimensions.country)

# Extend it for different use cases
by_active = base.filter(model.dimensions.status == "active")
by_inactive = base.filter(model.dimensions.status == "inactive")
```

---

## Filters

Filters let you constrain your query results. The SDK provides an intuitive, Pythonic API for creating filters.

### Creating Filters with Operators

Use standard Python comparison operators on dimensions:

```python
# Equality
f = model.dimensions.country == "USA"

# Inequality
f = model.dimensions.country != "USA"

# Numeric comparisons
f = model.dimensions.amount > 1000
f = model.dimensions.amount >= 1000
f = model.dimensions.amount < 500
f = model.dimensions.amount <= 500
```

### Filter Helper Methods

For more complex filters, use helper methods:

```python
# Check if value is in a list
f = model.dimensions.country.in_(["USA", "UK", "Canada"])

# String operations
f = model.dimensions.name.starts_with("John")
f = model.dimensions.name.ends_with("son")
f = model.dimensions.name.includes("Smith")

# Null checks
f = model.dimensions.email.is_null()
f = model.dimensions.email.is_not_null()
```

### Supported Operators by Data Type

| Operator | Numeric | String | Boolean | Date |
|----------|---------|--------|---------|------|
| `is null` | Yes | Yes | Yes | Yes |
| `is not null` | Yes | Yes | Yes | Yes |
| `equals` / `is` | Yes | Yes | Yes | Yes |
| `is not` | Yes | Yes | - | Yes |
| `is less than` | Yes | - | - | - |
| `is greater than` | Yes | - | - | - |
| `starts with` | - | Yes | - | - |
| `ends with` | - | Yes | - | - |
| `includes` | - | Yes | - | - |
| `in the last` | - | - | - | Yes |
| `in the next` | - | - | - | Yes |
| `in the current` | - | - | - | Yes |
| `is before` | - | - | - | Yes |
| `is after` | - | - | - | Yes |
| `is between` | - | - | - | Yes |

### Combining Filters

Use `&` (AND) and `|` (OR) operators to combine filters:

```python
# AND: Both conditions must be true
f = (model.dimensions.country == "USA") & (model.dimensions.amount > 1000)

# OR: Either condition must be true
f = (model.dimensions.status == "active") | (model.dimensions.status == "pending")

# Complex combinations
f = (
    (model.dimensions.country == "USA") &
    ((model.dimensions.amount > 1000) | (model.dimensions.priority == "high"))
)
```

Multiple `.filter()` calls on a query are combined with AND logic:

```python
query = (
    model.query()
    .filter(model.dimensions.country == "USA")
    .filter(model.dimensions.amount > 1000)  # AND-ed with above
)
```

---

## Dimensions and Metrics

Access dimensions and metrics as attributes on the model:

```python
# Access via attribute
country = model.dimensions.country
revenue = model.metrics.revenue

# List all available
all_dimensions = model.dimensions.list()
all_metrics = model.metrics.list()
```

**Features:**

- **Lazy loading**: Fetched from API on first access, then cached
- **Fuzzy matching**: Typos suggest closest matches
- **Tab completion**: Works in Jupyter/IPython for discovery
- **Rich display**: HTML rendering in notebooks

---

## Sorting

Sort results using the `.sort()` method or `Sort` class:

```python
from lightdash import Sort

# Using metric/dimension methods (recommended)
query = model.query().sort(model.metrics.revenue.desc())
query = model.query().sort(model.dimensions.country.asc())

# Multiple sorts
query = model.query().sort(
    model.metrics.revenue.desc(),
    model.dimensions.country.asc()
)

# Control null positioning
query = model.query().sort(model.dimensions.name.asc(nulls_first=True))

# Using Sort class directly
query = model.query().sort(Sort("orders_revenue", descending=True))
```

---

## Results

Query results implement a unified `ResultSet` interface:

### Converting Results

```python
result = query.execute()

# To pandas DataFrame
df = result.to_df()  # or result.to_df(backend="pandas")

# To polars DataFrame
df = result.to_df(backend="polars")

# To list of dictionaries
records = result.to_records()

# To JSON string
json_str = result.to_json_str()
```

### Iterating Over Results

```python
# Iterate over rows
for row in result:
    print(row)

# Get total count
total = len(result)
```

### Pagination

For large result sets, results are paginated automatically:

```python
result = query.execute()

# Access specific page
page_2 = result.page(2)

# Iterate through all pages
for page in result.iter_pages():
    process(page)

# Lazy DataFrame loading (polars only)
lazy_df = result.to_df_lazy()
```

**Properties:**

- `result.query_uuid` - Unique identifier for the query
- `result.total_results` - Total number of rows
- `result.total_pages` - Number of pages
- `result.fields` - Field metadata

---

## SQL Runner

Execute raw SQL queries directly against your data warehouse:

```python
# Execute SQL
result = client.sql("SELECT * FROM orders WHERE status = 'active' LIMIT 100")
df = result.to_df()

# With custom limit
result = client.sql("SELECT * FROM orders", limit=1000)

# Introspection
tables = client.sql_runner.tables()
fields = client.sql_runner.fields("orders")
fields = client.sql_runner.fields("orders", schema="public")
```

---

## Exception Handling

The SDK provides specific exceptions for different error conditions:

```python
from lightdash import LightdashError, QueryError, QueryTimeout, QueryCancelled

try:
    result = query.execute()
except QueryTimeout as e:
    print(f"Query timed out: {e.query_uuid}")
except QueryCancelled as e:
    print(f"Query was cancelled: {e.query_uuid}")
except QueryError as e:
    print(f"Query failed: {e.message}")
except LightdashError as e:
    print(f"Lightdash error: {e.message} (status: {e.status_code})")
```

**Exception hierarchy:**

- `LightdashError` - Base exception for all SDK errors
  - `QueryError` - Query execution failed (HTTP 400)
  - `QueryTimeout` - Query exceeded timeout (HTTP 408)
  - `QueryCancelled` - Query was cancelled (HTTP 499)

---

## Complete Example

```python
from lightdash import Client, QueryError, QueryTimeout

# Initialize client
client = Client(
    base_url="https://app.lightdash.cloud",
    token="your-api-token"
)

# Get a model
model = client.get_model("project-uuid", "orders")

# Build a query with filters
query = (
    model.query()
    .metrics(model.metrics.total_revenue, model.metrics.order_count)
    .dimensions(model.dimensions.country, model.dimensions.order_date)
    .filter(
        (model.dimensions.status == "completed") &
        (model.dimensions.order_date >= "2024-01-01")
    )
    .sort(model.metrics.total_revenue.desc())
    .limit(50)
)

try:
    result = query.execute()
    df = result.to_df()
    print(f"Fetched {len(result)} rows")
    print(df.head())
except QueryTimeout:
    print("Query took too long - try adding more filters")
except QueryError as e:
    print(f"Query failed: {e.message}")
```
