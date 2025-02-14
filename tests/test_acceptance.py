"""
Acceptance tests for the Lightdash client.

These tests require valid credentials to be set in environment variables:
- LIGHTDASH_INSTANCE_URL
- LIGHTDASH_ACCESS_TOKEN
- LIGHTDASH_PROJECT_UUID
"""
import os
import pytest
import logging
from lightdash import Client
from lightdash.models import Model
from lightdash.metrics import Metric
from lightdash.dimensions import Dimension


# Configure logging for tests
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def get_required_env_var(name: str) -> str:
    """Get an environment variable or raise a helpful error."""
    value = os.getenv(name)
    if not value:
        raise ValueError(
            f"Missing required environment variable: {name}. "
            "Please set this before running acceptance tests."
        )
    return value


@pytest.fixture
def client_params() -> dict:
    """Get the client parameters from environment variables."""
    return {
        "instance_url": get_required_env_var("LIGHTDASH_INSTANCE_URL"),
        "access_token": get_required_env_var("LIGHTDASH_ACCESS_TOKEN"),
        "project_uuid": get_required_env_var("LIGHTDASH_PROJECT_UUID"),
    }


@pytest.fixture
def client(client_params) -> Client:
    """Create a Client instance."""
    return Client(**client_params)


@pytest.fixture
def first_model(client) -> Model:
    """Get the first available model for testing."""
    models = client.list_models()
    if not models:
        pytest.skip("No models available for testing")
    return models[0]


def test_client_initialization(client_params):
    """Test that the client initializes correctly with the provided credentials."""
    client = Client(**client_params)
    assert client.instance_url == client_params["instance_url"].rstrip('/')
    assert client.access_token == client_params["access_token"]
    assert client.project_uuid == client_params["project_uuid"]


def test_list_models(client):
    """Test that we can list models from the API."""
    models = client.list_models()
    assert isinstance(models, list)
    if models:  # If there are any models
        model = models[0]
        assert isinstance(model, Model)
        assert isinstance(model.name, str)
        assert model.label is None or isinstance(model.label, str)
        assert isinstance(model.type, str)
        assert isinstance(model.database_name, str)
        assert isinstance(model.schema_name, str)
        assert model.description is None or isinstance(model.description, str)


def test_model_attribute_access(client_params):
    """Test that we can access models as attributes."""
    client = Client(**client_params)
    
    # First, list all models to get a name we can use
    models = client.list_models()
    if not models:
        pytest.skip("No models available to test attribute access")
    
    # Get the first model's name
    model_name = models[0].name
    
    # Access the same model via attribute
    model = getattr(client.models, model_name)
    assert isinstance(model, Model)
    assert model.name == model_name
    
    # Verify that accessing a non-existent model raises AttributeError
    with pytest.raises(AttributeError):
        getattr(client.models, "non_existent_model")


def test_model_caching(client_params):
    """Test that models are cached after first access."""
    client = Client(**client_params)
    
    # First access should fetch from API
    models1 = client.list_models()
    if not models1:
        pytest.skip("No models available to test caching")
    
    # Second access should use cache
    models2 = client.list_models()
    
    # Both lists should be identical
    assert len(models1) == len(models2)
    assert all(m1.name == m2.name for m1, m2 in zip(models1, models2))
    
    # Access via attribute should also use cache
    model = getattr(client.models, models1[0].name)
    assert model.name == models1[0].name


def test_list_metrics(first_model):
    """Test that we can list metrics for a model."""
    metrics = first_model.list_metrics()
    assert isinstance(metrics, list)
    if metrics:  # If there are any metrics
        metric = metrics[0]
        assert isinstance(metric, Metric)
        assert isinstance(metric.name, str)
        assert metric.label is None or isinstance(metric.label, str)
        assert metric.description is None or isinstance(metric.description, str)


def test_metrics_caching(first_model):
    """Test that metrics are cached after first access."""
    # First call should fetch from API
    metrics1 = first_model.metrics.list()
    if not metrics1:
        pytest.skip("No metrics available to test caching")
    
    # Second call should use cache
    metrics2 = first_model.metrics.list()
    
    # Both lists should be identical
    assert len(metrics1) == len(metrics2)
    assert all(m1.name == m2.name for m1, m2 in zip(metrics1, metrics2))


def test_metric_attribute_access(first_model):
    """Test that we can access metrics as attributes."""
    # First, list all metrics to get a name we can use
    metrics = first_model.list_metrics()
    if not metrics:
        pytest.skip("No metrics available to test attribute access")
    
    # Get the first metric's name
    metric_name = metrics[0].name
    
    # Access the same metric via attribute
    metric = getattr(first_model.metrics, metric_name)
    assert isinstance(metric, Metric)
    assert metric.name == metric_name
    
    # Verify that accessing a non-existent metric raises AttributeError
    with pytest.raises(AttributeError):
        getattr(first_model.metrics, "non_existent_metric")


def test_list_dimensions(first_model):
    """Test that we can list dimensions for a model."""
    dimensions = first_model.list_dimensions()
    assert isinstance(dimensions, list)
    if dimensions:  # If there are any dimensions
        dimension = dimensions[0]
        assert isinstance(dimension, Dimension)
        assert isinstance(dimension.name, str)
        assert dimension.label is None or isinstance(dimension.label, str)
        assert dimension.description is None or isinstance(dimension.description, str)


def test_dimensions_caching(first_model):
    """Test that dimensions are cached after first access."""
    # First call should fetch from API
    dimensions1 = first_model.dimensions.list()
    if not dimensions1:
        pytest.skip("No dimensions available to test caching")
    
    # Second call should use cache
    dimensions2 = first_model.dimensions.list()
    
    # Both lists should be identical
    assert len(dimensions1) == len(dimensions2)
    assert all(d1.name == d2.name for d1, d2 in zip(dimensions1, dimensions2))


def test_dimension_attribute_access(first_model):
    """Test that we can access dimensions as attributes."""
    # First, list all dimensions to get a name we can use
    dimensions = first_model.list_dimensions()
    if not dimensions:
        pytest.skip("No dimensions available to test attribute access")
    
    # Get the first dimension's name
    dimension_name = dimensions[0].name
    
    # Access the same dimension via attribute
    dimension = getattr(first_model.dimensions, dimension_name)
    assert isinstance(dimension, Dimension)
    assert dimension.name == dimension_name
    
    # Verify that accessing a non-existent dimension raises AttributeError
    with pytest.raises(AttributeError):
        getattr(first_model.dimensions, "non_existent_dimension")


def test_dimensions_require_client(client_params):
    """Test that dimensions cannot be fetched without a client reference."""
    # Create a model without a client reference
    model = Model(
        name="test_model",
        type="default",
        database_name="test_db",
        schema_name="test_schema"
    )
    
    # Attempting to list dimensions should raise an error
    with pytest.raises(RuntimeError, match="Model not properly initialized with client reference"):
        model.dimensions.list()


def test_query_execution(first_model):
    """Test that we can execute queries against a model."""
    # Get first available dimension and metric
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Execute query with both field IDs and objects
    results = first_model.query(
        dimensions=[dimensions[0], dimensions[0].field_id] if len(dimensions) > 1 else [dimensions[0]],
        metrics=[metrics[0], metrics[0].field_id] if len(metrics) > 1 else [metrics[0]],
        limit=10
    ).to_records()
    
    # Verify results structure
    assert isinstance(results, list)
    if results:  # If any results returned
        row = results[0]
        assert isinstance(row, dict)
        # Verify labels are in results (fallback to name if no label)
        dim_label = dimensions[0].label or dimensions[0].name
        metric_label = metrics[0].label or metrics[0].name
        assert dim_label in row
        assert metric_label in row


def test_query_with_field_ids(first_model):
    """Test that we can execute queries using field IDs directly."""
    # Get first available dimension and metric to get their field IDs
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Get field IDs and labels
    dim_field_id = dimensions[0].field_id
    metric_field_id = metrics[0].field_id
    dim_label = dimensions[0].label or dimensions[0].name
    metric_label = metrics[0].label or metrics[0].name
    
    # Execute query using field IDs as strings
    results = first_model.query(
        dimensions=[dim_field_id],
        metrics=[metric_field_id],
        limit=10
    ).to_records()
    
    # Verify results structure
    assert isinstance(results, list)
    if results:  # If any results returned
        row = results[0]
        assert isinstance(row, dict)
        # Verify labels are in results
        assert dim_label in row
        assert metric_label in row


def test_query_limit_validation(first_model):
    """Test that query limits are properly validated."""
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Test invalid limits
    with pytest.raises(ValueError, match="Limit must be between 1 and 5000"):
        first_model.query(
            dimensions=[dimensions[0].field_id],
            metrics=[metrics[0].field_id],
            limit=0
        ).to_records()
    
    with pytest.raises(ValueError, match="Limit must be between 1 and 5000"):
        first_model.query(
            dimensions=[dimensions[0].field_id],
            metrics=[metrics[0].field_id],
            limit=5001
        ).to_records()


def test_query_requires_client(client_params):
    """Test that queries cannot be executed without a client reference."""
    model = Model(
        name="test_model",
        type="default",
        database_name="test_db",
        schema_name="test_schema"
    )
    
    with pytest.raises(RuntimeError, match="Model not properly initialized with client reference"):
        model.query(
            dimensions=["test_model_dimension"],
            metrics=["test_model_metric"]
        ).to_records()


def test_metric_field_id():
    """Test that metrics generate correct field IDs."""
    metric = Metric(
        name="revenue",
        model_name="orders",
        label="Revenue",
        description="Total revenue"
    )
    assert metric.field_id == "orders_revenue"


def test_dimension_field_id():
    """Test that dimensions generate correct field IDs."""
    dimension = Dimension(
        name="email",
        model_name="users",
        label="Email",
        description="User email"
    )
    assert dimension.field_id == "users_email"


def test_query_to_df_no_results():
    """Test that to_df raises an error when no query has been executed."""
    model = Model(
        name="test_model",
        type="default",
        database_name="test_db",
        schema_name="test_schema"
    )
    
    with pytest.raises(RuntimeError, match="Model not properly initialized with client reference"):
        model.query(
            dimensions=["test_model_dimension"],
            metrics=["test_model_metric"]
        ).to_df()


def test_query_to_json_no_results():
    """Test that to_json raises an error when no query has been executed."""
    model = Model(
        name="test_model",
        type="default",
        database_name="test_db",
        schema_name="test_schema"
    )
    
    with pytest.raises(RuntimeError, match="Model not properly initialized with client reference"):
        model.query(
            dimensions=["test_model_dimension"],
            metrics=["test_model_metric"]
        ).to_json()


def test_query_to_df_pandas(first_model):
    """Test converting query results to a pandas DataFrame."""
    try:
        import pandas as pd
    except ImportError:
        pytest.skip("pandas not installed")

    # Get first available dimension and metric
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Get labels for verification
    dim_label = dimensions[0].label or dimensions[0].name
    metric_label = metrics[0].label or metrics[0].name
    
    # Execute query and convert to DataFrame
    df = first_model.query(
        dimensions=[dimensions[0]],
        metrics=[metrics[0]],
        limit=10
    ).to_df()
    
    # Verify DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 10  # Check limit is respected
    assert list(df.columns) == [dim_label, metric_label]


def test_query_to_df_polars(first_model):
    """Test converting query results to a polars DataFrame."""
    try:
        import polars as pl
    except ImportError:
        pytest.skip("polars not installed")

    # Get first available dimension and metric
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Get labels for verification
    dim_label = dimensions[0].label or dimensions[0].name
    metric_label = metrics[0].label or metrics[0].name
    
    # Execute query and convert to DataFrame
    df = first_model.query(
        dimensions=[dimensions[0]],
        metrics=[metrics[0]],
        limit=10
    ).to_df(backend="polars")
    
    # Verify DataFrame structure
    assert isinstance(df, pl.DataFrame)
    assert len(df) <= 10  # Check limit is respected
    assert list(df.columns) == [dim_label, metric_label]


def test_query_to_df_invalid_backend(first_model):
    """Test that to_df raises an error for invalid backends."""
    # Get first available dimension and metric
    dimensions = first_model.list_dimensions()
    metrics = first_model.list_metrics()
    
    if not dimensions or not metrics:
        pytest.skip("No dimensions or metrics available for testing")
    
    # Execute query
    with pytest.raises(ValueError, match="Unsupported DataFrame backend"):
        first_model.query(
            dimensions=[dimensions[0]],
            metrics=[metrics[0]],
            limit=10
        ).to_df(backend="invalid") 