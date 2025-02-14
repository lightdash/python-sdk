def test_query_with_single_fields(first_model, dimensions, metrics):
    # Test with single metric as string
    results = first_model.query(
        metrics=metrics[0].field_id,
        limit=10
    ).to_records()
    assert len(results) == 10
    assert metrics[0].field_id in results[0]

    # Test with single metric as object
    results = first_model.query(
        metrics=metrics[0],
        limit=10
    ).to_records()
    assert len(results) == 10
    assert metrics[0].field_id in results[0]

    # Test with single dimension as string
    results = first_model.query(
        metrics=metrics[0],
        dimensions=dimensions[0].field_id,
        limit=10
    ).to_records()
    assert len(results) == 10
    assert dimensions[0].field_id in results[0]
    assert metrics[0].field_id in results[0]

    # Test with single dimension as object
    results = first_model.query(
        metrics=metrics[0],
        dimensions=dimensions[0],
        limit=10
    ).to_records()
    assert len(results) == 10
    assert dimensions[0].field_id in results[0]
    assert metrics[0].field_id in results[0] 