import json

def test_create_right_event(test_app_with_db):
    data = {
        "client.user_id": -1,
        "direction": -1,
        "timestamp": -1,
        "size": -1,
        "time.backend": -1,
        "status": -1
    }
    
    response = test_app_with_db.post(
        "/ingest/", data=json.dumps(data)
    )

    assert response.status_code == 201

def test_create_wrong_event(test_app_with_db):
    # without client.user_id
    data = {
        "direction": -1,
        "timestamp": -1,
        "size": -1,
        "time.backend": -1,
        "status": -1
    }

    response = test_app_with_db.post(
        "/ingest/", data=json.dumps(data)
    )

    assert response.status_code == 406