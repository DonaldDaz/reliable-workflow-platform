#!/usr/bin/env python3
"""Exercise the full workflow and three failure modes against the running stack."""

import json
import os
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("API_URL", "http://localhost:8092/api")


def call(method, path, body=None, content_type="application/json"):
    data = None if body is None else body
    if isinstance(body, (dict, list)):
        data = json.dumps(body).encode()
    request = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": content_type},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise AssertionError(f"{method} {path}: HTTP {error.code} {error.read().decode()}") from error


def until(description, predicate, timeout=35):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.4)
    raise AssertionError(f"Timed out waiting for {description}")


def view(practice_id):
    return next((item for item in call("GET", "/dashboard") if item["id"] == practice_id), None)


def upload(practice_id):
    boundary = "boundary" + uuid.uuid4().hex
    content = b"Synthetic evidence for the workflow recovery test."
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"evidence.txt\"\r\n"
        "Content-Type: text/plain\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return call("POST", f"/practices/{practice_id}/documents", body, f"multipart/form-data; boundary={boundary}")


def expect_conflict(path):
    request = urllib.request.Request(BASE + path, data=b"{}", method="POST", headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error:
        assert error.code == 409, f"Expected HTTP 409, got {error.code}"
    else:
        raise AssertionError("Invalid transition was accepted")


def main():
    call("POST", "/dashboard/pause", {})
    until("consumer pause", lambda: call("GET", "/dashboard/health")["consumerPaused"])

    practice_id = call("POST", "/practices", {"title": "Recovery check " + uuid.uuid4().hex[:8]})["id"]
    expect_conflict(f"/practices/{practice_id}/approve")
    until("outbox publication", lambda: call("GET", "/dashboard/health")["pendingOutbox"] == 0)
    assert call("GET", f"/practices/{practice_id}")["status"] == "CREATED"
    assert view(practice_id) is None, "A paused consumer must leave the projection behind"
    print("PASS: stopped consumer leaves PostgreSQL state available and MongoDB behind")

    call("POST", "/dashboard/resume", {})
    until("consumer resume", lambda: not call("GET", "/dashboard/health")["consumerPaused"])
    until("created projection", lambda: (view(practice_id) or {}).get("lastSequence") == 1)

    before = call("GET", "/dashboard/health")["injectedFailures"]
    call("POST", "/dashboard/fail-next", {})
    upload(practice_id)
    until("injected failure", lambda: call("GET", "/dashboard/health")["injectedFailures"] > before)
    until("retry success", lambda: (view(practice_id) or {}).get("lastSequence") == 2)
    assert view(practice_id)["documents"] == ["evidence.txt"]
    print("PASS: transient consumer failure is retried without duplicating the document")

    call("POST", f"/practices/{practice_id}/approve", {})
    until("approval projection", lambda: (view(practice_id) or {}).get("lastSequence") == 3)
    assert call("GET", f"/practices/{practice_id}")["status"] == "APPROVED"
    assert [event["type"] for event in view(practice_id)["events"]] == [
        "PracticeCreated", "DocumentUploaded", "PracticeApproved"
    ]
    print("PASS: creation, upload, approval, and dashboard history agree")

    duplicates_before = call("GET", "/dashboard/health")["duplicateDeliveries"]
    call("POST", f"/dashboard/duplicate-last/{practice_id}", {})
    until("duplicate delivery", lambda: call("GET", "/dashboard/health")["duplicateDeliveries"] > duplicates_before)
    assert len(view(practice_id)["events"]) == 3
    print("PASS: a repeated Kafka record does not duplicate projected history")

    result = call("POST", "/dashboard/rebuild", {})
    rebuilt = view(practice_id)
    assert result["replayedEvents"] >= 3
    assert rebuilt["lastSequence"] == 3 and rebuilt["status"] == "APPROVED"
    assert len(rebuilt["events"]) == 3
    print("PASS: MongoDB projection rebuilt from retained Kafka events")


if __name__ == "__main__":
    main()
