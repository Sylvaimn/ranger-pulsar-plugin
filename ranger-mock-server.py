#!/usr/bin/env python3
"""Ranger Admin Mock Server - Simulates Ranger REST API for integration testing."""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json

POLICIES = [
    {
        "id": 1,
        "guid": "mock-policy-1",
        "name": "pulsar-admin-all",
        "service": "pulsar",
        "isEnabled": True,
        "policyItems": [
            {
                "accessTypes": [{"type": "admin"}],
                "users": ["admin"],
                "groups": [],
                "conditions": [],
                "delegateAdmin": True
            }
        ],
        "resources": {
            "cluster": {"values": ["standalone"], "isExcludes": False, "isRecursive": True},
            "namespace": {"values": ["*"], "isExcludes": False, "isRecursive": True},
            "topic": {"values": ["*"], "isExcludes": False, "isRecursive": True},
            "subscription": {"values": ["*"], "isExcludes": False, "isRecursive": True}
        }
    },
    {
        "id": 2,
        "guid": "mock-policy-2",
        "name": "pulsar-producer",
        "service": "pulsar",
        "isEnabled": True,
        "policyItems": [
            {
                "accessTypes": [{"type": "produce"}],
                "users": ["producer1", "admin"],
                "groups": [],
                "conditions": [],
                "delegateAdmin": False
            }
        ],
        "resources": {
            "cluster": {"values": ["standalone"], "isExcludes": False, "isRecursive": True},
            "namespace": {"values": ["public/default"], "isExcludes": False, "isRecursive": True},
            "topic": {"values": ["*"], "isExcludes": False, "isRecursive": True},
            "subscription": {"values": ["*"], "isExcludes": False, "isRecursive": True}
        }
    },
    {
        "id": 3,
        "guid": "mock-policy-3",
        "name": "pulsar-consumer",
        "service": "pulsar",
        "isEnabled": True,
        "policyItems": [
            {
                "accessTypes": [{"type": "consume"}],
                "users": ["consumer1", "admin"],
                "groups": [],
                "conditions": [],
                "delegateAdmin": False
            }
        ],
        "resources": {
            "cluster": {"values": ["standalone"], "isExcludes": False, "isRecursive": True},
            "namespace": {"values": ["public/default"], "isExcludes": False, "isRecursive": True},
            "topic": {"values": ["*"], "isExcludes": False, "isRecursive": True},
            "subscription": {"values": ["*"], "isExcludes": False, "isRecursive": True}
        }
    },
    {
        "id": 4,
        "guid": "mock-policy-4",
        "name": "pulsar-lookup-all",
        "service": "pulsar",
        "isEnabled": True,
        "policyItems": [
            {
                "accessTypes": [{"type": "lookup"}],
                "users": ["*"],
                "groups": [],
                "conditions": [],
                "delegateAdmin": False
            }
        ],
        "resources": {
            "cluster": {"values": ["standalone"], "isExcludes": False, "isRecursive": True},
            "namespace": {"values": ["public/default"], "isExcludes": False, "isRecursive": True},
            "topic": {"values": ["*"], "isExcludes": False, "isRecursive": True},
            "subscription": {"values": ["*"], "isExcludes": False, "isRecursive": True}
        }
    }
]


class RangerMockHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        if '/service/public/v2/api/service' in self.path or '/service/public/policies' in self.path:
            self.wfile.write(json.dumps({"policies": POLICIES, "totalCount": len(POLICIES)}).encode())
        elif '/service/public/v2/api/servicedef' in self.path:
            servicedef = {
                "name": "pulsar",
                "type": "pulsar",
                "accessTypes": [
                    {"name": "produce", "label": "Produce"},
                    {"name": "consume", "label": "Consume"},
                    {"name": "lookup", "label": "Lookup"},
                    {"name": "admin", "label": "Admin"},
                    {"name": "function", "label": "Function"}
                ],
                "resources": [
                    {"name": "cluster", "type": "string", "level": 1},
                    {"name": "namespace", "type": "string", "level": 2},
                    {"name": "topic", "type": "string", "level": 3},
                    {"name": "subscription", "type": "string", "level": 4}
                ]
            }
            self.wfile.write(json.dumps(servicedef).encode())
        else:
            self.wfile.write(json.dumps({
                "status": "running",
                "mock": True,
                "service": "Apache Ranger Admin Mock",
                "policies_count": len(POLICIES)
            }).encode())

    def do_POST(self):
        self.send_response(201)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "created", "mock": True}).encode())

    def do_PUT(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "updated", "mock": True}).encode())

    def log_message(self, format, *args):
        print(f"[Ranger Mock] {args[0]}")


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 6080), RangerMockHandler)
    print("=" * 50)
    print("  Ranger Admin Mock Server")
    print("  Running on http://0.0.0.0:6080")
    print(f"  Loaded {len(POLICIES)} mock policies")
    print("=" * 50)
    server.serve_forever()
