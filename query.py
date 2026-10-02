#!/usr/bin/env python3
"""
query.py — Optional Secondary Backend Entry Point for MongoDB Atlas Query API
==============================================================================
Provides a direct execution interface for the Read-Only MongoDB Query Layer.
Usage:
    python query.py
"""

from query_api import run_query_api_server

if __name__ == "__main__":
    run_query_api_server()
