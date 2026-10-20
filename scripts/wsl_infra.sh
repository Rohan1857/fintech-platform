#!/usr/bin/env bash
set -e

service postgresql start
service redis-server start

# Configure postgres
su - postgres << 'EOF'
psql -tc "SELECT 1 FROM pg_roles WHERE rolname='ledger'" | grep -q 1 || psql -c "CREATE USER ledger WITH PASSWORD 'ledger_password' SUPERUSER;"
psql -tc "SELECT 1 FROM pg_database WHERE datname='ledger'" | grep -q 1 || psql -c "CREATE DATABASE ledger OWNER ledger;"
EOF

# Verify redis
redis-cli ping

echo "WSL Infrastructure Ready: PostgreSQL 16 & Redis 7 are active."
