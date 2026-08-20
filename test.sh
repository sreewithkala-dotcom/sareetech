#!/bin/bash
# Test script for Silk ERP System
# Usage: ./test.sh

set -e

echo "=== Silk ERP System Test Suite ==="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test 1: Check if Docker is running
echo "Test 1: Checking Docker..."
if docker info > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Docker is running"
else
    echo -e "${RED}✗${NC} Docker is not running"
    exit 1
fi

# Test 2: Check if docker-compose is available
echo "Test 2: Checking docker-compose..."
if docker-compose --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} docker-compose is available"
else
    echo -e "${RED}✗${NC} docker-compose is not available"
    exit 1
fi

# Test 3: Start services
echo "Test 3: Starting services with docker-compose..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Test 4: Check PostgreSQL
echo "Test 4: Checking PostgreSQL..."
if docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} PostgreSQL is ready"
else
    echo -e "${RED}✗${NC} PostgreSQL is not ready"
fi

# Test 5: Check Kafka
echo "Test 5: Checking Kafka..."
if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Kafka is ready"
else
    echo -e "${RED}✗${NC} Kafka is not ready"
fi

# Test 6: Check Redis
echo "Test 6: Checking Redis..."
if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Redis is ready"
else
    echo -e "${RED}✗${NC} Redis is not ready"
fi

# Test 7: Check Auth Service
echo "Test 7: Checking Auth Service..."
if curl -s http://localhost:5000/api/v1/auth/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Auth Service is ready"
else
    echo -e "${RED}✗${NC} Auth Service is not ready"
fi

# Test 8: Check Scanner Service
echo "Test 8: Checking Scanner Service..."
if curl -s http://localhost:5001/api/v1/scanner/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Scanner Service is ready"
else
    echo -e "${RED}✗${NC} Scanner Service is not ready"
fi

# Test 9: Check AI Service
echo "Test 9: Checking AI Service..."
if curl -s http://localhost:5002/api/v1/ai/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} AI Service is ready"
else
    echo -e "${RED}✗${NC} AI Service is not ready"
fi

# Test 10: Check Workflow Service
echo "Test 10: Checking Workflow Service..."
if curl -s http://localhost:5003/api/v1/workflow/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Workflow Service is ready"
else
    echo -e "${RED}✗${NC} Workflow Service is not ready"
fi

# Test 11: Check IoT Service
echo "Test 11: Checking IoT Service..."
if curl -s http://localhost:5004/api/v1/iot/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} IoT Service is ready"
else
    echo -e "${RED}✗${NC} IoT Service is not ready"
fi

# Test 12: Check Design Service
echo "Test 12: Checking Design Service..."
if curl -s http://localhost:5005/api/v1/design/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Design Service is ready"
else
    echo -e "${RED}✗${NC} Design Service is not ready"
fi

# Test 13: Check Buy-Back Service
echo "Test 13: Checking Buy-Back Service..."
if curl -s http://localhost:5006/api/v1/buyback/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Buy-Back Service is ready"
else
    echo -e "${RED}✗${NC} Buy-Back Service is not ready"
fi

# Test 14: Check Guild Service
echo "Test 14: Checking Guild Service..."
if curl -s http://localhost:5007/api/v1/guilds/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Guild Service is ready"
else
    echo -e "${RED}✗${NC} Guild Service is not ready"
fi

# Test 15: Check Localization Service
echo "Test 15: Checking Localization Service..."
if curl -s http://localhost:5008/api/v1/i18n/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Localization Service is ready"
else
    echo -e "${RED}✗${NC} Localization Service is not ready"
fi

# Test 16: Check Frontend
echo "Test 16: Checking Frontend..."
if curl -s http://localhost:3000 > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Frontend is ready"
else
    echo -e "${RED}✗${NC} Frontend is not ready"
fi

echo ""
echo "=== Test Summary ==="
echo "All services started. Access the application at:"
echo "  Frontend: http://localhost:3000"
echo "  Auth API: http://localhost:5000"
echo "  Scanner API: http://localhost:5001"
echo "  AI API: http://localhost:5002"
echo "  Workflow API: http://localhost:5003"
echo "  IoT API: http://localhost:5004"
echo "  Design API: http://localhost:5005"
echo "  Buy-Back API: http://localhost:5006"
echo "  Guild API: http://localhost:5007"
echo "  Localization API: http://localhost:5008"
echo ""
echo "Default login (if seeded):"
echo "  Email: admin@factory.com"
echo "  Password: admin123"
echo "  Factory: FACT-BLR-01"
