#!/bin/bash
# Run all 10 queries on all 3 backends and print comparison table

API="http://localhost:8000"

echo "============================================"
echo "  Cross-Backend Query Comparison"
echo "============================================"
echo ""

printf "%-6s | %-12s | %-12s | %-12s | %-40s\n" "Query" "PostgreSQL" "MongoDB" "Neo4j" "Name"
printf "%-6s-+-%-12s-+-%-12s-+-%-12s-+-%-40s\n" "------" "------------" "------------" "------------" "----------------------------------------"

TOTAL_PG=0
TOTAL_MONGO=0
TOTAL_NEO4J=0
SUCCESS=0
FAIL=0

for Q in Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10; do
    PG_TIME="-"
    MONGO_TIME="-"
    NEO4J_TIME="-"
    NAME=""

    # Get query name
    NAME=$(curl -sf "$API/api/v1/queries" | python -c "
import sys, json
for q in json.load(sys.stdin)['queries']:
    if q['query_id'] == '$Q':
        print(q['name'][:40])
        break
" 2>/dev/null || echo "Unknown")

    for BACKEND in postgres mongodb neo4j; do
        RESULT=$(curl -sf -X POST "$API/api/v1/query" \
            -H "Content-Type: application/json" \
            -d "{\"query_id\": \"$Q\", \"backend\": \"$BACKEND\"}" 2>/dev/null)

        if [ $? -eq 0 ]; then
            TIME=$(echo "$RESULT" | python -c "import sys,json; print(json.load(sys.stdin)['execution_time_ms'])" 2>/dev/null || echo "err")
            ROWS=$(echo "$RESULT" | python -c "import sys,json; print(json.load(sys.stdin)['row_count'])" 2>/dev/null || echo "0")

            case $BACKEND in
                postgres)  PG_TIME="${TIME}ms (${ROWS}r)"; TOTAL_PG=$(python -c "print($TOTAL_PG + $TIME)" 2>/dev/null || echo $TOTAL_PG) ;;
                mongodb)   MONGO_TIME="${TIME}ms (${ROWS}r)"; TOTAL_MONGO=$(python -c "print($TOTAL_MONGO + $TIME)" 2>/dev/null || echo $TOTAL_MONGO) ;;
                neo4j)     NEO4J_TIME="${TIME}ms (${ROWS}r)"; TOTAL_NEO4J=$(python -c "print($TOTAL_NEO4J + $TIME)" 2>/dev/null || echo $TOTAL_NEO4J) ;;
            esac
            SUCCESS=$((SUCCESS + 1))
        else
            FAIL=$((FAIL + 1))
            case $BACKEND in
                postgres) PG_TIME="FAIL" ;;
                mongodb) MONGO_TIME="FAIL" ;;
                neo4j) NEO4J_TIME="FAIL" ;;
            esac
        fi
    done

    printf "%-6s | %-12s | %-12s | %-12s | %-40s\n" "$Q" "$PG_TIME" "$MONGO_TIME" "$NEO4J_TIME" "$NAME"
done

echo ""
printf "%-6s | %-12s | %-12s | %-12s\n" "TOTAL" "${TOTAL_PG}ms" "${TOTAL_MONGO}ms" "${TOTAL_NEO4J}ms"
echo ""
echo "Successful: $SUCCESS / $((SUCCESS + FAIL)) queries"

# Graph-exclusive queries
echo ""
echo "--- Graph-Exclusive Queries (Neo4j only) ---"
for Q in Q11 Q12 Q13; do
    RESULT=$(curl -sf -X POST "$API/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{\"query_id\": \"$Q\", \"backend\": \"neo4j\"}" 2>/dev/null)
    if [ $? -eq 0 ]; then
        TIME=$(echo "$RESULT" | python -c "import sys,json; print(json.load(sys.stdin)['execution_time_ms'])" 2>/dev/null || echo "err")
        ROWS=$(echo "$RESULT" | python -c "import sys,json; print(json.load(sys.stdin)['row_count'])" 2>/dev/null || echo "0")
        echo "  $Q: ${TIME}ms, ${ROWS} rows"
    else
        echo "  $Q: FAILED"
    fi
done
echo ""
echo "Done."
