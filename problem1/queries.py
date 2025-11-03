import argparse
import json
import psycopg2
import sys

def parse_args():
    # python queries.py --query Q1 --dbname transit --format json
    # python queries.py --all --dbname transit
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--query", help="Run a specific query (e.g., Q1)")
    group.add_argument("--all", action="store_true", help="Run all queries")

    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--format", default="json", help="Output format")

    return parser.parse_args()

queries = {
    "Q1": {
        "description": "List all stops on Route 20 in order",
        "sql": """
            SELECT s.stop_name, ls.sequence_number, ls.time_offset_minutes
            FROM line_stops ls
            JOIN lines l ON ls.line_id = l.line_id
            JOIN stops s ON ls.stop_id = s.stop_id
            WHERE l.line_name = 'Route 20'
            ORDER BY ls.sequence_number;
        """
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": """
            SELECT t.trip_id, l.line_name, t.departure_time
            FROM trips t
            JOIN lines l ON t.line_id = l.line_id
            WHERE t.departure_time::time BETWEEN '07:00:00' AND '09:00:00'
            ORDER BY t.departure_time, t.trip_id;
        """
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": """
            SELECT s.stop_name, COUNT(*) AS line_count
            FROM line_stops ls
            JOIN lines l ON ls.line_id = l.line_id
            JOIN stops s ON ls.stop_id = s.stop_id
            GROUP BY s.stop_name
            HAVING COUNT(*) > 1;
        """
    },
    "Q4": {
        "description": "Complete route for trip T0001",
        "sql": """
            SELECT s.stop_name
            FROM trips t
            JOIN lines l ON t.line_id = l.line_id
            JOIN line_stops ls ON l.line_id = ls.line_id
            JOIN stops s ON ls.stop_id = s.stop_id
            WHERE t.trip_id = 'T0001'
            ORDER BY ls.sequence_number;
        """
    },
    "Q5": {
        "description": "Routes serving both Wilshire / Veteran and Le Conte / Broxton",
        "sql": """
            SELECT l.line_name
            FROM line_stops ls
            JOIN lines l ON ls.line_id = l.line_id
            JOIN stops s ON ls.stop_id = s.stop_id
            WHERE s.stop_name IN ('Wilshire / Veteran', 'Le Conte / Broxton')
            GROUP BY l.line_name
            HAVING COUNT(DISTINCT s.stop_name) = 2;
        """
    },
    "Q6": {
        "description": "Average ridership by line",
        "sql": """
            SELECT l.line_name, AVG(se.passengers_on) AS avg_passengers
            FROM stop_events se
            JOIN trips t ON se.trip_id = t.trip_id
            JOIN lines l ON t.line_id = l.line_id
            GROUP BY l.line_name;
        """
    },
    "Q7": {
        "description": "Top 10 busiest stops",
        "sql": """
            SELECT s.stop_name, SUM(passengers_on + passengers_off) AS total_activity
            FROM stop_events se
            JOIN stops s ON se.stop_id = s.stop_id
            GROUP BY s.stop_name
            ORDER BY total_activity DESC
            LIMIT 10;
        """
    },
    "Q8": {
        "description": "Count delays by line (>2 min late)",
        "sql": """
            SELECT l.line_name, COUNT(*) AS delay_count
            FROM stop_events se
            JOIN trips t ON se.trip_id = t.trip_id
            JOIN lines l ON t.line_id = l.line_id
            WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
            GROUP BY l.line_name;
        """
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops",
        "sql": """
            SELECT se.trip_id, COUNT(*) AS delayed_stop_count
            FROM stop_events se
            WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
            GROUP BY se.trip_id
            HAVING COUNT(*) > 2;
        """
    },
    "Q10": {
        "description": "Stops with above-average ridership",
        "sql": """
            SELECT s.stop_name, SUM(se.passengers_on) AS total_boardings
            FROM stop_events se
            JOIN stops s ON se.stop_id = s.stop_id
            GROUP BY s.stop_name
            HAVING SUM(se.passengers_on) > (
                SELECT AVG(total_boardings) 
                FROM (
                    SELECT SUM(passengers_on) AS total_boardings
                    FROM stop_events
                    GROUP BY stop_id
                ) AS sub
            );
        """
    }
}

def run_query(conn, query_name):
    if query_name not in queries:
        raise ValueError(f"Unknown query name: {query_name} (Q1 - Q10)")
    
    qinfo = queries[query_name]
    sql = qinfo["sql"]

    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    results = [dict(zip(cols, row)) for row in rows]

    output = {
        "query": query_name,
        "description": qinfo["description"],
        "results": results,
        "count": len(results)
    }

    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))

def run_all(conn):
    for qname in queries:
        run_query(conn, qname)



def main():
    
    args = parse_args()

    # 連線資料庫
    conn = psycopg2.connect(dbname=args.dbname)

    if args.query:
        run_query(conn, args.query)
    elif args.all:
        run_all(conn)

    conn.close()

if __name__ == "__main__":
    main()
