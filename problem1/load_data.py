import argparse
import psycopg2
import csv
import os


def parse_args():
    # python load_data.py --host localhost --dbname transit --user postgres --password pass --datadir data/
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--datadir", default="data", help="Directory containing CSVs")
    
    return p.parse_args()

def connect_PostgreSQL(args):
    conn = psycopg2.connect(
        host=args.host,
        dbname=args.dbname, 
        user=args.user, 
        password=args.password
    )

    conn.autocommit = False  # we control transactions
    
    print(f"Connected to {args.dbname}@{args.host}")

    return conn

def run_schema(conn):
    print("Creating schema...")
    with open("schema.sql", "r", encoding="utf-8") as f:
        ddl = f.read()
    
    with conn.cursor() as cur:
        cur.execute(ddl)
    
    conn.commit()

    print("Tables created: lines, stops, line_stops, trips, stop_events")


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    return rows # list[dict[str, str]]


def load_lines(conn, data_dir):
    path = os.path.join(data_dir, "lines.csv")
    rows = read_csv(path)

    sql = """
    INSERT INTO lines (line_name, vehicle_type)
    VALUES (%s, %s)
    ON CONFLICT (line_name)
    DO UPDATE SET vehicle_type = EXCLUDED.vehicle_type
    """

    with conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            line_name = r.get("line_name")
            vehicle_type = r.get("vehicle_type")

            cur.execute(sql, (line_name, vehicle_type))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lines")
        total = cur.fetchone()[0]

    print(f"/nLoading data/lines.csv... {total} rows")


def load_stops(conn, data_dir):
    path = os.path.join(data_dir, "stops.csv")
    rows = read_csv(path)

    sql = """
    INSERT INTO stops (stop_name, latitude, longitude)
    VALUES (%s, %s, %s)
    ON CONFLICT (stop_name, latitude, longitude)
    DO UPDATE SET latitude = EXCLUDED.latitude,
                  longitude = EXCLUDED.longitude
    """

    with conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            stop_name = r.get("stop_name")
            latitude = float(r.get("latitude", 0))
            longitude = float(r.get("longitude", 0))

            cur.execute(sql, (stop_name, latitude, longitude))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stops")
        total = cur.fetchone()[0]

    print(f"Loading data/stops.csv... {total} rows")


def load_line_stops(conn, data_dir):
    path = os.path.join(data_dir, "line_stops.csv")
    rows = read_csv(path)

    with conn.cursor() as cur:
        # 建立 line_name -> line_id 映射
        line_cache = {}
        stop_cache = {}
        with conn.cursor() as cur:
            cur.execute("SELECT line_id, line_name FROM lines")
            for line_id, line_name in cur.fetchall():
                line_cache[line_name] = line_id

            cur.execute("SELECT stop_id, stop_name FROM stops")
            for stop_id, stop_name in cur.fetchall():
                stop_cache[stop_name] = stop_id
    
    sql = """
    INSERT INTO line_stops (line_id, stop_id, sequence_number, time_offset_minutes)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (line_id, sequence_number)
    DO UPDATE SET stop_id = EXCLUDED.stop_id,
                  time_offset_minutes = EXCLUDED.time_offset_minutes;
    """

    with conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            line_name = r["line_name"]
            stop_name = r["stop_name"]
            seq = int(r.get("sequence", 1))
            offset = int(r.get("time_offset", 0))

            # 用快取查 line_id
            line_id = line_cache[line_name]
            stop_id = stop_cache[stop_name] 
            # if line_id == 1 and seq == 1: stop_id = 1 # problem!!!

            cur.execute(sql, (line_id, stop_id, seq, offset))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM line_stops")
        total = cur.fetchone()[0]

    print(f"Loading data/line_stops.csv... {total} rows")


def load_trips(conn, data_dir):
    path = os.path.join(data_dir, "trips.csv")
    rows = read_csv(path)

    with conn.cursor() as cur:
        # 建立 line_name -> line_id 映射
        line_cache = {}
        with conn.cursor() as cur:
            cur.execute("SELECT line_id, line_name FROM lines")
            for line_id, line_name in cur.fetchall():
                line_cache[line_name] = line_id

    sql = """
    INSERT INTO trips (trip_id, line_id, departure_time, vehicle_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (trip_id)
    DO UPDATE SET line_id = EXCLUDED.line_id,
                  departure_time = EXCLUDED.departure_time,
                  vehicle_id = EXCLUDED.vehicle_id;
    """

    with conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            trip_id = r["trip_id"]
            line_name = r["line_name"]
            departure_time = r["scheduled_departure"]
            vehicle_id = r["vehicle_id"]

            line_id = line_cache[line_name]
            cur.execute(sql, (trip_id, line_id, departure_time, vehicle_id))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trips")
        total = cur.fetchone()[0]

    print(f"Loading data/trips.csv... {total} rows")


def load_stop_events(conn, data_dir, commit_every=1000):
    path = os.path.join(data_dir, "stop_events.csv")
    rows = read_csv(path)

    with conn.cursor() as cur:
        stop_cache = {}
        with conn.cursor() as cur:
            cur.execute("SELECT stop_id, stop_name FROM stops")
            for stop_id, stop_name in cur.fetchall():
                stop_cache[stop_name] = stop_id

    # We enforce referential integrity:
    #   - trip_id must exist
    #   - stop_id must belong to trip's line (via (line_id, stop_id) FK)
    # CSV may or may not provide line_name; if absent we look it up via trip_id.
    get_trip_line_sql = "SELECT line_id FROM trips WHERE trip_id = %s;"
    sql = """
    INSERT INTO stop_events
        (trip_id, stop_id, scheduled_time, actual_time, passengers_on, passengers_off)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            trip_id = r["trip_id"]
            stop_name = r["stop_name"]
            scheduled = r["scheduled"]
            actual = r["actual"]
            pon = int(r.get("passengers_on", 0))
            poff = int(r.get("passengers_off", 0))


            cur.execute(get_trip_line_sql, (trip_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"trip_id {trip_id} not found when loading stop_events")
            line_id = row[0]

            stop_id = stop_cache[stop_name] 

            # This will be validated by FK (line_id, stop_id) -> line_stops
            cur.execute(sql, (trip_id, stop_id, scheduled, actual, pon, poff))

            if i % commit_every == 0:
                conn.commit()
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stop_events")
        total = cur.fetchone()[0]

    print(f"Loading data/stop_events.csv... {total} rows")


    


def report_statistics(conn):
    statistics = 0
    for table in ["lines", "stops", "line_stops", "trips", "stop_events"]:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            statistics += cur.fetchone()[0]

    print(f"Total: {statistics} rows loaded")


def main():

    args = parse_args()
    
    conn = connect_PostgreSQL(args) # Connects to PostgreSQL

    run_schema(conn)

    load_lines(conn, args.datadir)
    load_stops(conn, args.datadir)
    load_line_stops(conn, args.datadir)
    load_trips(conn, args.datadir)
    load_stop_events(conn, args.datadir, commit_every=1000)
    report_statistics(conn)


    conn.close()


if __name__ == "__main__":
    main()