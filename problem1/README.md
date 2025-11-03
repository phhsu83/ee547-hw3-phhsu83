# Analysis
1. Schema Decisions: Natural vs surrogate keys? Why?
 - lines: surrogate keys, stops: surrogate keys, line_stops: surrogate keys, trips: natural keys, stop_events: surrogate keys
 - If the natural key is stable, never changes, and uniquely identifies a record, it can serve as the primary key. If it may change, is too long, or consists of multiple fields, a surrogate key is preferred for simplicity and better relational performance.

2. Constraints: What CHECK/UNIQUE constraints did you add?
 - stops: 
     - UNIQUE (stop_name, latitude, longitude)
 - line_stops: 
     - CHECK (sequence_number >= 1)
     - CHECK (time_offset_minutes >= 0)
 - stop_events:
     - CHECK (passengers_on >= 0)
     - CHECK (passengers_off >= 0)

3. Complex Query: Which query was hardest? Why?
 - Question 10.
 - It required calculating total ridership per stop and then comparing each value against the average ridership across all stops. This cannot be done in a single GROUP BY query, so I had to use a nested (two-level) subquery to compute the global average first. Managing aggregation inside aggregation made this question more complex than others.

4. Foreign Keys: Give example of invalid data they prevent
 - Foreign keys ensure that data in one table must correspond to valid data in another table.
 - Example: The column trips.line_id references lines.line_id. Without this foreign key, someone could insert a trip that runs on a non-existent line (e.g., line_id = 999).

5. When Relational: Why is SQL good for this domain?
 - Public transit data is highly structured and naturally relational. Entities such as lines, stops, trips, and stop events have clear 1-to-many and many-to-many relationships, which are best modeled using tables with primary and foreign keys.
 - In addition, this homework requires complex queries: finding all stops on a line, computing ridership per stop, identifying delays, or joining trips with stop events. These aggregate and join operations are exactly what SQL is optimized for.

