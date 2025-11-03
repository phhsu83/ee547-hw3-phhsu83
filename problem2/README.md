# Analysis

1. Schema Design Decisions:
 - Why did you choose your partition key structure?
     - I chose the partition key based on the primary access patterns. Since most queries retrieve papers by category and date, I used PK = CATEGORY#<name> and SK = DATE#<date>#<arxiv_id>. This ensures even data distribution and allows efficient range queries.
 - How many GSIs did you create and why?
     - I created 3 GSIs. GSI1 uses author_name as the partition key to support queries like “get all papers by an author”. GSI2 uses arxiv id to query single paper information. GSI3 uses keyword to allow keyword-based search. I avoided unnecessary GSIs to reduce write cost and index maintenance overhead.
 - What denormalization trade-offs did you make?
     - Paper metadata is duplicated under category, author, and keyword items. This eliminates joins and makes lookups O(1), but introduces update complexity and data redundancy, which accept for performance.

2. Denormalization Analysis:
 - Average number of DynamoDB items per paper
     - Average number of DynamoDB items = number of categories + number of authors + number of keywords (per paper)
 - Storage multiplication factor
     - Storage factor = number of DynamoDB items / number of papers
                      = 140 / 10
 - Which access patterns caused the most duplication?
     - The largest contributor to duplication is keyword-based access, followed by author-based queries, because each of them requires a separate item per paper.

3. Query Limitations:
 - What queries are NOT efficiently supported by your schema?
    - Although DynamoDB schema is optimized for common access patterns (e.g., retrieving recent papers by category, querying papers by author, or fetching a paper by ID), certain types of queries are not efficiently supported. Examples include: “count total number of papers per author,” “find the globally most cited papers,” or “calculate the number of papers in each category.”
 - Why are these difficult in DynamoDB?
     - These queries require scanning across all partition keys and performing aggregation, but DynamoDB does not natively support SQL-style COUNT(*), GROUP BY, or global sorting. As a result, such queries would require full table scans, which are inefficient and unsuitable for real-time analytics.

4. When to Use DynamoDB:
 - Based on this exercise, when would you choose DynamoDB over PostgreSQL?
     - From this exercise, it becomes clear that DynamoDB is a better choice when access patterns are well-defined, the workload is read-heavy, and scalability is a priority. In a paper retrieval system, if the application mainly performs lookups like “latest papers in a category,” “papers by a specific author,” or “get paper by ID,” without requiring complex joins or ad-hoc analytics, DynamoDB provides lower latency and automatic scaling compared to PostgreSQL.
 - What are the key trade-offs?
     - DynamoDB excels in high-volume, predictable access patterns with low-latency requirements, while PostgreSQL is more suitable for systems requiring flexible querying, relational integrity, and analytics.

5. EC2 Deployment:
 - Your EC2 instance public IP
     - 54.91.208.68 
 - IAM role ARN used
     - arn:aws:iam::xxxxxxxxxxxx:role/ee547-hw3-iam-role
 - Any challenges encountered during 
     - Every AWS feature feels like a challenge for me since I don’t use the platform often and am not very familiar with its functions. On top of that, some features or settings can cost money, which makes me reluctant to try or practice them freely.
