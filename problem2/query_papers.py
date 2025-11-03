import argparse
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import json


dynamodb = boto3.resource("dynamodb")

def parse_args():
    # python query_papers.py recent <category> [--limit 20] [--table TABLE]
    # python query_papers.py author <author_name> [--table TABLE]
    # python query_papers.py get <arxiv_id> [--table TABLE]
    # python query_papers.py daterange <category> <start_date> <end_date> [--table TABLE]
    # python query_papers.py keyword <keyword> [--limit 20] [--table TABLE]
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---------------- recent ----------------
    recent_parser = subparsers.add_parser("recent", help="Get recent papers by category")
    recent_parser.add_argument("category", help="Paper category, e.g., cs.LG")
    recent_parser.add_argument("--limit", type=int, default=10, help="Max results to return")
    recent_parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")

    # ---------------- author ----------------
    author_parser = subparsers.add_parser("author", help="Search papers by author")
    author_parser.add_argument("author_name", help="Author name to search for")
    author_parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")

    # ---------------- get ----------------
    get_parser = subparsers.add_parser("get", help="Get a paper by arxiv_id")
    get_parser.add_argument("arxiv_id", help="arXiv paper ID")
    get_parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")

    # ---------------- daterange ----------------
    date_parser = subparsers.add_parser("daterange", help="Query papers within a date range")
    date_parser.add_argument("category", help="Paper category")
    date_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    date_parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    date_parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")

    # ---------------- keyword ----------------
    kw_parser = subparsers.add_parser("keyword", help="Search papers by keyword")
    kw_parser.add_argument("keyword", help="Keyword to search for")
    kw_parser.add_argument("--limit", type=int, default=10, help="Max results to return")
    kw_parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")

    return parser.parse_args()


def query_recent_in_category(table_name, category, limit=20):
    """
    Query 1: Browse recent papers in category.
    Uses: Main table partition key query with sort key descending.
    """
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']

def query_papers_by_author(table_name, author_name):
    """
    Query 2: Find all papers by author.
    Uses: GSI1 (AuthorIndex) partition key query.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    return response['Items']

def get_paper_by_id(table_name, arxiv_id):
    """
    Query 3: Get specific paper by ID.
    Uses: GSI2 (PaperIdIndex) for direct lookup.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'ARXIV_ID#{arxiv_id}')
    )
    return response['Items'][0] if response['Items'] else []

def query_papers_in_date_range(table_name, category, start_date, end_date):
    """
    Query 4: Papers in category within date range.
    Uses: Main table with composite sort key range query.
    """
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    return response['Items']

def query_papers_by_keyword(table_name, keyword, limit=20):
    """
    Query 5: Papers containing keyword.
    Uses: GSI3 (KeywordIndex) partition key query.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']




def main():

    args = parse_args()

    results = []
    IS_GET = False
    # Dispatch
    if args.command == "recent":
        query_type = "recent_in_category"
        parameters = {
            "category": args.category,
            "limit": args.limit
        }
        start = datetime.now()
        items = query_recent_in_category(args.table, args.category, limit=10)
        end = datetime.now()

        for item in items:
            results.append({
                "arxiv_id": item.get("arxiv_id"),
                "title": item.get("title"),
                "authors": item.get("authors"),
                "published": item.get("published"),
                "categories": item.get("categories"),
            })

    elif args.command == "author":
        query_type = "papers_by_author"
        parameters = {
            "author_name": args.author_name
        }
        start = datetime.now()
        items = query_papers_by_author(args.table, args.author_name)
        end = datetime.now()

        for item in items:
            results.append({
                "arxiv_id": item.get("arxiv_id"),
                "title": item.get("title"),
                "authors": item.get("authors"),
                "published": item.get("published"),
                "categories": item.get("categories"),
            })

    elif args.command == "get":
        query_type = "paper_by_id"
        parameters = {
            "arxiv_id": args.arxiv_id
        }
        start = datetime.now()
        items = get_paper_by_id(args.table, args.arxiv_id)
        end = datetime.now()

        results = {
            "arxiv_id": items.get("arxiv_id"),
            "title": items.get("title"),
            "authors": items.get("authors"),
            "published": items.get("published"),
            "categories": items.get("categories"),
        }

        IS_GET = True

    elif args.command == "daterange":
        query_type = "papers_in_date_range"
        parameters = {
            "category": args.category,
            "start_date": args.start_date,
            "end_date": args.end_date
        }
        start = datetime.now()
        items = query_papers_in_date_range(args.table, args.category, args.start_date, args.end_date)
        end = datetime.now()

        for item in items:
            results.append({
                "arxiv_id": item.get("arxiv_id"),
                "title": item.get("title"),
                "authors": item.get("authors"),
                "published": item.get("published"),
                "categories": item.get("categories"),
            })

    elif args.command == "keyword":
        query_type = "papers_by_keyword"
        parameters = {
            "keyword": args.keyword,
            "limit": args.limit
        }
        start = datetime.now()
        items = query_papers_by_keyword(args.table, args.keyword, limit=10)
        end = datetime.now()

        for item in items:
            results.append({
                "arxiv_id": item.get("arxiv_id"),
                "title": item.get("title"),
                "authors": item.get("authors"),
                "published": item.get("published"),
                "categories": item.get("categories"),
            })

    else:
        raise SystemExit("Unknown command")


    execution_time_ms = (end - start).total_seconds() * 1000  # timedelta → 毫秒

    output = {
        "query_type": query_type,
        "parameters": parameters,
        "results": results,
        "count": len(results) if not IS_GET else 1,
        "execution_time_ms": execution_time_ms
    }

    # Output JSON to stdout
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()