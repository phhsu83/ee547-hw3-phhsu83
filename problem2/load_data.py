import argparse
import boto3
import re
import json
from collections import Counter




def parse_args():
    # python load_data.py <papers_json_path> <table_name> [--region REGION]
    parser = argparse.ArgumentParser()
    # 必要參數
    parser.add_argument("papers_json_path", help="Path to the papers JSON file")
    parser.add_argument("table_name", help="Target table name")
    # 可選參數
    parser.add_argument("--region", help="Region name (optional)", default=None)

    return parser.parse_args()


def create_table(dynamodb, table_name):
    """
    Main Table keys:
      - PK: partition key (e.g., CATEGORY#<cat> / AUTHOR#<author> / KEYWORD#<kw> ... per item type)
      - SK: sort key (e.g., <published>#<arxiv_id> / prefixes like KW#... / AUTH#...)
    GSIs:
      - AuthorIndex   : HASH = GSI1PK ("AUTHOR#<name>"), RANGE = GSI1SK ("<published>#<arxiv_id>")
      - PaperIdIndex  : HASH = GSI2PK ("ARXIV_ID#<arxiv_id>")  [no sort key]
      - KeywordIndex  : HASH = GSI3PK ("KEYWORD#<kw>"), RANGE = GSI3SK ("<published>#<arxiv_id>")
    """

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'PK', 'KeyType': 'HASH'},    # Partition Key
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}    # Sort Key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI2PK', 'AttributeType': 'S'},
            {"AttributeName": "GSI3PK", "AttributeType": "S"},
            {"AttributeName": "GSI3SK", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'AuthorIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {
                    'ProjectionType': 'INCLUDE',
                    'NonKeyAttributes': ['arxiv_id', 'title', 'categories', 'published']
                },
                # 'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'PaperIdIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'},
                    # {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                    # 'NonKeyAttributes': ['title', 'published', 'categories']
                },
                # 'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'KeywordIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI3PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI3SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                    # 'NonKeyAttributes': ['title', 'published', 'categories']
                },
                # 'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        BillingMode='PAY_PER_REQUEST'  # 建議：自動計費模式
    )

    table.wait_until_exists()
    
    return table


STOPWORDS = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
        'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
        'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
        'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
        'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
    }
TOKENIZER = re.compile(r"[a-zA-Z][a-zA-Z0-9\-]*")

def extract_keywords(text, topk=10):

    tokens = [t.lower() for t in TOKENIZER.findall(text)]
    tokens = [t for t in tokens if t not in STOPWORDS]
    
    return [w for w, _ in Counter(tokens).most_common(topk)]



def transform_paper(p):
    """
    將一篇 paper 轉成多個 DynamoDB items：
      - 多分類：每個 category 一條「主 item」（PK=CATEGORY#<cat>）
      - 多作者：每個作者一條「AUTHOR item」（供 AuthorIndex）
      - 多關鍵字：每個 keyword 一條「KW item」（供 KeywordIndex）
      - PaperIdIndex：讓你用 arxiv_id 快速 Get (GSI2PK)
    需具備欄位：arxiv_id, title, authors(list), categories(list), abstract, published
    """

    arxiv_id = p["arxiv_id"]
    title    = p.get("title", "")
    authors  = list(p.get("authors", []))
    cats     = list(p.get("categories", []))
    abstract = p.get("abstract", "")
    published= p.get("published", "")
    # print("Extracting keywords from abstracts...")
    keywords = extract_keywords(p["abstract"], 10)
    
    items = []


    # 主表 item
    '''
    main_item = {
        "PK": f"CATEGORY#{p['categories'][0]}",
        "SK": f"{p['published']}#{p['arxiv_id']}",
        "arxiv_id": p["arxiv_id"],
        "title": p["title"],
        "authors": p["authors"],
        "abstract": p["abstract"], 
        "categories": p["categories"],
        "keywords": keywords,
        "published": p["published"],
        "GSI1PK": f"AUTHOR#{p['authors'][0]}",  # 用第一作者建立 GSI
        "GSI1SK": f"{p['published']}",
        "GSI2PK": f"ARXIV_ID#{p['arxiv_id']}",
        "GSI3PK": f"KEYWORD#{kw}",
        "GSI3SK": f"{p['published']}#{p['id']}",
    }
    items.append(main_item)
    '''
    # 1) 多分類 → 多條主 item
    for cat in cats:
        items.append({
            "PK": f"CATEGORY#{cat}",
            "SK": f"{published}#{arxiv_id}",

            "arxiv_id": arxiv_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "categories": cats,
            "keywords": keywords,
            "published": published,

            # PaperIdIndex（查單篇）
            "GSI2PK": f"ARXIV_ID#{arxiv_id}",
        })

    # 2) 多作者 → 每位作者一條 AUTHOR item（供 AuthorIndex）
    for author in authors:
        items.append({
            "PK": f"AUTHOR#{author}",
            "SK": f"{published}#{arxiv_id}",
            "type": "AUTHOR",

            "arxiv_id": arxiv_id,
            "title": title,
            "categories": cats,
            "published": published,

            "GSI1PK": f"AUTHOR#{author}",
            "GSI1SK": f"{published}#{arxiv_id}",
        })

    # 3) 多關鍵字 → 每個 keyword 一條 KW item（供 KeywordIndex）
    # 關鍵字 GSI (倒排索引)
    for kw in keywords:
        items.append({
            "PK": f"KEYWORD#{kw}",
            "SK": f"KW#{arxiv_id}#{kw}",
            "type": "KW",
            
            "arxiv_id": arxiv_id,
            "title": title,
            "categories": cats,
            "published": published,
            "token": kw,

            "GSI3PK": f"KEYWORD#{kw}",
            "GSI3SK": f"{published}#{arxiv_id}",
        })

    return items



def main():
    '''
    dynamodb = boto3.resource("dynamodb", region_name=args.region) if args.region else boto3.resource("dynamodb")
    table = dynamodb.Table("arxiv-papers")
    table.delete()
    # 等待刪除完成（可選）
    table.wait_until_not_exists()
    print("Table deleted.")
    '''

    args = parse_args()
    papers_json_path = args.papers_json_path
    table_name = args.table_name

    # DynamoDB client
    #dynamodb = boto3.resource("dynamodb")
    # table = dynamodb.Table(table_name)
    # boto3 clients
    dynamodb_resource = boto3.resource("dynamodb", region_name=args.region) if args.region else boto3.resource("dynamodb")
    dynamodb_client   = boto3.client("dynamodb",   region_name=args.region) if args.region else boto3.client("dynamodb")
    
    print(f"Creating DynamoDB table: {table_name}")
    print("Creating GSIs: AuthorIndex, PaperIdIndex, KeywordIndex")
    table = create_table(dynamodb_resource, table_name)

    print("Loading papers from papers.json...")
    papers = json.load(open(papers_json_path))
    print(f"Loaded {len(papers)} papers")

    total_items_written = 0
    # 批次寫入 (BatchWriteItem 最多 25 筆)
    with table.batch_writer() as batch:
        for paper in papers:
            items = transform_paper(paper)
            for item in items:
                batch.put_item(Item=item)
                total_items_written += 1

    print(f"Created {total_items_written} DynamoDB items (denormalized).")
    approx_factor = round(total_items_written / len(papers), 2)
    print(f"Denormalization factor: {approx_factor}")

    #
    print("\nStorage breakdown:")
    response = dynamodb_client.describe_table(TableName=args.table_name)
    print("Main table items:", response['Table'].get("ItemCount", 0))

    gsis = response["Table"].get("GlobalSecondaryIndexes", [])
    for gsi in gsis:
        print(f"{gsi['IndexName']} items:", gsi.get("ItemCount", 0))

    


if __name__ == "__main__":
    main()