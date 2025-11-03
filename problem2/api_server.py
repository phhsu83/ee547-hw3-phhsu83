import sys
import argparse
import boto3
import json
from boto3.dynamodb.conditions import Key
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, unquote

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table_name = "arxiv-papers"

def parse_args():
    # python3 api_server.py 8080
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int, default=8080, help="Port to listen on (default: 8080)")

    return parser.parse_args()


class PapersHandler(BaseHTTPRequestHandler):

    def _send_json(self, json_response, status=200):
        body = json.dumps(json_response, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status, message):
        self._send_json({"error": message}, status=status)

    def _log_requests(self, fmt, *args):
        print("%s - - [%s] %s" % (self.client_address[0],
                                  self.log_date_time_string(),
                                  fmt % args), file=sys.stdout)


    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") # 去尾斜線以便比對
            query = parse_qs(parsed.query) 

            # 路由
            if path == "/papers/recent":
                # /papers/recent?category={category}&limit={limit}
                self.handle_recent(query)
            
            elif path == "/papers/search":
                # /papers/search?category={category}&start={date}&end={date}
                self.handle_search(query)

            elif path.startswith("/papers/author/"):
                # /papers/author/{author_name}
                author_name = unquote(path.split("/papers/author/", 1)[1])
                self.handle_author(author_name)

            elif path.startswith("/papers/keyword/"):
                # /papers/keyword/{keyword}?limit={limit}
                keyword = unquote(path.split("/papers/keyword/", 1)[1])
                self.handle_keyword(keyword, query)            

            elif path.startswith("/papers/"):
                # /papers/{arxiv_id}
                arxiv_id = unquote(path.split("/papers/", 1)[1])
                self.handle_get_by_id(arxiv_id)
            
            else:
                # 其他路徑
                self._send_error(404, "Invalid path")
                

        except Exception as e:
            self._log_requests("ERROR %s %r", self.path, e)
            self._send_error(500, "Server errors")



    # ----- Endpoints -----
    def handle_recent(self, query):
        category = query.get("category", [None])[0]
        limit = int(query.get("limit", ["10"])[0])
        
        if not category:
            self._log_requests('GET /papers/recent?category=%s&limit=%s -> Missing query parameter: category', category, limit)
            self._send_error(404, "Missing query parameter: category")
            return

        response = dynamodb.Table(table_name).query(
            KeyConditionExpression=Key("PK").eq(f"CATEGORY#{category}"),
            ScanIndexForward=False,
            Limit=limit
        )

        papers = [{
            "arxiv_id": item.get("arxiv_id"),
            "title": item.get("title"),
            "authors": item.get("authors"),
            "published": item.get("published")
            } for item in response.get("Items", [])
        ]
        json_response = {
            "category": category,
            "papers": papers,
            "count": len(papers)
        }
        if not papers:
            self._log_requests('GET /papers/recent?category=%s&limit=%s -> Invalid category', category, limit)
            self._send_error(404, "Invalid category")
            return

        self._log_requests('GET /papers/recent?category=%s&limit=%s -> %s items', category, limit, len(papers))
        self._send_json(json_response, 200)


    def handle_author(self, author_name):

        if not author_name:
            self._log_requests('GET /papers/author/%s -> Missing query parameter: author_name', author_name)
            self._send_error(404, "Missing query parameter: author_name")
            return
        
        response = dynamodb.Table(table_name).query(
            IndexName='AuthorIndex',
            KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
        )
            
        papers = [{
            "arxiv_id": item.get("arxiv_id"),
            "title": item.get("title"),
            "categories": item.get("categories"),
            "published": item.get("published")
            } for item in response.get("Items", [])
        ]
        json_response = {
            "author": author_name,
            "papers": papers,
            "count": len(papers)
        }
        if not papers:
            self._log_requests('GET /papers/author/%s -> Invalid author_name', author_name)
            self._send_error(404, "Invalid author_name")
            return
        
        self._log_requests('GET /papers/author/%s -> %s items', author_name, len(papers))
        self._send_json(json_response, 200)


    def handle_get_by_id(self, arxiv_id):

        if not arxiv_id:
            self._log_requests('GET /papers/%s -> Missing query parameter: arxiv_id', arxiv_id)
            self._send_error(404, "Missing query parameter: arxiv_id")
            return

        response = dynamodb.Table(table_name).query(
            IndexName='PaperIdIndex',
            KeyConditionExpression=Key('GSI2PK').eq(f'ARXIV_ID#{arxiv_id}')
        )

        papers = response.get("Items", [])
        json_response = {
            "arxiv_id": arxiv_id,
            "papers": papers,
            "count": 1
        }
        if not papers:
            self._log_requests('GET /papers/%s -> Invalid arxiv_id', arxiv_id)
            self._send_error(404, "Invalid arxiv_id")
            return 
        
        self._log_requests('GET /papers/%s -> Found', arxiv_id)
        self._send_json(json_response, 200)


    def handle_search(self, query):
        category = query.get("category", [None])[0]
        start = query.get("start", [None])[0]
        end = query.get("end", [None])[0]
        if not (category and start and end):
            self._log_requests('GET /papers/search?category=%s&start=%s&end=%s -> Missing query parameters: category, start, end', category, start, end)
            self._send_error(404, "Missing query parameters: category, start, end")
            return

        response = dynamodb.Table(table_name).query(
            KeyConditionExpression=(
                Key('PK').eq(f'CATEGORY#{category}') &
                Key('SK').between(f'{start}#', f'{end}#zzzzzzz')
            )
        )
        
        papers = [{
            "arxiv_id": item.get("arxiv_id"),
            "title": item.get("title"),
            "authors": item.get("authors"),
            "categories": item.get("categories"),
            "published": item.get("published")
            } for item in response.get("Items", [])
        ]
        json_response = {
            "category": category,
            "start": start,
            "end": end,
            "papers": papers,
            "count": len(papers)
        }
        if not papers:
            self._log_requests('GET /papers/search?category=%s&start=%s&end=%s -> Invalid category, start, end', category, start, end)
            self._send_error(404, "Invalid category, start, end")
            return 
        
        self._log_requests('GET /papers/search?category=%s&start=%s&end=%s -> %s items', category, start, end, len(papers))
        self._send_json(json_response, 200)


    def handle_keyword(self, keyword, query):
        limit = int(query.get("limit", ["10"])[0])
        if not keyword:
            self._log_requests('GET /papers/keyword/%s?limit=%s -> Missing query parameter: keyword', keyword, limit)
            self._send_error(404, "Missing query parameter: keyword")
            return

        response = dynamodb.Table(table_name).query(
            IndexName='KeywordIndex',
            KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
            ScanIndexForward=False,
            Limit=limit
        )

        papers = [{
            "arxiv_id": item.get("arxiv_id"),
            "title": item.get("title"),
            "authors": item.get("authors"),
            "categories": item.get("categories"),
            "published": item.get("published")
            } for item in response.get("Items", [])
        ]
        json_response = {
            "keyword": keyword,
            "papers": papers,
            "count": len(papers)
        }
        if not papers:
            self._log_requests('GET /papers/keyword/%s?limit=%s -> Invalid keyword', keyword, limit)
            self._send_error(404, "Invalid keyword")
            return 

        self._log_requests('GET /papers/keyword/%s?limit=%s -> %s items', keyword, limit, len(papers))
        self._send_json(json_response, 200)


def main():
    
    args = parse_args()

    server = HTTPServer(("0.0.0.0", args.port), PapersHandler)
    print(f"Server running at http://localhost:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()