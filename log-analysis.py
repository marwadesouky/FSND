#!/usr/bin/env python
import datetime
import psycopg2

DBNAME = "news"

statement1 = "What are the most popular three articles of all time?"

query1 = '''select title,count(*) as count from articles,log
    where log.path=CONCAT('/article/',articles.slug)
    group by articles.title order by count DESC limit 3;'''

statement2 = "Who are the most popular article authors of all time?"

query2 = '''select authors.name, sum(subq.num) as views
    from authors, (select title, author, count(*) as num from articles,
    log where log.path=CONCAT('/article/',articles.slug) group by
    articles.title, articles.author order by num DESC) as subq
    where authors.id=subq.author group by authors.name order by
    views DESC;'''

statement3 = "On which days did more than 1% of requests lead to errors?"

query3 = '''select day, errp from
    (select b.day, round(100.00*b.hits/a.hits, 2) as errp from
    (select date(time) as day, count(*) as hits
    from log group by day) as a,
    (select date(time) as day, count(*) as hits
    from log where status not like '%200%'
    group by day) as b
    where a.day=b.day order by day) as log_percent
    where errp > 1;'''


def print_query(statement, query, text):
    query_result = exec_query(query)
    print"\n", statement
    for i in query_result:
        print(i[0], " -- ", i[1], text)


def exec_query(query):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    return c.fetchall()
    db.close()


print_query(statement1, query1, " views")
print_query(statement2, query2, " views")
print_query(statement3, query3, "% errors")
